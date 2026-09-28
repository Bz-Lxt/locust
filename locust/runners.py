from __future__ import annotations

from locust import __version__

import functools
import inspect
import json
import logging
import os
import re
import socket
import sys
import time
import traceback
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Callable, Collection, Iterator, MutableMapping, ValuesView
from operator import itemgetter, methodcaller
from types import TracebackType
from typing import TYPE_CHECKING, Any, NoReturn, TypedDict, cast
from uuid import uuid4

import gevent
import greenlet
import psutil
from gevent.event import Event
from gevent.pool import Group

from . import argument_parser
from .dispatch import UsersDispatcher
from .exception import RPCError, RPCReceiveError, RPCSendError, StopTest
from .log import get_logs, greenlet_exception_logger
from .rpc import Message, rpc
from .stats import RequestStats, StatsError, setup_distributed_stats_event_listeners
from .util.directory import get_abspaths_in
from .util.url import is_url

if TYPE_CHECKING:
    from . import User
    from .env import Environment

logger = logging.getLogger(__name__)

STATE_INIT, STATE_SPAWNING, STATE_RUNNING, STATE_CLEANUP, STATE_STOPPING, STATE_STOPPED, STATE_MISSING = [
    "ready",
    "spawning",
    "running",
    "cleanup",
    "stopping",
    "stopped",
    "missing",
]
WORKER_REPORT_INTERVAL = 3.0
WORKER_LOG_REPORT_INTERVAL = 10
CPU_MONITOR_INTERVAL = 10.0
CPU_WARNING_THRESHOLD = 90
HEARTBEAT_INTERVAL = 1
HEARTBEAT_LIVENESS = 3
HEARTBEAT_DEAD_INTERNAL = -60
MASTER_HEARTBEAT_TIMEOUT = 60
FALLBACK_INTERVAL = 5
CONNECT_TIMEOUT = 5
CONNECT_RETRY_COUNT = 60

# Result states of a cluster-wide command (one generation of master intent)
COMMAND_PENDING, COMMAND_CONFIRMED, COMMAND_TIMEOUT, COMMAND_SUPERSEDED = [
    "pending",
    "confirmed",
    "timeout",
    "superseded",
]
COMMAND_TERMINAL_STATES = {COMMAND_CONFIRMED, COMMAND_TIMEOUT, COMMAND_SUPERSEDED}

# Master intents that are totally ordered by the generation counter
INTENT_SPAWN, INTENT_STOP, INTENT_QUIT = ["spawn", "stop", "quit"]


def canonical_spawn_target(
    user_count: int, spawn_rate: float, user_classes: list[type[User]] | None
) -> tuple[Any, ...]:
    """
    A hashable fingerprint of a spawn intent. Two spawn calls with the same fingerprint
    express the same cluster intent and therefore reuse the current generation.
    """
    class_names = tuple(sorted(user_class.__name__ for user_class in user_classes)) if user_classes else ()
    return (user_count, spawn_rate, class_names)


class ClusterCommandResult:
    """
    The confirmable outcome of one generation of master intent.

    The result stays ``pending`` until every expected worker has acknowledged the
    generation, it times out while workers are missing, or a newer generation
    supersedes it. Terminal transitions happen at most once and wake up all waiters.
    """

    def __init__(
        self,
        generation: int,
        intent: str,
        target: Any = None,
        expected_workers: Collection[str] | None = None,
        on_terminal: Callable[[ClusterCommandResult], None] | None = None,
    ) -> None:
        self.generation = generation
        self.intent = intent
        self.target = target
        self.expected_workers: set[str] = set(expected_workers or ())
        self.acked_workers: set[str] = set()
        self.ack_payloads: dict[str, Any] = {}
        self.status: str = COMMAND_PENDING
        self.missing_snapshot: set[str] = set()
        self._on_terminal = on_terminal
        self._completed_event = Event()

    @property
    def is_terminal(self) -> bool:
        return self.status in COMMAND_TERMINAL_STATES

    @property
    def missing_workers(self) -> set[str]:
        if self.is_terminal:
            return set(self.missing_snapshot)
        return self.expected_workers - self.acked_workers

    @property
    def confirmed(self) -> bool:
        return self.status == COMMAND_CONFIRMED

    def wait(self, timeout: float | None = None) -> ClusterCommandResult:
        self._completed_event.wait(timeout=timeout)
        return self

    def expect(self, worker_id: str) -> None:
        """Add a worker to the set that must acknowledge this generation."""
        if not self.is_terminal:
            self.expected_workers.add(worker_id)

    def drop_expected(self, worker_id: str) -> None:
        """A worker vanished (marked missing/removed); it can no longer acknowledge."""
        self.expected_workers.discard(worker_id)
        self.acked_workers.discard(worker_id)
        self.ack_payloads.pop(worker_id, None)
        self._maybe_confirm()

    def ack(self, worker_id: str, payload: Any = None) -> bool:
        """Record a worker acknowledgement. Returns True if this completed the result."""
        if self.is_terminal:
            return False
        self.acked_workers.add(worker_id)
        if payload is not None:
            self.ack_payloads[worker_id] = payload
        return self._maybe_confirm()

    def _maybe_confirm(self) -> bool:
        # An empty expected set must not count as confirmed: a generation that lost all
        # workers is either superseded (a stop generation follows) or times out.
        if self.expected_workers and self.expected_workers <= self.acked_workers:
            self.finish(COMMAND_CONFIRMED)
            return True
        return False

    def finish(self, status: str, missing: Collection[str] | None = None) -> bool:
        if self.is_terminal:
            return False
        assert status in COMMAND_TERMINAL_STATES
        self.status = status
        self.missing_snapshot = set(missing if missing is not None else self.expected_workers - self.acked_workers)
        if self._on_terminal is not None:
            self._on_terminal(self)
        self._completed_event.set()
        return True

    def snapshot(self) -> dict[str, Any]:
        return {
            "generation": self.generation,
            "intent": self.intent,
            "status": self.status,
            "expected_workers": set(self.expected_workers),
            "acked_workers": set(self.acked_workers),
            "missing_workers": self.missing_workers,
        }

    def __repr__(self) -> str:
        return f"<ClusterCommandResult gen={self.generation} {self.intent} {self.status}>"


class ClusterCommandLedger:
    """
    Master-side bookkeeping of the totally ordered cluster intent generations.

    Only the latest generation is expected to converge cluster-wide. Submitting the
    same intent+target while it is still pending coalesces onto the pending result;
    submitting it after it went terminal (rebalance / reconnect re-dispatch) opens a
    new result while reusing the generation number; any different intent/target
    allocates a new generation and supersedes the pending one.
    """

    def __init__(self, on_terminal: Callable[[ClusterCommandResult], None] | None = None) -> None:
        self.generation: int = 0
        self.current: ClusterCommandResult | None = None
        self.intent: str | None = None
        self.target: Any = None
        self._on_terminal = on_terminal

    def submit(
        self,
        intent: str,
        target: Any = None,
        expected_workers: Collection[str] | None = None,
    ) -> ClusterCommandResult:
        if self.current is not None and not self.current.is_terminal:
            if self.current.intent == intent and self.current.target == target:
                # Same intent dispatched again while in flight (concurrent call): coalesce.
                for worker_id in expected_workers or ():
                    self.current.expect(worker_id)
                return self.current
            self.current.finish(COMMAND_SUPERSEDED)

        same_intent = self.current is not None and self.intent == intent and self.target == target
        if not same_intent:
            self.generation += 1
            self.intent = intent
            self.target = target

        result = ClusterCommandResult(
            self.generation, intent, target, expected_workers=expected_workers, on_terminal=self._on_terminal
        )
        self.current = result
        return result

    def observe_ack(self, worker_id: str, generation: int, payload: Any = None) -> ClusterCommandResult | None:
        """
        Route an acknowledgement carrying a generation. Acks for anything but the
        current result are ignored (callers may treat them as stale information).
        """
        if self.current is None or generation != self.current.generation or self.current.is_terminal:
            return None
        self.current.ack(worker_id, payload)
        return self.current

    def expect_worker(self, worker_id: str) -> None:
        if self.current is not None and not self.current.is_terminal:
            self.current.expect(worker_id)

    def drop_worker(self, worker_id: str) -> None:
        if self.current is not None and not self.current.is_terminal:
            self.current.drop_expected(worker_id)

    def is_current(self, generation: int) -> bool:
        return self.current is not None and self.current.generation == generation


def locust_exception_handler(environment: Environment):
    exception_logger = greenlet_exception_logger(logger)

    def handler(greenlet):
        if greenlet.exc_info[0] is StopTest:
            logger.error(greenlet.exc_info[1])
            logger.warning("Stopping Locust...")
            if environment.parsed_options.headless:
                environment.runner.quit()
            else:
                environment.runner.stop()
        else:
            exception_logger(greenlet)

    return handler


class ExceptionDict(TypedDict):
    count: int
    msg: str
    traceback: str
    nodes: set[str]


class Runner:
    """
    Orchestrates the load test by starting and stopping the users.

    Use one of the :meth:`create_local_runner <locust.env.Environment.create_local_runner>`,
    :meth:`create_master_runner <locust.env.Environment.create_master_runner>` or
    :meth:`create_worker_runner <locust.env.Environment.create_worker_runner>` methods on
    the :class:`Environment <locust.env.Environment>` instance to create a runner of the
    desired type.
    """

    def __init__(self, environment: Environment) -> None:
        self.environment = environment
        self.user_greenlets = Group()
        self.greenlet = Group()
        self.state = STATE_INIT
        self.spawning_greenlet: gevent.Greenlet | None = None
        self.shape_greenlet: gevent.Greenlet | None = None
        self.shape_last_tick: tuple[int, float] | tuple[int, float, list[type[User]] | None] | None = None
        self.current_cpu_usage: float = 0.0
        self.cpu_warning_emitted: bool = False
        self.worker_cpu_warning_emitted: bool = False
        self.current_memory_usage: int = 0
        self.greenlet.spawn(self.monitor_cpu_and_memory).link_exception(locust_exception_handler(self.environment))
        self.exceptions: dict[int, ExceptionDict] = {}
        # Because of the way the ramp-up/ramp-down is implemented, target_user_classes_count
        # is only updated at the end of the ramp-up/ramp-down.
        # See https://github.com/locustio/locust/issues/1883#issuecomment-919239824 for context.
        self.target_user_classes_count: dict[str, int] = {}
        # target_user_count is set before the ramp-up/ramp-down occurs.
        self.target_user_count: int = 0
        self.custom_messages: dict[str, tuple[Callable, bool]] = {}

        self._users_dispatcher: UsersDispatcher | None = None

        # Serializes intent submission (start/stop/quit). The lock is only held while
        # state transitions, ledger submissions and command dispatch setup happen;
        # cluster convergence waits occur outside the lock so a newer intent can
        # supersede one that is still waiting for acknowledgements.
        self._command_lock = gevent.lock.RLock()

        # set up event listeners for recording requests
        def on_request(request_type, name, response_time, response_length, exception=None, **_kwargs):
            self.stats.log_request(request_type, name, response_time, response_length)
            if exception:
                self.stats.log_error(request_type, name, exception)

        self.environment.events.request.add_listener(on_request)

        self.connection_broken = False
        self.final_user_classes_count: dict[str, int] = {}  # just for the ratio report, fills before runner stops

        # register listener that resets stats when spawning is complete
        def on_spawning_complete(user_count: int) -> None:
            self.update_state(STATE_RUNNING)
            if environment.reset_stats:
                logger.info("Resetting stats\n")
                self.stats.reset_all()

        self.environment.events.spawning_complete.add_listener(on_spawning_complete)

    def __del__(self) -> None:
        # don't leave any stray greenlets if runner is removed
        if self.greenlet and len(self.greenlet) > 0:
            self.greenlet.kill(block=False)

    @property
    def user_classes(self) -> list[type[User]]:
        return self.environment.user_classes

    @property
    def user_classes_by_name(self) -> dict[str, type[User]]:
        return self.environment.user_classes_by_name

    @property
    def stats(self) -> RequestStats:
        return self.environment.stats

    @property
    def errors(self) -> dict[str, StatsError]:
        return self.stats.errors

    @property
    def user_count(self) -> int:
        """
        :returns: Number of currently running users
        """
        return len(self.user_greenlets)

    @property
    def user_classes_count(self) -> dict[str, int]:
        """
        :returns: Number of currently running users for each user class
        """
        user_classes_count = {user_class.__name__: 0 for user_class in self.user_classes}
        for user_greenlet in self.user_greenlets:
            try:
                user = user_greenlet.args[0]
            except IndexError:
                # TODO: Find out why args is sometimes empty. In gevent code,
                #       the supplied args are cleared in the gevent.greenlet.Greenlet.__free,
                #       so it seems a good place to start investigating. My suspicion is that
                #       the supplied args are emptied whenever the greenlet is dead, so we can
                #       simply ignore the greenlets with empty args.
                logger.debug(
                    "ERROR: While calculating number of running users, we encountered a user that didn't have proper args %s (user_greenlet.dead=%s)",
                    user_greenlet,
                    user_greenlet.dead,
                )
                continue
            user_classes_count[user.__class__.__name__] += 1
        return user_classes_count

    def update_state(self, new_state: str) -> None:
        """
        Updates the current state
        """
        # I (cyberwiz) commented out this logging, because it is too noisy even for debug level
        # Uncomment it if you are specifically debugging state transitions
        # logger.debug("Updating state to '%s', old state was '%s'" % (new_state, self.state))
        self.state = new_state

    def cpu_log_warning(self) -> bool:
        """Called at the end of the test"""
        if self.cpu_warning_emitted:
            logger.warning(
                "CPU usage was too high at some point during the test! See https://docs.locust.io/en/stable/running-distributed.html for how to distribute the load over multiple CPU cores or machines"
            )
        return self.cpu_warning_emitted

    def spawn_users(self, user_classes_spawn_count: dict[str, int], wait: bool = False):
        if self.state == STATE_INIT or self.state == STATE_STOPPED:
            self.update_state(STATE_SPAWNING)

        logger.debug(
            f"Spawning additional {json.dumps(user_classes_spawn_count)} ({json.dumps(self.user_classes_count)} already running)..."
        )

        def spawn(user_class: str, spawn_count: int) -> list[User]:
            n = 0
            new_users: list[User] = []
            while n < spawn_count:
                new_user = self.user_classes_by_name[user_class](self.environment)
                assert hasattr(new_user, "environment"), (
                    f"Attribute 'environment' is missing on user {user_class}. Perhaps you defined your own __init__ and forgot to call the base constructor? (super().__init__(*args, **kwargs))"
                )
                new_user.start(self.user_greenlets)
                new_users.append(new_user)
                n += 1
                if n % 10 == 0 or n == spawn_count:
                    logger.debug("%i users spawned" % self.user_count)
            logger.debug(f"All users of class {user_class} spawned")
            return new_users

        new_users: list[User] = []
        for user_class, spawn_count in user_classes_spawn_count.items():
            new_users += spawn(user_class, spawn_count)

        if wait:
            self.user_greenlets.join()
            logger.info("All users stopped\n")
        return new_users

    def stop_users(self, user_classes_stop_count: dict[str, int]) -> None:
        async_calls_to_stop = Group()
        stop_group = Group()

        for user_class, stop_count in user_classes_stop_count.items():
            if self.user_classes_count[user_class] == 0:
                continue

            to_stop: list[greenlet.greenlet] = []
            for user_greenlet in self.user_greenlets:
                if len(to_stop) == stop_count:
                    break
                try:
                    user = user_greenlet.args[0]
                except IndexError:
                    logger.error(
                        "While stopping users, we encountered a user that didn't have proper args %s", user_greenlet
                    )
                    continue
                if type(user) is self.user_classes_by_name[user_class]:
                    to_stop.append(user)

            if not to_stop:
                continue

            while True:
                user_to_stop: User = to_stop.pop()
                logger.debug(f"Stopping {user_to_stop.greenlet.name}")
                if user_to_stop.greenlet is greenlet.getcurrent():
                    # User called runner.quit(), so don't block waiting for killing to finish
                    user_to_stop.group.killone(user_to_stop.greenlet, block=False)
                elif self.environment.stop_timeout:
                    async_calls_to_stop.add(gevent.spawn_later(0, user_to_stop.stop, force=False))
                    stop_group.add(user_to_stop.greenlet)
                else:
                    async_calls_to_stop.add(gevent.spawn_later(0, user_to_stop.stop, force=True))
                if not to_stop:
                    break

        async_calls_to_stop.join()

        if not stop_group.join(timeout=self.environment.stop_timeout):
            logger.info(
                f"Not all users finished their tasks & terminated in {self.environment.stop_timeout} seconds. Stopping them..."
            )
            stop_group.kill(block=True)

        logger.debug(
            "%g users have been stopped, %g still running", sum(user_classes_stop_count.values()), self.user_count
        )

    def monitor_cpu_and_memory(self) -> NoReturn:
        process = psutil.Process()
        while True:
            gevent.sleep(CPU_MONITOR_INTERVAL)
            self.current_cpu_usage = process.cpu_percent()
            self.current_memory_usage = process.memory_info().rss
            if self.current_cpu_usage > CPU_WARNING_THRESHOLD:
                self.environment.events.cpu_warning.fire(environment=self.environment, cpu_usage=self.current_cpu_usage)
                if not self.cpu_warning_emitted:
                    logging.warning(
                        f"CPU usage above {CPU_WARNING_THRESHOLD}%! This may constrain your throughput and may even give inconsistent response time measurements! See https://docs.locust.io/en/stable/running-distributed.html for how to distribute the load over multiple CPU cores or machines"
                    )
                    self.cpu_warning_emitted = True

            self.environment.events.usage_monitor.fire(
                environment=self.environment, cpu_usage=self.current_cpu_usage, memory_usage=self.current_memory_usage
            )

    @abstractmethod
    def start(
        self, user_count: int, spawn_rate: float, wait: bool = False, user_classes: list[type[User]] | None = None
    ) -> ClusterCommandResult | None: ...

    @abstractmethod
    def send_message(self, msg_type: str, data: Any = None, client_id: str | None = None) -> None: ...

    def start_shape(self) -> None:
        """
        Start running a load test with a custom LoadTestShape specified in the :meth:`Environment.shape_class <locust.env.Environment.shape_class>` parameter.
        """
        if self.shape_greenlet:
            logger.info("There is an ongoing shape test running. Editing is disabled")
            return

        logger.info("Shape test starting.")
        self.update_state(STATE_INIT)
        self.shape_greenlet = self.greenlet.spawn(self.shape_worker)
        self.shape_greenlet.link_exception(locust_exception_handler(self.environment))
        if self.environment.shape_class is not None:
            self.environment.shape_class.reset_time()

    def shape_worker(self) -> None:
        logger.info("Shape worker starting")
        while self.state == STATE_INIT or self.state == STATE_SPAWNING or self.state == STATE_RUNNING:
            shape_adjustment_start = time.time()
            current_tick = self.environment.shape_class.tick() if self.environment.shape_class is not None else None
            if current_tick is None:
                logger.info("Shape test stopping")
                if self.environment.parsed_options and self.environment.parsed_options.headless:
                    self.quit()
                else:
                    self.stop()
                self.shape_greenlet = None
                self.shape_last_tick = None
                return
            elif self.shape_last_tick != current_tick:
                if len(current_tick) == 2:
                    user_count, spawn_rate = current_tick
                    user_classes = None
                else:
                    user_count, spawn_rate, user_classes = current_tick
                logger.info("Shape test updating to %d users at %.2f spawn rate" % (user_count, spawn_rate))
                # TODO: This `self.start()` call is blocking until the ramp-up is completed. This can leads
                #       to unexpected behaviours such as the one in the following example:
                #       A load test shape has the following stages:
                #           stage 1: (user_count=100, spawn_rate=1) for t < 50s
                #           stage 2: (user_count=120, spawn_rate=1) for t < 100s
                #           stage 3: (user_count=130, spawn_rate=1) for t < 120s
                #        Because the first stage will take 100s to complete, the second stage
                #        will be skipped completely because the shape worker will be blocked
                #        at the `self.start()` of the first stage.
                #        Of course, this isn't a problem if the load test shape is well-defined.
                #        We should probably use a `gevent.timeout` with a duration a little over
                #        `(user_count - prev_user_count) / spawn_rate` in order to limit the runtime
                #        of each load test shape stage.
                self.start(user_count=user_count, spawn_rate=spawn_rate, user_classes=user_classes)
                self.shape_last_tick = current_tick
            shape_adjustment_time_ms = time.time() - shape_adjustment_start
            gevent.sleep(max(1 - shape_adjustment_time_ms, 0))

    def stop(self, send_stop_to_client: bool = True) -> ClusterCommandResult | None:
        """
        Stop a running load test by stopping all running users.

        ``send_stop_to_client`` is only meaningful for the distributed master; the
        base/local implementation ignores it. Returns the cluster command result
        for distributed runners, ``None`` locally until the result is wired up.
        """
        if self.state in (STATE_STOPPED, STATE_STOPPING, STATE_CLEANUP):
            # Re-entrant/concurrent call while a stop is already in progress:
            # never fire test_stopping/test_stop a second time.
            return None
        try:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            logger.debug(f"Stopping all users (called from {caller.filename}:{caller.lineno})")
        except Exception:
            logger.debug("Stopping all users (couldn't determine where stop() was called from)")
        self.environment.events.test_stopping.fire(environment=self.environment)
        self.final_user_classes_count = {**self.user_classes_count}
        self.update_state(STATE_CLEANUP)

        # if we are currently spawning users we need to kill the spawning greenlet first
        if self.spawning_greenlet and not self.spawning_greenlet.ready():
            self.spawning_greenlet.kill(block=True)

        if self.environment.shape_class is not None and self.shape_greenlet is not greenlet.getcurrent():
            # If the test was not started yet and locust is
            # stopped/quit, shape_greenlet will be None.
            if self.shape_greenlet is not None:
                self.shape_greenlet.kill(block=True)
                self.shape_greenlet = None
            self.shape_last_tick = None

        self.stop_users(self.user_classes_count)

        self._users_dispatcher = None

        self.update_state(STATE_STOPPED)

        self.cpu_log_warning()
        self.environment.events.test_stop.fire(environment=self.environment)
        return None

    def quit(self) -> None:
        """
        Stop any running load test and kill all greenlets for the runner
        """
        self.stop()
        self.greenlet.kill(block=True)

    def log_exception(self, node_id: str, msg: str, formatted_tb: str) -> None:
        key = hash(formatted_tb)
        row = self.exceptions.setdefault(key, {"count": 0, "msg": msg, "traceback": formatted_tb, "nodes": set()})
        row["count"] += 1
        row["nodes"].add(node_id)
        self.exceptions[key] = row

    def register_message(self, msg_type: str, listener: Callable, concurrent=False) -> None:
        """
        Register a listener for a custom message from another node

        :param msg_type: The type of the message to listen for
        :param listener: The function to execute when the message is received
        """
        if msg_type in self.custom_messages:
            raise Exception(f"Tried to register listener method for {msg_type}, but it already had a listener!")
        self.custom_messages[msg_type] = (listener, concurrent)


class LocalRunner(Runner):
    """
    Runner for running single process load test
    """

    def __init__(self, environment) -> None:
        """
        :param environment: Environment instance
        """
        super().__init__(environment)
        # These attributes dont make a lot of sense for LocalRunner
        # but it makes it easier to write tests that work for both local and distributed runs
        self.worker_index = 0
        self.client_id = socket.gethostname() + "_" + uuid4().hex
        self.worker_count = 1
        # Only when running in standalone mode (non-distributed)
        self._local_worker_node = WorkerNode(id="local")
        self._local_worker_node.user_classes_count = self.user_classes_count

        # register listener that's logs the exception for the local runner
        def on_user_error(user_instance, exception, tb):
            formatted_tb = "".join(traceback.format_tb(tb))
            self.log_exception("local", str(exception), formatted_tb)

        self.environment.events.user_error.add_listener(on_user_error)

    def _start(self, user_count: int, spawn_rate: float, wait: bool = False, user_classes: list | None = None) -> None:
        """
        Start running a load test

        :param user_count: Total number of users to start
        :param spawn_rate: Number of users to spawn per second
        :param wait: If True calls to this method will block until all users are spawned.
                     If False (the default), a greenlet that spawns the users will be
                     started and the call to this method will return immediately.
        :param user_classes: The user classes to be dispatched, None indicates to use the classes the dispatcher was
                             invoked with.
        """
        self.target_user_count = user_count

        if self.state != STATE_RUNNING and self.state != STATE_SPAWNING:
            self.stats.clear_all()
            self.exceptions = {}
            self.cpu_warning_emitted = False
            self.worker_cpu_warning_emitted = False
            self.environment._filter_tasks_by_tags()
            self.environment.events.test_start.fire(environment=self.environment)

        if wait and user_count - self.user_count > spawn_rate:
            raise ValueError("wait is True but the amount of users to add is greater than the spawn rate")

        for user_class in self.user_classes:
            if self.environment.host:
                user_class.host = self.environment.host

        if self.state != STATE_INIT and self.state != STATE_STOPPED:
            self.update_state(STATE_SPAWNING)

        if self._users_dispatcher is None:
            self._users_dispatcher = self.environment.dispatcher_class(
                worker_nodes=[self._local_worker_node], user_classes=self.user_classes
            )

        logger.info("Ramping to %d users at a rate of %.2f per second" % (user_count, spawn_rate))

        self._users_dispatcher.new_dispatch(user_count, spawn_rate, user_classes)

        try:
            for dispatched_users in self._users_dispatcher:
                user_classes_spawn_count: dict[str, int] = {}
                user_classes_stop_count: dict[str, int] = {}
                user_classes_count = dispatched_users[self._local_worker_node.id]
                logger.debug(f"Ramping to {_format_user_classes_count_for_log(user_classes_count)}")
                for user_class_name, user_class_count in user_classes_count.items():
                    if self.user_classes_count[user_class_name] > user_class_count:
                        user_classes_stop_count[user_class_name] = (
                            self.user_classes_count[user_class_name] - user_class_count
                        )
                    elif self.user_classes_count[user_class_name] < user_class_count:
                        user_classes_spawn_count[user_class_name] = (
                            user_class_count - self.user_classes_count[user_class_name]
                        )

                if wait:
                    # spawn_users will block, so we need to call stop_users first
                    self.stop_users(user_classes_stop_count)
                    self.spawn_users(user_classes_spawn_count, wait)
                else:
                    # call spawn_users before stopping the users since stop_users
                    # can be blocking because of the stop_timeout
                    self.spawn_users(user_classes_spawn_count, wait)
                    self.stop_users(user_classes_stop_count)

                self._local_worker_node.user_classes_count = next(iter(dispatched_users.values()))

        except KeyboardInterrupt:
            # TODO: Find a cleaner way to handle that
            # We need to catch keyboard interrupt. Otherwise, if KeyboardInterrupt is received while in
            # a gevent.sleep inside the dispatch_users function, locust won't gracefully shutdown.
            self.quit()

        logger.info(f"All users spawned: {_format_user_classes_count_for_log(self.user_classes_count)}")

        self.target_user_classes_count = self.user_classes_count

        self.environment.events.spawning_complete.fire(user_count=sum(self.target_user_classes_count.values()))

    def start(
        self, user_count: int, spawn_rate: float, wait: bool = False, user_classes: list[type[User]] | None = None
    ) -> ClusterCommandResult | None:
        if spawn_rate > 100:
            logger.warning(
                "Your selected spawn rate is very high (>100), and this is known to sometimes cause issues. Do you really need to ramp up that fast?"
            )

        with self._command_lock:
            if self.spawning_greenlet:
                # kill existing spawning_greenlet before we start a new one
                self.spawning_greenlet.kill(block=True)
            self.spawning_greenlet = self.greenlet.spawn(
                lambda: self._start(user_count, spawn_rate, wait=wait, user_classes=user_classes)
            )
            self.spawning_greenlet.link_exception(locust_exception_handler(self.environment))
        return None

    def stop(self, send_stop_to_client: bool = True) -> ClusterCommandResult | None:
        with self._command_lock:
            if self.state in (STATE_STOPPED, STATE_STOPPING, STATE_CLEANUP):
                return None
            super().stop()
        return None

    def send_message(self, msg_type: str, data: Any = None, client_id: str | None = None) -> None:
        """
        Emulates internodal messaging by calling registered listeners

        :param msg_type: The type of the message to emulate sending
        :param data: Optional data to include
        """
        logger.debug(f"Running locally: sending {msg_type} message to self")
        if msg_type in self.custom_messages:
            listener, concurrent = self.custom_messages[msg_type]
            msg = Message(msg_type, data, "local")
            listener(environment=self.environment, msg=msg)
        else:
            logger.warning(f"Unknown message type received: {msg_type}")


class DistributedRunner(Runner):
    def __init__(self, environment) -> None:
        super().__init__(environment)
        setup_distributed_stats_event_listeners(self.environment.events, self.stats)


class WorkerNode:
    def __init__(self, id: str, state=STATE_INIT, heartbeat_liveness=HEARTBEAT_LIVENESS) -> None:
        self.id: str = id
        self.state = state
        self.heartbeat = heartbeat_liveness
        self.cpu_usage: int = 0
        self.cpu_warning_emitted = False
        self.memory_usage: int = 0
        # The reported users running on the worker
        self.user_classes_count: dict[str, int] = {}
        # Highest command generation the worker reported to have seen/applied
        self.last_generation: int = -1
        # Last per-worker user target dispatched to this worker
        self.last_target: dict[str, int] = {}

    @property
    def user_count(self) -> int:
        return sum(self.user_classes_count.values())


class WorkerNodes(MutableMapping):
    def __init__(self) -> None:
        self._worker_nodes: dict[str, WorkerNode] = {}

    def get_by_state(self, state) -> list[WorkerNode]:
        return [c for c in self.values() if c.state == state]

    @property
    def all(self) -> ValuesView[WorkerNode]:
        return self.values()

    @property
    def ready(self) -> list[WorkerNode]:
        return self.get_by_state(STATE_INIT)

    @property
    def spawning(self) -> list[WorkerNode]:
        return self.get_by_state(STATE_SPAWNING)

    @property
    def running(self) -> list[WorkerNode]:
        return self.get_by_state(STATE_RUNNING)

    @property
    def missing(self) -> list[WorkerNode]:
        return self.get_by_state(STATE_MISSING)

    def __setitem__(self, k: str, v: WorkerNode) -> None:
        self._worker_nodes[k] = v

    def __delitem__(self, k: str) -> None:
        del self._worker_nodes[k]

    def __getitem__(self, k: str) -> WorkerNode:
        return self._worker_nodes[k]

    def __len__(self) -> int:
        return len(self._worker_nodes)

    def __iter__(self) -> Iterator[str]:
        return iter(list(self._worker_nodes.keys()))


def parse_client_ready_data(data: Any) -> tuple[Any, int, str | None]:
    """
    Parse a worker's client_ready handshake payload.

    New workers send ``{"version": ..., "generation": <watermark>, "state": ...}``.
    Legacy payloads (a bare version string/int/None) return version ``None`` so the
    master can reject them: the cluster command generation protocol is mandatory.
    """
    if isinstance(data, dict) and "version" in data:
        return data["version"], int(data.get("generation", -1)), data.get("state")
    return None, -1, None


def make_client_ready_data(version: str, generation: int, state: str) -> dict[str, Any]:
    return {"version": version, "generation": generation, "state": state}


class MasterRunner(DistributedRunner):
    """
    Runner used to run distributed load tests across multiple processes and/or machines.

    MasterRunner doesn't spawn any user greenlets itself. Instead it expects
    :class:`WorkerRunners <WorkerRunner>` to connect to it, which it will then direct
    to start and stop user greenlets. Stats sent back from the
    :class:`WorkerRunners <WorkerRunner>` will aggregated.
    """

    def __init__(self, environment, master_bind_host, master_bind_port) -> None:
        """
        :param environment: Environment instance
        :param master_bind_host: Host/interface to use for incoming worker connections
        :param master_bind_port: Port to use for incoming worker connections
        """
        super().__init__(environment)
        self.worker_cpu_warning_emitted = False
        self.master_bind_host = master_bind_host
        self.master_bind_port = master_bind_port
        self.spawn_rate: float = 0.0
        self.spawning_completed = False
        self.worker_indexes: dict[str, int] = {}
        self.worker_index_max = 0

        self.clients = WorkerNodes()
        try:
            self.server = rpc.Server(master_bind_host, master_bind_port)
        except RPCError as e:
            if e.args[0] == "Socket bind failure: Address already in use":
                port_string = (
                    master_bind_host + ":" + str(master_bind_port) if master_bind_host != "*" else str(master_bind_port)
                )
                logger.error(
                    f"The Locust master port ({port_string}) was busy. Close any applications using that port - perhaps an old instance of Locust master is still running? ({e.args[0]})"
                )
                sys.exit(1)
            else:
                raise

        self._users_dispatcher: UsersDispatcher | None = None
        # Totally ordered generations of cluster intent and their confirmable results
        self._command_ledger = ClusterCommandLedger(on_terminal=self._on_command_result_terminal)
        # The result of the latest submitted cluster command
        self.last_command_result: ClusterCommandResult | None = None

        self.greenlet.spawn(self.heartbeat_worker).link_exception(locust_exception_handler(self.environment))
        self.greenlet.spawn(self.client_listener).link_exception(locust_exception_handler(self.environment))

        # listener that gathers info on how many users the worker has spawned
        def on_worker_report(client_id: str, data: dict[str, Any]) -> None:
            if client_id not in self.clients:
                logger.info("Discarded report from unrecognized worker %s", client_id)
                return
            self.clients[client_id].user_classes_count = data["user_classes_count"]

        self.environment.events.worker_report.add_listener(on_worker_report)

        # register listener that sends quit message to worker nodes
        def on_quitting(environment: Environment, **kw):
            self.quit()

        self.environment.events.quitting.add_listener(on_quitting)

    def rebalancing_enabled(self) -> bool:
        return self.environment.parsed_options is not None and cast(
            bool, self.environment.parsed_options.enable_rebalancing
        )

    def _on_command_result_terminal(self, result: ClusterCommandResult) -> None:
        if result.status == COMMAND_TIMEOUT:
            logger.warning(
                "Timed out waiting for workers to confirm generation %i (%s), missing workers: %s",
                result.generation,
                result.intent,
                sorted(result.missing_workers),
            )
        elif result.status == COMMAND_SUPERSEDED:
            logger.debug(
                "Generation %i (%s) was superseded before all workers confirmed it",
                result.generation,
                result.intent,
            )
        else:
            logger.debug("Generation %i (%s) confirmed by all workers", result.generation, result.intent)

    def _handshake_ack_data(self, client_id: str) -> dict[str, Any]:
        ledger = self._command_ledger
        return {
            "index": self.get_worker_index(client_id),
            "generation": ledger.generation,
            "intent": ledger.intent,
            "target": ledger.target if ledger.intent == INTENT_SPAWN else None,
        }

    def _spawn_payload(self, worker_user_classes_count: dict[str, int], generation: int) -> dict[str, Any]:
        return {
            "timestamp": time.time(),
            "generation": generation,
            "user_classes_count": worker_user_classes_count,
            "host": self.environment.host,
            "stop_timeout": self.environment.stop_timeout,
            "parsed_options": vars(self.environment.parsed_options) if self.environment.parsed_options else {},
        }

    def _send_spawn_to_worker(
        self, client_id: str, worker_user_classes_count: dict[str, int], generation: int
    ) -> None:
        self.server.send_to_client(
            Message("spawn", self._spawn_payload(worker_user_classes_count, generation), client_id)
        )
        if client_id in self.clients:
            # Record what we last dispatched to this node. Its applied watermark
            # (last_generation) is only advanced by worker-originated evidence
            # (ack/heartbeat/client_ready), so that command_stale can trigger a resend.
            self.clients[client_id].last_target = dict(worker_user_classes_count)

    def _resync_worker(self, client_id: str) -> None:
        """
        Re-drive a single stale/reconnected worker to the current cluster intent.
        Workers apply every intent idempotently per generation, so resending the
        current command is always safe.
        """
        if client_id not in self.clients:
            return
        ledger = self._command_ledger
        node = self.clients[client_id]
        if node.last_generation >= ledger.generation and ledger.current is not None:
            # The worker is at the current generation but answered an old one; nothing to send.
            return
        self._command_ledger.expect_worker(client_id)
        if ledger.intent == INTENT_SPAWN:
            if node.last_target:
                logger.debug(
                    "Re-dispatching current spawn (generation %i) to stale worker %s", ledger.generation, client_id
                )
                self._send_spawn_to_worker(client_id, node.last_target, ledger.generation)
            else:
                # We do not know this worker's slice: a full coalesced re-dispatch rebuilds it.
                logger.debug(
                    "Triggering a full re-dispatch to reconcile worker %s with generation %i",
                    client_id,
                    ledger.generation,
                )
                self.greenlet.spawn(self.start, self.target_user_count, self.spawn_rate).link_exception(
                    locust_exception_handler(self.environment)
                )
        elif ledger.intent == INTENT_STOP:
            logger.debug("Re-sending current stop (generation %i) to stale worker %s", ledger.generation, client_id)
            self.server.send_to_client(Message("stop", {"generation": ledger.generation}, client_id))
        elif ledger.intent == INTENT_QUIT:
            logger.debug("Re-sending current quit (generation %i) to stale worker %s", ledger.generation, client_id)
            self.server.send_to_client(Message("quit", {"generation": ledger.generation}, client_id))

    def get_worker_index(self, client_id):
        """
        Get the worker index for the specified client ID;
        this is a deterministic 0-based ordinal number and guaranteed to not change
        while Master is alive.
        """
        if client_id in self.worker_indexes:
            return self.worker_indexes[client_id]
        index = self.worker_index_max
        self.worker_indexes[client_id] = index
        self.worker_index_max += 1
        return index

    @property
    def user_count(self) -> int:
        return sum([c.user_count for c in self.clients.values()])

    def cpu_log_warning(self) -> bool:
        warning_emitted = Runner.cpu_log_warning(self)
        if self.worker_cpu_warning_emitted:
            logger.warning("CPU usage threshold was exceeded on workers during the test!")
            warning_emitted = True
        return warning_emitted

    def start(
        self, user_count: int, spawn_rate: float, wait=False, user_classes: list[type[User]] | None = None
    ) -> ClusterCommandResult | None:
        self.spawning_completed = False

        self.target_user_count = user_count

        # Serialized submission section: worker-set computation, ledger submission,
        # lifecycle gating and dispatcher (re)arm are atomic with respect to concurrent
        # start/stop/quit calls. The convergence wait happens below, outside the lock, so
        # a newer intent can still supersede this one while it is unconfirmed.
        with self._command_lock:
            active_workers = self.clients.ready + self.clients.running + self.clients.spawning
            num_workers = len(active_workers)
            if not num_workers:
                logger.warning(
                    "You can't start a distributed test before at least one worker processes has connected"
                )
                return None

            for user_class in self.user_classes:
                if self.environment.host:
                    user_class.host = self.environment.host

            self.spawn_rate = spawn_rate

            if self._users_dispatcher is None:
                self._users_dispatcher = self.environment.dispatcher_class(
                    worker_nodes=active_workers, user_classes=self.user_classes
                )

            logger.info(
                "Sending spawn jobs of %d users at %.2f spawn rate to %d ready workers"
                % (user_count, spawn_rate, num_workers)
            )

            worker_spawn_rate = float(spawn_rate) / (num_workers or 1)
            if worker_spawn_rate > 100:
                logger.warning(
                    "Your selected spawn rate is very high (>100/worker), and this is known to sometimes cause issues. Do you really need to ramp up that fast?"
                )

            target = canonical_spawn_target(user_count, spawn_rate, user_classes)
            current = self._command_ledger.current
            coalesced = (
                current is not None
                and not current.is_terminal
                and current.intent == INTENT_SPAWN
                and current.target == target
            )
            if coalesced:
                # An identical spawn intent is already in flight (e.g. a concurrent /swarm
                # call or a rebalance triggered while dispatching): join its result instead
                # of dispatching a competing generation, and re-drive any worker not acked.
                assert current is not None
                result = current
                generation = result.generation
                for worker_node in active_workers:
                    result.expect(worker_node.id)
                    if worker_node.id not in result.acked_workers and worker_node.last_target:
                        self._send_spawn_to_worker(worker_node.id, worker_node.last_target, generation)
            else:
                result = self._command_ledger.submit(
                    INTENT_SPAWN, target, expected_workers={worker_node.id for worker_node in active_workers}
                )
                self.last_command_result = result
                generation = result.generation
                logger.info("Cluster spawn command submitted with generation %i", generation)

                # Lifecycle gating inside the serialized section: concurrent starts cannot
                # both observe a non-running state, so test_start fires at most once per
                # running generation.
                if self.state != STATE_RUNNING and self.state != STATE_SPAWNING:
                    self.stats.clear_all()
                    self.exceptions = {}
                    self.environment._filter_tasks_by_tags()
                    self.environment.events.test_start.fire(environment=self.environment)
                    if self.environment.shape_class:
                        self.environment.shape_class.reset_time()

                self.update_state(STATE_SPAWNING)

                self._users_dispatcher.new_dispatch(
                    target_user_count=user_count, spawn_rate=spawn_rate, user_classes=user_classes
                )

        if coalesced:
            # A coalesced caller does not own the dispatch: wait bounded for convergence but
            # never terminalize the shared result (the dispatch owner is timeout authority).
            result.wait(self._wait_for_workers_report_after_ramp_up())
            self.last_command_result = result
            return result

        dispatched_users: dict[str, dict[str, int]] = {}
        try:
            for dispatched_users in self._users_dispatcher:
                # A newer intent may have superseded this generation while we were
                # dispatching. Remaining batches would carry the old generation and
                # workers would drop them as stale, so stop sending them. A plain
                # convergence TIMEOUT must NOT stop the ramp: it only bounds how long
                # start() blocks afterwards, and a late ack is still reconciled.
                if self._command_ledger.current is not result:
                    break
                dispatch_greenlets = Group()
                for worker_node_id, worker_user_classes_count in dispatched_users.items():
                    dispatch_greenlets.add(
                        gevent.spawn_later(
                            0,
                            self._send_spawn_to_worker,
                            worker_node_id,
                            dict(worker_user_classes_count),
                            generation,
                        )
                    )
                dispatched_user_count = sum(map(sum, map(methodcaller("values"), dispatched_users.values())))
                logger.debug(
                    "Sending spawn messages for %g total users to %i worker(s)",
                    dispatched_user_count,
                    len(dispatch_greenlets),
                )
                dispatch_greenlets.join()

                logger.debug(
                    f"Currently spawned users: {_format_user_classes_count_for_log(self.reported_user_classes_count)}"
                )

            if self._command_ledger.current is result:
                self.target_user_classes_count = _aggregate_dispatched_users(dispatched_users)

        except KeyboardInterrupt:
            # TODO: Find a cleaner way to handle that
            # We need to catch keyboard interrupt. Otherwise, if KeyboardInterrupt is received while in
            # a gevent.sleep inside the dispatch_users function, locust won't gracefully shutdown.
            self.quit()

        # A superseded generation never owns cluster state: it must not fire
        # spawning_complete (the superseding generation does); hand the result back.
        if result.status == COMMAND_SUPERSEDED:
            logger.info("Spawn generation %i was superseded by a newer intent", generation)
            return result

        # Bounded convergence wait: start() blocks until the generation is confirmed by
        # the workers, superseded by a newer intent, or the report wait-time elapses.
        # The reported user count below is only used for an accurate log message.
        timeout = gevent.Timeout(self._wait_for_workers_report_after_ramp_up())
        timeout.start()
        msg_prefix = "All users spawned"
        try:
            while not result.is_terminal:
                result.wait(0.01)
            if result.status == COMMAND_SUPERSEDED:
                return result
            if self.user_count != self.target_user_count:
                gevent.sleep(0)  # let a final report land before logging
        except gevent.Timeout:
            msg_prefix = (
                "Spawning is complete and report waittime is expired, but not all reports received from workers"
            )
            if not result.is_terminal:
                result.finish(COMMAND_TIMEOUT)
        finally:
            timeout.cancel()

        user_count = sum(self.target_user_classes_count.values())
        self.environment.events.spawning_complete.fire(user_count=user_count)
        # notify workers so they can fire their own event
        self.send_message("spawning_complete", data={"user_count": user_count})
        self.spawning_completed = True

        logger.info(f"{msg_prefix}: {_format_user_classes_count_for_log(self.reported_user_classes_count)}")
        return result

    def _await_command_result(self, result: ClusterCommandResult, timeout: float) -> None:
        """Wait for a generation to reach a terminal state, timing it out if necessary."""
        if result.is_terminal:
            return
        result.wait(timeout)
        if not result.is_terminal:
            # A topology change (worker dropped) may have confirmed it concurrently;
            # only the waiter times the generation out.
            result.finish(COMMAND_TIMEOUT)

    @functools.lru_cache
    def _wait_for_workers_report_after_ramp_up(self) -> float:
        """
        The amount of time to wait after a ramp-up in order for all the workers to report their state
        to the master. If not supplied by the user, it is 1000ms by default. If the supplied value is a number,
        it is taken as-is. If the supplied value is a pattern like "some_number * WORKER_REPORT_INTERVAL",
        the value will be "some_number * WORKER_REPORT_INTERVAL". The most sensible value would be something
        like "1.25 * WORKER_REPORT_INTERVAL". However, some users might find it too high, so it is left
        to a relatively small value of 1000ms by default.
        """
        locust_wait_for_workers_report_after_ramp_up = os.getenv("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP")
        if locust_wait_for_workers_report_after_ramp_up is None:
            return 1.0

        match = re.search(
            r"^(?P<coeff>(\d+)|(\d+\.\d+))[ ]*\*[ ]*WORKER_REPORT_INTERVAL$",
            locust_wait_for_workers_report_after_ramp_up,
        )
        if match is None:
            assert float(locust_wait_for_workers_report_after_ramp_up) >= 0
            return float(locust_wait_for_workers_report_after_ramp_up)
        else:
            return float(match.group("coeff")) * WORKER_REPORT_INTERVAL

    def stop(self, send_stop_to_client: bool = True, _allow_join: bool = True) -> ClusterCommandResult | None:
        # Serialized section: the state check, lifecycle firing and command submission
        # must be one atomic transition so concurrent/re-entrant stop() calls cannot
        # duplicate commands or lifecycle events. The convergence wait is below, outside
        # the lock, to leave start() able to supersede a stalled stop.
        # ``_allow_join`` is False when quit() drives the teardown itself: in that case an
        # in-flight stop owns the lifecycle and quit must neither join nor fire events.
        join_result: ClusterCommandResult | None = None
        result: ClusterCommandResult | None = None
        fire_test_stop = False
        with self._command_lock:
            if self.state in [STATE_INIT, STATE_STOPPED]:
                return None
            if self.state == STATE_STOPPING:
                # A stop is already in flight: join its result instead of sending a new
                # stop generation or firing the lifecycle events a second time.
                existing = self.last_command_result
                if _allow_join and existing is not None and existing.intent == INTENT_STOP:
                    join_result = existing
                # Otherwise (e.g. called from quit() while another stop is converging)
                # there is nothing to submit and no lifecycle event to fire.
            else:
                fire_test_stop = True
                logger.debug("Stopping...")
                self.environment.events.test_stopping.fire(environment=self.environment)
                self.final_user_classes_count = {**self.reported_user_classes_count}
                self.update_state(STATE_STOPPING)

                if (
                    self.environment.shape_class is not None
                    and self.shape_greenlet is not None
                    and self.shape_greenlet is not greenlet.getcurrent()
                ):
                    self.shape_greenlet.kill(block=True)
                    self.shape_greenlet = None
                    self.shape_last_tick = None

                self._users_dispatcher = None

                if send_stop_to_client:
                    result = self._command_ledger.submit(
                        INTENT_STOP, None, expected_workers={client.id for client in self.clients.all}
                    )
                    self.last_command_result = result
                    stop_generation = result.generation
                    logger.info("Cluster stop command submitted with generation %i", stop_generation)
                    for client in self.clients.all:
                        logger.debug(f"Sending stop message to worker {client.id}")
                        self.server.send_to_client(Message("stop", {"generation": stop_generation}, client.id))

        if join_result is not None:
            join_result.wait(self.environment.stop_timeout + 60)
            return join_result

        if result is not None:
            # Wait for workers to acknowledge the generation, bounded by stop_timeout + 60s.
            # A reported user count of zero is accepted as converging evidence as well
            # (workers that vanished cannot acknowledge), matching the historical wait.
            deadline = time.time() + self.environment.stop_timeout + 60
            while not result.is_terminal:
                if self.user_count == 0:
                    result.finish(COMMAND_CONFIRMED)
                    break
                result.wait(min(1.0, max(deadline - time.time(), 0)))
                if time.time() >= deadline:
                    result.finish(COMMAND_TIMEOUT)
                    break
            if result.status == COMMAND_TIMEOUT:
                logger.error("Timeout waiting for all workers to stop")

        # test_stop fires exactly once: only the caller that performed the STOPPING
        # transition above fires it (re-entrant callers return join_result).
        if fire_test_stop:
            self.environment.events.test_stop.fire(environment=self.environment)
        return result

    def quit(self) -> None:
        with self._command_lock:
            self.stop(send_stop_to_client=False, _allow_join=False)
            logger.debug("Quitting...")
            result = self._command_ledger.submit(
                INTENT_QUIT, None, expected_workers={client.id for client in self.clients.all}
            )
            self.last_command_result = result
            quit_generation = result.generation
            for client in self.clients.all:
                logger.debug(f"Sending quit message to worker {client.id} (index {self.get_worker_index(client.id)})")
                self.server.send_to_client(Message("quit", {"generation": quit_generation}, client.id))
        gevent.sleep(0.5)  # wait for final stats report from all workers
        # Workers cannot acknowledge quit (they terminate immediately): delivery to the
        # worker set that was connected at submit time is the terminal fact.
        if not result.is_terminal:
            result.finish(COMMAND_CONFIRMED)
        self.greenlet.kill(block=True)

    def check_stopped(self) -> None:
        if (
            self.state == STATE_STOPPING
            and all(x.state == STATE_INIT for x in self.clients.all)
            or all(x.state not in (STATE_RUNNING, STATE_SPAWNING, STATE_INIT) for x in self.clients.all)
        ):
            self.update_state(STATE_STOPPED)

    def heartbeat_worker(self) -> NoReturn:
        while True:
            gevent.sleep(HEARTBEAT_INTERVAL)
            if self.connection_broken:
                self.reset_connection()
                continue

            missing_clients_to_be_removed = []
            for client in self.clients.all:
                # if clients goes missing for more than HEARTBEAT_DEAD_INTERNAL then add them to be removed list
                if client.state == STATE_MISSING and client.heartbeat <= HEARTBEAT_DEAD_INTERNAL:
                    missing_clients_to_be_removed.append(client.id)

                if client.heartbeat < 0 and client.state != STATE_MISSING:
                    logger.info(f"Worker {str(client.id)} failed to send heartbeat, setting state to missing.")
                    client.state = STATE_MISSING
                    client.user_classes_count = {}
                    # A missing worker cannot acknowledge the current generation; removing it
                    # from the expected set lets the command result converge with the live set.
                    self._command_ledger.drop_worker(client.id)
                    if self._users_dispatcher is not None:
                        self._users_dispatcher.remove_worker(client)
                        if self.rebalancing_enabled() and self.state == STATE_RUNNING and self.spawning_completed:
                            self.start(self.target_user_count, self.spawn_rate)
                    if self.worker_count <= 0:
                        logger.info("The last worker went missing, stopping test.")
                        self.stop()
                        self.check_stopped()
                else:
                    client.heartbeat -= 1

            # if there are any missing clients to be removed then remove them and trigger rebalance.
            if len(missing_clients_to_be_removed) > 0:
                for to_remove_client_id in missing_clients_to_be_removed:
                    if self.clients.get(to_remove_client_id) is not None:
                        self._command_ledger.drop_worker(to_remove_client_id)
                        del self.clients[to_remove_client_id]
                if self.state == STATE_RUNNING or self.state == STATE_SPAWNING:
                    # _users_dispatcher is set to none so that during redistribution the dead clients are not picked, alternative is to call self.stop() before start
                    self._users_dispatcher = None
                    # trigger redistribution after missing cclient removal
                    self.start(user_count=self.target_user_count, spawn_rate=self.spawn_rate)

    def reset_connection(self) -> None:
        logger.info("Resetting RPC server and all worker connections.")
        try:
            self.server.close(linger=0)
            self.server = rpc.Server(self.master_bind_host, self.master_bind_port)
            self.connection_broken = False
        except RPCError as e:
            logger.error(f"Temporary failure when resetting connection: {e}, will retry later.")

    def client_listener(self) -> NoReturn:
        while True:
            try:
                client_id, msg = self.server.recv_from_client()
            except RPCReceiveError as e:
                client_id = e.addr

                if client_id and client_id in self.clients:
                    logger.error(f"RPCError when receiving from client: {e}. Will reset client {client_id}.")
                    try:
                        self.server.send_to_client(Message("reconnect", None, client_id))
                    except Exception as error:
                        logger.error(f"Error sending reconnect message to worker: {error}. Will reset RPC server.")
                        self.connection_broken = True
                        gevent.sleep(FALLBACK_INTERVAL)
                        continue
                else:
                    message = f"{e}" if not client_id else f"{e} from {client_id}"
                    logger.error(f"Unrecognized message detected: {message}")
                    continue
            except RPCSendError as e:
                logger.error(f"Error sending reconnect message to worker: {e}. Will reset RPC server.")
                self.connection_broken = True
                gevent.sleep(FALLBACK_INTERVAL)
                continue
            except RPCError as e:
                if self.clients.ready or self.clients.spawning or self.clients.running:
                    logger.error(f"RPCError: {e}. Will reset RPC server.")
                else:
                    logger.debug(
                        f"RPCError when receiving from worker: {e} (but no workers were expected to be connected anyway)"
                    )
                self.connection_broken = True
                gevent.sleep(FALLBACK_INTERVAL)
                continue
            except KeyboardInterrupt:
                logging.debug(
                    "Got KeyboardInterrupt in client_listener. Other greenlets should catch this and shut down."
                )
                continue
            self.handle_message(client_id, msg)

    def handle_message(self, client_id: str, msg: Message) -> None:
        match msg.type:
            case "client_ready":
                worker_version, worker_generation, worker_state = parse_client_ready_data(msg.data)
                if worker_version is None:
                    logger.error(
                        f"An old worker (no cluster command generation support) tried to connect ({client_id}). That's not going to work."
                    )
                    return
                elif worker_version != __version__ and worker_version != -1:
                    if str(worker_version)[0:4] == __version__[0:4]:
                        logger.debug(
                            f"A worker ({client_id}) running a different patch version ({repr(worker_version)}) connected, master version is {repr(__version__)}"
                        )
                    else:
                        logger.warning(
                            f"A worker ({client_id}) running a different version ({worker_version}) connected, master version is {__version__}"
                        )
                self.send_message("ack", client_id=client_id, data=self._handshake_ack_data(client_id))
                self.environment.events.worker_connect.fire(client_id=msg.node_id)
                client_already_connected = client_id in self.clients
                new_node = WorkerNode(client_id, heartbeat_liveness=HEARTBEAT_LIVENESS)
                new_node.last_generation = worker_generation
                if client_already_connected and worker_state is not None:
                    # A reconnecting worker (duplicate client_ready): preserve what we know about
                    # its running users, trust its reported state and recorded generation until the
                    # next heartbeat reconciles it.
                    existing = self.clients[client_id]
                    new_node.state = worker_state
                    new_node.user_classes_count = existing.user_classes_count
                    new_node.last_target = existing.last_target
                    if self._command_ledger.is_current(existing.last_generation):
                        new_node.last_generation = max(worker_generation, existing.last_generation)
                self.clients[client_id] = new_node
                if not client_already_connected:
                    # A freshly connected worker must acknowledge any in-flight generation
                    self._command_ledger.expect_worker(client_id)
                if self._users_dispatcher is not None:
                    self._users_dispatcher.add_worker(worker_node=self.clients[client_id])
                    if not self._users_dispatcher.dispatch_in_progress and self.state == STATE_RUNNING:
                        # TODO: Test this situation
                        self.start(self.target_user_count, self.spawn_rate)
                if client_already_connected:
                    logger.debug(
                        f"{client_id} (index {self.get_worker_index(client_id)}) reported as ready (duplicate message). {len(self.clients.ready + self.clients.running + self.clients.spawning)} workers connected."
                    )
                else:
                    logger.info(
                        f"{client_id} (index {self.get_worker_index(client_id)}) reported as ready. {len(self.clients.ready + self.clients.running + self.clients.spawning)} workers connected."
                    )
                if self.rebalancing_enabled() and self.state == STATE_RUNNING and self.spawning_completed:
                    self.start(self.target_user_count, self.spawn_rate)
                # Reconcile a reconnecting/new worker that reported an older watermark
                # with the current generation (idempotent no-op when already aligned).
                self._resync_worker(client_id)
                # emit a warning if the worker's clock seem to be out of sync with our clock
                # if abs(time() - msg.data["time"]) > 5.0:
                #    warnings.warn("The worker node's clock seem to be out of sync. For the statistics to be correct the different locust servers need to have synchronized clocks.")
            case "locustfile":
                if not msg.data["version"]:
                    logger.error("A very old worker version requested locustfile. This probably won't work.")
                elif msg.data["version"][0:4] == __version__[0:4]:
                    logger.debug(
                        f"A worker ({msg.node_id}) running a different patch version ({msg.data['version']}) connected, master version is {__version__}"
                    )

                logging.debug("Worker requested locust file")
                assert self.environment.parsed_locustfiles
                locustfile_options = self.environment.parsed_locustfiles
                locustfile_list = [f.strip() for f in locustfile_options if not os.path.isdir(f)]

                for locustfile_option in locustfile_options:
                    if os.path.isdir(locustfile_option):
                        locustfile_list.extend(get_abspaths_in(locustfile_option, extension=".py"))

                try:
                    locustfiles: list[str | dict[str, str]] = []

                    for filename in locustfile_list:
                        if is_url(filename):
                            locustfiles.append(filename)
                        else:
                            with open(filename) as f:
                                filename = os.path.basename(filename)
                                file_contents = f.read()

                            locustfiles.append({"filename": filename, "contents": file_contents})
                except Exception as e:
                    error_message = "locustfile must be a full path to a single locustfile, a comma-separated list of .py files, or a URL for file distribution to work"
                    logger.error(f"{error_message} {e}")
                    self.send_message(
                        "locustfile",
                        client_id=client_id,
                        data={"error": f"{error_message} (was '{filename}')"},
                    )
                else:
                    self.send_message(
                        "locustfile",
                        client_id=client_id,
                        data={"locustfiles": locustfiles},
                    )
                return
            case "client_stopped":
                if msg.node_id not in self.clients:
                    logger.warning(f"Received {msg.type} message from an unknown worker: {msg.node_id}.")
                    return
                stopped_generation = msg.data.get("generation", -1) if isinstance(msg.data, dict) else -1
                client = self.clients[msg.node_id]
                client.last_generation = max(client.last_generation, stopped_generation)
                self._command_ledger.observe_ack(msg.node_id, stopped_generation, msg.data)
                del self.clients[msg.node_id]
                if self._users_dispatcher is not None:
                    self._users_dispatcher.remove_worker(client)
                    if not self._users_dispatcher.dispatch_in_progress and self.state == STATE_RUNNING:
                        # TODO: Test this situation
                        self.start(self.target_user_count, self.spawn_rate)
                logger.info(f"{msg.node_id} (index {self.get_worker_index(client_id)}) reported that it has stopped")
            case "heartbeat":
                if msg.node_id in self.clients:
                    c = self.clients[msg.node_id]
                    c.heartbeat = HEARTBEAT_LIVENESS
                    client_state = msg.data["state"]
                    if c.state == STATE_MISSING:
                        logger.info(f"Worker {str(c.id)} self-healed with heartbeat, setting state to {client_state}.")
                        if self._users_dispatcher is not None:
                            self._users_dispatcher.add_worker(worker_node=c)
                            if not self._users_dispatcher.dispatch_in_progress and self.state == STATE_RUNNING:
                                # TODO: Test this situation
                                self.start(self.target_user_count, self.spawn_rate)
                    c.state = client_state
                    if "generation" in msg.data:
                        c.last_generation = max(c.last_generation, int(msg.data["generation"]))
                    c.cpu_usage = msg.data["current_cpu_usage"]
                    if not c.cpu_warning_emitted and c.cpu_usage > 90:
                        self.worker_cpu_warning_emitted = True  # used to fail the test in the end
                        c.cpu_warning_emitted = True  # used to suppress logging for this node
                        logger.warning(
                            f"Worker {msg.node_id} (index {self.get_worker_index(msg.node_id)}) exceeded cpu threshold (will only log this once per worker)"
                        )
                    if "current_memory_usage" in msg.data:
                        c.memory_usage = msg.data["current_memory_usage"]
                    self.environment.events.heartbeat_sent.fire(client_id=msg.node_id, timestamp=time.time())
                    self.server.send_to_client(Message("heartbeat", None, msg.node_id))
                else:
                    logging.debug(f"Got heartbeat message from unknown worker {msg.node_id}")
            case "stats":
                self.environment.events.worker_report.fire(client_id=msg.node_id, data=msg.data)
            case "spawning":
                try:
                    self.clients[msg.node_id].state = STATE_SPAWNING
                except KeyError:
                    logger.warning(f"Got spawning message from unknown worker {msg.node_id}. Asking worker to quit.")
                    self.server.send_to_client(
                        Message("quit", {"generation": self._command_ledger.generation}, msg.node_id)
                    )
            case "spawning_complete":
                # a worker finished spawning (this happens multiple times during rampup)
                node = self.clients[msg.node_id]
                node.state = STATE_RUNNING
                node.user_classes_count = msg.data["user_classes_count"]
                spawned_generation = msg.data.get("generation", -1)
                node.last_generation = max(node.last_generation, spawned_generation)
                self._command_ledger.observe_ack(msg.node_id, spawned_generation, msg.data)
            case "logs":
                self.environment.update_worker_logs(msg.data)
            case "quit":
                if msg.node_id in self.clients:
                    client = self.clients[msg.node_id]
                    del self.clients[msg.node_id]
                    if self._users_dispatcher is not None:
                        self._users_dispatcher.remove_worker(client)
                        if not self._users_dispatcher.dispatch_in_progress and self.state == STATE_RUNNING:
                            # TODO: Test this situation
                            self.start(self.target_user_count, self.spawn_rate)
                    logger.info(
                        f"Worker {msg.node_id!r} (index {self.get_worker_index(msg.node_id)}) quit. {len(self.clients.ready)} workers ready."
                    )
                    if self.worker_count <= 0:
                        logger.info("The last worker quit, stopping test.")
                        self.stop()
                        if self.environment.parsed_options and self.environment.parsed_options.headless:
                            self.quit()
            case "command_ack":
                # Generation-correlated acknowledgement that a worker applied an intent
                if msg.node_id not in self.clients:
                    logger.debug("Got command_ack from unknown worker %s", msg.node_id)
                    return
                acked_generation = int(msg.data.get("generation", -1))
                self.clients[msg.node_id].last_generation = max(
                    self.clients[msg.node_id].last_generation, acked_generation
                )
                routed = self._command_ledger.observe_ack(msg.node_id, acked_generation, msg.data)
                if routed is None and acked_generation < self._command_ledger.generation:
                    # Worker answered for an old generation: drive it to the current intent
                    self._resync_worker(msg.node_id)
            case "command_stale":
                # The worker discarded one of our commands because it already applied a
                # newer generation. Record its watermark and re-drive it to current intent.
                worker_generation = msg.data.get("generation", -1) if msg.data else -1
                if msg.node_id in self.clients:
                    self.clients[msg.node_id].last_generation = max(
                        self.clients[msg.node_id].last_generation, worker_generation
                    )
                logger.info(
                    "Worker %s discarded a stale command (its generation: %s, current: %i)",
                    msg.node_id,
                    worker_generation,
                    self._command_ledger.generation,
                )
                self._resync_worker(msg.node_id)
            case "exception":
                self.log_exception(msg.node_id, msg.data["msg"], msg.data["traceback"])
            case _ if lc := self.custom_messages.get(msg.type):
                listener, concurrent = lc
                logger.debug(
                    f"Received {msg.type} message from worker {msg.node_id} (index {self.get_worker_index(msg.node_id)})"
                )
                try:
                    if not concurrent:
                        listener(environment=self.environment, msg=msg)
                    else:
                        gevent.spawn(listener, environment=self.environment, msg=msg)
                except Exception:
                    logging.error(f"Uncaught exception in handler for {msg.type}\n{traceback.format_exc()}")
            case _:
                logger.warning(
                    f"Unknown message type received from worker {msg.node_id} (index {self.get_worker_index(msg.node_id)}): {msg.type}"
                )

        self.check_stopped()

    @property
    def worker_count(self) -> int:
        return len(self.clients.ready) + len(self.clients.spawning) + len(self.clients.running)

    @property
    def reported_user_classes_count(self) -> dict[str, int]:
        reported_user_classes_count: dict[str, int] = defaultdict(int)
        for client in self.clients.ready + self.clients.spawning + self.clients.running:
            for name, count in client.user_classes_count.items():
                reported_user_classes_count[name] += count
        return reported_user_classes_count

    def send_message(self, msg_type: str, data: dict[str, Any] | None = None, client_id: str | None = None):
        """
        Sends a message to attached worker node(s)

        :param msg_type: The type of the message to send
        :param data: Optional data to send
        :param client_id: Optional id of the target worker node.
                            If None, will send to all attached workers
        """
        if client_id:
            logger.debug(f"Sending {msg_type} message to worker {client_id}")
            self.server.send_to_client(Message(msg_type, data, client_id))
        else:
            for client in self.clients.all:
                logger.debug(f"Sending {msg_type} message to worker {client.id}")
                self.server.send_to_client(Message(msg_type, data, client.id))


class WorkerRunner(DistributedRunner):
    """
    Runner used to run distributed load tests across multiple processes and/or machines.

    WorkerRunner connects to a :class:`MasterRunner` from which it'll receive
    instructions to start and stop user greenlets. The WorkerRunner will periodically
    take the stats generated by the running users and send back to the :class:`MasterRunner`.
    """

    # the worker index is set on ACK, if master provided it (masters <= 2.10.2 do not provide it)
    worker_index = -1

    def __init__(self, environment: Environment, master_host: str, master_port: int) -> None:
        """
        :param environment: Environment instance
        :param master_host: Host/IP to use for connection to the master
        :param master_port: Port to use for connecting to the master
        """
        super().__init__(environment)
        self.retry = 0
        self.connected = False
        self.last_heartbeat_timestamp: float | None = None
        self.connection_event = Event()
        self.worker_state = STATE_INIT
        # Cluster command ordering: highest applied generation, its intent fingerprint
        # ("spawn"/"stop"/"quit" + target) and the generation whose test_start has fired.
        self._applied_generation: int = -1
        self._applied_target: Any = None
        self._running_generation: int | None = None
        # Master's view reported in the latest ack (None before the first handshake)
        self._master_intent: str | None = None
        self._master_generation: int = -1
        self.client_id = socket.gethostname() + "_" + uuid4().hex
        self.master_host = master_host
        self.master_port = master_port
        self.web_base_path = environment.parsed_options.web_base_path if environment.parsed_options else ""
        self.logs: list[str] = []
        self.worker_cpu_warning_emitted = False
        self._users_dispatcher: UsersDispatcher | None = None
        self.client = rpc.Client(master_host, master_port, self.client_id)
        self.greenlet.spawn(self.worker).link_exception(locust_exception_handler(self.environment))
        self.connect_to_master()
        self.greenlet.spawn(self.heartbeat).link_exception(locust_exception_handler(self.environment))
        self.greenlet.spawn(self.heartbeat_timeout_checker).link_exception(locust_exception_handler(self.environment))
        self.greenlet.spawn(self.stats_reporter).link_exception(locust_exception_handler(self.environment))
        self.greenlet.spawn(self.logs_reporter).link_exception(locust_exception_handler(self.environment))

        # register listener that adds the current number of spawned users to the report that is sent to the master node
        def on_report_to_master(client_id: str, data: dict[str, Any]):
            data["user_classes_count"] = self.user_classes_count
            data["user_count"] = self.user_count

        self.environment.events.report_to_master.add_listener(on_report_to_master)

        # register listener that sends quit message to master
        def on_quitting(environment: Environment, **kw) -> None:
            self.client.send(Message("quit", None, self.client_id))

        self.environment.events.quitting.add_listener(on_quitting)

        # register listener that's sends user exceptions to master
        def on_user_error(user_instance: User, exception: Exception, tb: TracebackType) -> None:
            formatted_tb = "".join(traceback.format_tb(tb))
            self.client.send(Message("exception", {"msg": str(exception), "traceback": formatted_tb}, self.client_id))

        self.environment.events.user_error.add_listener(on_user_error)

    def spawning_complete(self, user_count, generation: int | None = None):
        assert user_count == sum(self.user_classes_count.values())
        self.client.send(
            Message(
                "spawning_complete",
                {
                    "user_classes_count": self.user_classes_count,
                    "user_count": self.user_count,
                    "generation": self._applied_generation if generation is None else generation,
                },
                self.client_id,
            )
        )
        self.worker_state = STATE_RUNNING

    def start(
        self, user_count: int, spawn_rate: float, wait: bool = False, user_classes: list[type[User]] | None = None
    ) -> None:
        raise NotImplementedError("use start_worker")

    def start_worker(self, user_classes_count: dict[str, int], *, generation: int) -> None:
        """
        Start running a load test as a worker

        :param user_classes_count: Users to run
        :param generation: Cluster command generation this dispatch belongs to
        """
        self.target_user_classes_count = user_classes_count
        self.target_user_count = sum(user_classes_count.values())

        for user_class in self.user_classes:
            if self.environment.host:
                user_class.host = self.environment.host

        user_classes_spawn_count: dict[str, int] = {}
        user_classes_stop_count: dict[str, int] = {}

        for user_class_name, user_class_count in user_classes_count.items():
            if self.user_classes_count[user_class_name] > user_class_count:
                user_classes_stop_count[user_class_name] = self.user_classes_count[user_class_name] - user_class_count
            elif self.user_classes_count[user_class_name] < user_class_count:
                user_classes_spawn_count[user_class_name] = user_class_count - self.user_classes_count[user_class_name]

        # call spawn_users before stopping the users since stop_users
        # can be blocking because of the stop_timeout
        self.spawn_users(user_classes_spawn_count)
        self.stop_users(user_classes_stop_count)
        self.spawning_complete(sum(self.user_classes_count.values()), generation)
        # Explicit, generation-correlated acknowledgement of an applied cluster command
        self._send_command_ack(generation, INTENT_SPAWN)
        self.update_state(STATE_RUNNING)
        self.worker_state = STATE_RUNNING

    def _classify_command(self, generation: int, intent: str, fingerprint: Any) -> str:
        """
        Decide what to do with an incoming state command.

        - ``stale``: generation older than the applied watermark, must be discarded
        - ``noop``: same generation and same applied intent/target, acknowledge idempotently
        - ``apply``: new generation, or same generation with a changed target (rebalance)
        """
        if generation < self._applied_generation:
            return "stale"
        if generation == self._applied_generation and self._applied_target == (intent, fingerprint):
            return "noop"
        return "apply"

    def _send_command_stale(self) -> None:
        self.client.send(
            Message(
                "command_stale",
                {"generation": self._applied_generation, "state": self.worker_state},
                self.client_id,
            )
        )

    def _send_command_ack(self, generation: int, intent: str) -> None:
        self.client.send(
            Message(
                "command_ack",
                {
                    "generation": generation,
                    "intent": intent,
                    "state": self.worker_state,
                    "user_classes_count": self.user_classes_count,
                    "user_count": self.user_count,
                },
                self.client_id,
            )
        )

    def heartbeat(self) -> NoReturn:
        while True:
            try:
                self.client.send(
                    Message(
                        "heartbeat",
                        {
                            "state": self.worker_state,
                            "current_cpu_usage": self.current_cpu_usage,
                            "current_memory_usage": self.current_memory_usage,
                            "generation": self._applied_generation,
                        },
                        self.client_id,
                    )
                )
            except RPCError as e:
                logger.error(f"RPCError found when sending heartbeat: {e}")
                self.reset_connection()
            gevent.sleep(HEARTBEAT_INTERVAL)

    def heartbeat_timeout_checker(self) -> NoReturn:
        while True:
            gevent.sleep(1)
            if self.last_heartbeat_timestamp and self.last_heartbeat_timestamp < time.time() - MASTER_HEARTBEAT_TIMEOUT:
                logger.error(f"Didn't get heartbeat from master in over {MASTER_HEARTBEAT_TIMEOUT}s")
                self.quit()

    def reset_connection(self) -> None:
        logger.info("Reset connection to master")
        try:
            self.client.close()
            self.client = rpc.Client(self.master_host, self.master_port, self.client_id)
        except RPCError as e:
            logger.error(f"Temporary failure when resetting connection: {e}, will retry later.")

    def worker(self) -> NoReturn:
        while True:
            try:
                msg = self.client.recv()
            except RPCError as e:
                logger.error(f"RPCError found when receiving from master: {e}")
            else:
                self.handle_message(msg)

    def handle_message(self, msg: Message) -> None:
        match msg.type:
            case "ack":
                # backward-compatible support of masters that do not send a worker index
                if msg.data is not None and "index" in msg.data:
                    self.worker_index = msg.data["index"]
                if isinstance(msg.data, dict) and "generation" in msg.data:
                    master_generation = int(msg.data["generation"])
                    self._master_generation = master_generation
                    self._master_intent = msg.data.get("intent")
                    if master_generation < self._applied_generation:
                        # A restarted/reset master speaks for a lower generation; adopt
                        # its authority and let post-handshake reconciliation converge
                        # us to its current intent instead of discarding it as stale.
                        logger.info(
                            "Master generation %i is older than applied generation %i, adopting master authority after reconnect",
                            master_generation,
                            self._applied_generation,
                        )
                        self._applied_generation = master_generation
                        self._applied_target = None
                        self._running_generation = None
                self.connection_event.set()
            case "spawn":
                job = msg.data
                generation = int(job.get("generation", -1))
                fingerprint = tuple(sorted(job["user_classes_count"].items()))
                decision = self._classify_command(generation, INTENT_SPAWN, fingerprint)
                if decision == "stale":
                    logger.info(
                        "Discarding stale spawn (generation %i), already applied generation %i",
                        generation,
                        self._applied_generation,
                    )
                    self._send_command_stale()
                    return
                if decision == "noop":
                    logger.debug(
                        "Duplicate spawn for generation %i with the same target, re-acknowledging", generation
                    )
                    self._send_command_ack(generation, INTENT_SPAWN)
                    return

                self.client.send(Message("spawning", {"generation": generation}, self.client_id))
                self.environment.host = job["host"]
                self.environment.stop_timeout = job["stop_timeout"] or 0.0

                # receive custom arguments
                if self.environment.parsed_options is None:
                    default_parser = argument_parser.get_empty_argument_parser()
                    argument_parser.setup_parser_arguments(default_parser)
                    self.environment.parsed_options = default_parser.parse(args=[])
                custom_args_from_master = {
                    k: v
                    for k, v in job["parsed_options"].items()
                    if k not in argument_parser.default_args_dict()
                    # these settings are sometimes needed on workers
                    or k in ["expect_workers", "tags", "exclude_tags"]
                }
                vars(self.environment.parsed_options).update(custom_args_from_master)

                # Lifecycle fires at most once per running generation, including across
                # same-generation rebalances (changed target) and reconnect re-dispatches.
                if self._running_generation != generation:
                    self.stats.clear_all()
                    self.exceptions = {}
                    self.cpu_warning_emitted = False
                    self.worker_cpu_warning_emitted = False
                    self.environment._filter_tasks_by_tags()
                    self.environment.events.test_start.fire(environment=self.environment)
                    self._running_generation = generation

                self.worker_state = STATE_SPAWNING

                if self.spawning_greenlet:
                    # kill existing spawning greenlet before we launch new one
                    self.spawning_greenlet.kill(block=True)
                self.spawning_greenlet = self.greenlet.spawn(
                    lambda: self.start_worker(job["user_classes_count"], generation=generation)
                )
                self.spawning_greenlet.link_exception(locust_exception_handler(self.environment))
                self._applied_generation = generation
                self._applied_target = (INTENT_SPAWN, fingerprint)
            case "stop":
                if isinstance(msg.data, dict) and "generation" in msg.data:
                    stop_generation = int(msg.data["generation"])
                    decision = self._classify_command(stop_generation, INTENT_STOP, None)
                else:
                    # Legacy payloads without a generation are treated as newer-than-watermark
                    stop_generation = max(self._applied_generation + 1, 0)
                    decision = "apply"
                if decision == "stale":
                    logger.info(
                        "Discarding stale stop (generation %i), already applied generation %i",
                        stop_generation,
                        self._applied_generation,
                    )
                    self._send_command_stale()
                    return
                if decision == "noop":
                    logger.debug("Duplicate stop for generation %i, re-acknowledging", stop_generation)
                    self._send_command_ack(stop_generation, INTENT_STOP)
                    return

                self._applied_generation = stop_generation
                self._applied_target = (INTENT_STOP, None)
                if self._running_generation is not None:
                    self.stop()
                    self._running_generation = None
                self._send_command_ack(stop_generation, INTENT_STOP)
                self.client.send(Message("client_stopped", {"generation": stop_generation}, self.client_id))
                # +additional_wait is just a small buffer to account for the random network latencies and/or other
                # random delays inherent to distributed systems.
                additional_wait = int(os.getenv("LOCUST_WORKER_ADDITIONAL_WAIT_BEFORE_READY_AFTER_STOP", 0))
                gevent.sleep(self.environment.stop_timeout + additional_wait)
                self.worker_state = STATE_INIT
                self.client.send(
                    Message(
                        "client_ready",
                        make_client_ready_data(__version__, self._applied_generation, self.worker_state),
                        self.client_id,
                    )
                )
            case "quit":
                quit_generation = (
                    int(msg.data["generation"]) if isinstance(msg.data, dict) and "generation" in msg.data else None
                )
                if quit_generation is not None and quit_generation < self._applied_generation:
                    logger.info(
                        "Discarding stale quit (generation %i), already applied generation %i",
                        quit_generation,
                        self._applied_generation,
                    )
                    self._send_command_stale()
                    return
                if quit_generation is not None:
                    self._applied_generation = quit_generation
                    self._applied_target = (INTENT_QUIT, None)
                logger.info("Got quit message from master, shutting down...")
                if self._running_generation is not None:
                    self.stop()
                    self._running_generation = None
                self._send_stats()  # send a final report, in case there were any samples not yet reported
                self.greenlet.kill(block=True)
            case "reconnect":
                logger.warning("Received reconnect message from master. Resetting RPC connection.")
                self.reset_connection()
            case "heartbeat":
                self.last_heartbeat_timestamp = time.time()
                self.environment.events.heartbeat_received.fire(
                    client_id=msg.node_id, timestamp=self.last_heartbeat_timestamp
                )
            case "update_user_class":
                self.environment.update_user_class(msg.data)
            case "spawning_complete":
                # master says we have finished spawning (happens only once during a normal rampup)
                self.environment.events.spawning_complete.fire(user_count=msg.data["user_count"])
            case _ if lc := self.custom_messages.get(msg.type):
                listener, concurrent = lc
                logger.debug(f"Received {msg.type} message from master")
                if not concurrent:
                    listener(environment=self.environment, msg=msg)
                else:
                    gevent.spawn(listener, self.environment, msg)
            case _:
                logger.warning(f"Unknown message type received: {msg.type}")

    def stats_reporter(self) -> NoReturn:
        while True:
            try:
                self._send_stats()
            except RPCError as e:
                logger.error(f"Temporary connection lost to master server: {e}, will retry later.")
            gevent.sleep(WORKER_REPORT_INTERVAL)

    def logs_reporter(self) -> None:
        if WORKER_LOG_REPORT_INTERVAL < 0:
            return

        while True:
            current_logs = get_logs()

            if (len(current_logs) - len(self.logs)) > 70:
                logger.warning(
                    "The worker attempted to send more than 70 log lines in one interval. Further log sending was disabled for this worker."
                )
                self._send_logs(get_logs())
                break
            if len(current_logs) > len(self.logs):
                self._send_logs(current_logs)

            self.logs = current_logs
            gevent.sleep(WORKER_LOG_REPORT_INTERVAL)

    def send_message(self, msg_type: str, data: dict[str, Any] | None = None, client_id: str | None = None) -> None:
        """
        Sends a message to master node

        :param msg_type: The type of the message to send
        :param data: Optional data to send
        :param client_id: (unused)
        """
        logger.debug(f"Sending {msg_type} message to master")
        self.client.send(Message(msg_type, data, self.client_id))

    def _send_stats(self) -> None:
        data: dict[str, Any] = {}
        self.environment.events.report_to_master.fire(client_id=self.client_id, data=data)
        self.client.send(Message("stats", data, self.client_id))

    def _send_logs(self, current_logs) -> None:
        self.send_message("logs", {"worker_id": self.client_id, "logs": current_logs})

    def connect_to_master(self):
        self.retry += 1
        self.client.send(
            Message(
                "client_ready",
                make_client_ready_data(__version__, self._applied_generation, self.worker_state),
                self.client_id,
            )
        )
        try:
            success = self.connection_event.wait(timeout=CONNECT_TIMEOUT)
        except KeyboardInterrupt:
            # dont complain about getting CTRL-C
            sys.exit(1)
        if not success:
            if self.retry < 30 / CONNECT_TIMEOUT:  # lower log level during the first 30 seconds
                logger.debug(
                    f"Failed to connect to master {self.master_host}:{self.master_port}{self.web_base_path}, retry {self.retry}/{CONNECT_RETRY_COUNT}."
                )
            else:
                logger.info(
                    f"Failed to connect to master {self.master_host}:{self.master_port}{self.web_base_path}, retry {self.retry}/{CONNECT_RETRY_COUNT}."
                )
            if self.retry > CONNECT_RETRY_COUNT:
                raise ConnectionError()
            self.connect_to_master()
        self.connected = True


def _format_user_classes_count_for_log(user_classes_count: dict[str, int]) -> str:
    return "{} ({} total users)".format(  # noqa: UP032
        json.dumps(dict(sorted(user_classes_count.items(), key=itemgetter(0)))),
        sum(user_classes_count.values()),
    )


def _aggregate_dispatched_users(d: dict[str, dict[str, int]]) -> dict[str, int]:
    # TODO: Test it
    user_classes = list(next(iter(d.values())).keys())
    return {u: sum(worker_counts[u] for worker_counts in d.values()) for u in user_classes}

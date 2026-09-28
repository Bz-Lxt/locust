from __future__ import annotations

import locust
from locust import LoadTestShape, __version__, constant, runners
from locust.argument_parser import get_parser
from locust.dispatch import UsersDispatcher
from locust.env import Environment
from locust.exception import RPCError, RPCReceiveError, StopUser
from locust.log import LogReader
from locust.main import create_environment
from locust.rpc import Message
from locust.runners import (
    STATE_INIT,
    STATE_MISSING,
    STATE_RUNNING,
    STATE_SPAWNING,
    STATE_STOPPED,
    STATE_STOPPING,
    LocalRunner,
    WorkerNode,
    WorkerRunner,
)
from locust.stats import RequestStats
from locust.user import TaskSet, User, task

import json
import logging
import random
import time
import unittest
from collections import defaultdict, deque
from operator import itemgetter
from unittest import mock

import gevent
import requests
from gevent import sleep
from gevent.pool import Group
from gevent.queue import Queue
from retry import retry  # type: ignore

from .testcases import LocustTestCase
from .util import patch_env

NETWORK_BROKEN = "network broken"
BAD_MESSAGE = "bad message"
UNRECOGNIZED_HOST_MESSAGE = "unrecognized host message"
UNRECOGNIZED_MESSAGE = "unrecognized message"


def mocked_rpc(raise_on_close=True):
    class MockedRpcServerClient:
        queue = Queue()
        outbox = []
        raise_error_on_close = raise_on_close

        def __init__(self, *args, **kwargs):
            pass

        @classmethod
        def mocked_send(cls, message):
            cls.queue.put(message.serialize())
            sleep(0)

        def recv(self):
            results = self.queue.get()
            msg = Message.unserialize(results)
            if msg.data == NETWORK_BROKEN:
                raise RPCError()
            return msg

        def send(self, message):
            self.outbox.append(message)

        def send_to_client(self, message):
            self.outbox.append(message)

        @classmethod
        def get_messages(cls, message_type=None) -> list:
            return [message for message in cls.outbox if message_type is None or message.type == message_type]

        def recv_from_client(self):
            results = self.queue.get()
            msg = Message.unserialize(results)
            if msg.data == NETWORK_BROKEN:
                raise RPCError()
            if msg.data == BAD_MESSAGE:
                raise RPCReceiveError(BAD_MESSAGE, addr=msg.node_id)
            if msg.data == UNRECOGNIZED_HOST_MESSAGE:
                raise RPCReceiveError(UNRECOGNIZED_HOST_MESSAGE, addr="FAKE")
            if msg.data == UNRECOGNIZED_MESSAGE:
                raise RPCReceiveError(UNRECOGNIZED_MESSAGE)
            return msg.node_id, msg

        def close(self, linger=None):
            if self.raise_error_on_close:
                raise RPCError()
            else:
                pass

    return MockedRpcServerClient


class mocked_options:
    def __init__(self):
        self.spawn_rate = 5
        self.num_users = 5
        self.host = "/"
        self.tags = None
        self.exclude_tags = None
        self.master_host = "localhost"
        self.master_port = 5557
        self.master_bind_host = "*"
        self.master_bind_port = 5557
        self.heartbeat_liveness = 3
        self.heartbeat_interval = 1
        self.stop_timeout = 0.0
        self.connection_broken = False
        self.profile = None

    def reset_stats(self):
        pass


class HeyAnException(Exception):
    pass


class LocustRunnerTestCase(LocustTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.runner_stopping = False
        self.runner_stopped = False

    def setUp(self):
        super().setUp()
        self.reset_state()

    def reset_state(self):
        self.runner_stopping = False
        self.runner_stopped = False


class TestLocustRunner(LocustRunnerTestCase):
    def test_missing_constructor_call_in_user(self):
        class BadUser(User):
            def __init__(self, *args, **kwargs):
                pass  # not calling base class constructor!!!

            @task
            def t(self):
                pass

        environment = Environment(user_classes=[BadUser])
        runner = LocalRunner(environment)
        with self.assertRaises(AssertionError) as assert_raises_context:
            runner.spawn_users({BadUser.__name__: 1})
        self.assertIn(
            "Attribute 'environment' is missing on user BadUser. Perhaps you defined your own __init__ and forgot to call the base constructor",
            str(assert_raises_context.exception),
        )

    def test_cpu_warning(self):
        _monitor_interval = runners.CPU_MONITOR_INTERVAL
        runners.CPU_MONITOR_INTERVAL = 0.1
        try:

            class CpuUser(User):
                @task
                def cpu_task(self):
                    for i in range(10):
                        for j in range(1000000):
                            _ = 3 / 2
                        time.sleep(0.0001)  # let other greenlets run, like the cpu monitor

            environment = Environment(user_classes=[CpuUser])
            environment._cpu_warning_event_triggered = False

            def cpu_warning(environment, cpu_usage, **kwargs):
                environment._cpu_warning_event_triggered = True
                environment._cpu_usage = cpu_usage

            environment.events.cpu_warning.add_listener(cpu_warning)
            runner = LocalRunner(environment)
            time.sleep(0.2)  # let first checks run
            self.assertFalse(runner.cpu_warning_emitted)
            runner.spawn_users({CpuUser.__name__: 1}, wait=False)
            sleep(2.5)
            self.assertTrue(environment._cpu_warning_event_triggered)
            self.assertGreater(environment._cpu_usage, 90)
            runner.quit()
            self.assertTrue(runner.cpu_warning_emitted)
        finally:
            runners.CPU_MONITOR_INTERVAL = _monitor_interval

    def test_kill_locusts(self):
        triggered = [False]

        class BaseUser(User):
            wait_time = constant(1)

            @task
            class task_set(TaskSet):
                @task
                def trigger(self):
                    triggered[0] = True

        runner = Environment(user_classes=[BaseUser]).create_local_runner()
        users = runner.spawn_users({BaseUser.__name__: 2}, wait=False)
        self.assertEqual(2, len(users))
        self.assertEqual(2, len(runner.user_greenlets))
        g1 = list(runner.user_greenlets)[0]
        g2 = list(runner.user_greenlets)[1]
        runner.stop_users({BaseUser.__name__: 2})
        self.assertEqual(0, len(runner.user_greenlets))
        self.assertTrue(g1.dead)
        self.assertTrue(g2.dead)
        self.assertTrue(triggered[0])

    def test_start_event(self):
        class MyUser(User):
            wait_time = constant(2)
            task_run_count = 0

            @task
            def my_task(self):
                MyUser.task_run_count += 1

        test_start_run = [0]

        environment = Environment(user_classes=[MyUser])

        def on_test_start(*args, **kwargs):
            test_start_run[0] += 1

        environment.events.test_start.add_listener(on_test_start)

        runner = LocalRunner(environment)
        runner.start(user_count=3, spawn_rate=3, wait=False)
        runner.spawning_greenlet.get(timeout=3)

        self.assertEqual(1, test_start_run[0])
        self.assertEqual(3, MyUser.task_run_count)

    def test_stop_event(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[MyUser])

        @environment.events.test_stopping.add_listener
        def on_test_stopping(*_, **__):
            self.runner_stopping = True

        @environment.events.test_stop.add_listener
        def on_test_stop(*_, **__):
            self.runner_stopped = True

        runner = LocalRunner(environment)
        runner.start(user_count=3, spawn_rate=3, wait=False)
        self.assertFalse(self.runner_stopping)
        self.assertFalse(self.runner_stopped)
        runner.stop()
        self.assertTrue(self.runner_stopping)
        self.assertTrue(self.runner_stopped)

    def test_stop_event_quit(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[MyUser])

        @environment.events.test_stopping.add_listener
        def on_test_stopping(*_, **__):
            self.runner_stopping = True

        @environment.events.test_stop.add_listener
        def on_test_stop(*_, **__):
            self.runner_stopped = True

        runner = LocalRunner(environment)
        runner.start(user_count=3, spawn_rate=3, wait=False)
        self.assertFalse(self.runner_stopping)
        self.assertFalse(self.runner_stopped)
        runner.quit()
        self.assertTrue(self.runner_stopping)
        self.assertTrue(self.runner_stopped)

    def test_stop_event_stop_and_quit(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[MyUser])

        @environment.events.test_stopping.add_listener
        def on_test_stopping(*_, **__):
            self.runner_stopping = True

        @environment.events.test_stop.add_listener
        def on_test_stop(*_, **__):
            self.runner_stopped = True

        runner = LocalRunner(environment)
        runner.start(user_count=3, spawn_rate=3, wait=False)
        self.assertFalse(self.runner_stopping)
        self.assertFalse(self.runner_stopped)
        runner.stop()
        runner.quit()
        self.assertTrue(self.runner_stopping)
        self.assertTrue(self.runner_stopped)

    def test_stopping_event(self):
        on_stop_called = [False]

        class MyUser(User):
            on_stop_called = False
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

            def on_stop(self):
                MyUser.on_stop_called = True

        environment = Environment(user_classes=[MyUser])

        @environment.events.test_stopping.add_listener
        def on_test_stopping(*_, **__):
            on_stop_called[0] = MyUser.on_stop_called
            self.runner_stopping = True

        runner = LocalRunner(environment)
        runner.start(user_count=3, spawn_rate=3, wait=False)
        runner.quit()
        self.assertTrue(self.runner_stopping)
        self.assertFalse(on_stop_called[0])

    def test_change_user_count_during_spawning(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[MyUser])
        runner = LocalRunner(environment)
        runner.start(user_count=10, spawn_rate=5, wait=False)
        sleep(0.6)
        runner.start(user_count=5, spawn_rate=5, wait=False)
        runner.spawning_greenlet.join()
        self.assertEqual(5, len(runner.user_greenlets))
        runner.quit()

    def test_reset_stats(self):
        class MyUser(User):
            @task
            class task_set(TaskSet):
                @task
                def my_task(self):
                    self.user.environment.events.request.fire(
                        request_type="GET",
                        name="/test",
                        response_time=666,
                        response_length=1337,
                        exception=None,
                        context={},
                    )
                    # Make sure each user only run this task once during the test
                    sleep(30)

        environment = Environment(user_classes=[MyUser], reset_stats=True)
        runner = LocalRunner(environment)
        runner.start(user_count=6, spawn_rate=1, wait=False)
        sleep(3)
        self.assertGreaterEqual(runner.stats.get("/test", "GET").num_requests, 3)
        sleep(3.25)
        self.assertLessEqual(runner.stats.get("/test", "GET").num_requests, 1)
        runner.quit()

    def test_no_reset_stats(self):
        class MyUser(User):
            @task
            class task_set(TaskSet):
                @task
                def my_task(self):
                    self.user.environment.events.request.fire(
                        request_type="GET",
                        name="/test",
                        response_time=666,
                        response_length=1337,
                        exception=None,
                        context={},
                    )
                    sleep(2)

        environment = Environment(reset_stats=False, user_classes=[MyUser])
        runner = LocalRunner(environment)
        runner.start(user_count=6, spawn_rate=12, wait=False)
        sleep(0.25)
        self.assertGreaterEqual(runner.stats.get("/test", "GET").num_requests, 3)
        sleep(0.3)
        self.assertEqual(6, runner.stats.get("/test", "GET").num_requests)
        runner.quit()

    def test_runner_reference_on_environment(self):
        env = Environment()
        runner = env.create_local_runner()
        self.assertEqual(env, runner.environment)
        self.assertEqual(runner, env.runner)

    def test_users_can_call_runner_quit_without_deadlocking(self):
        class BaseUser(User):
            stop_triggered = False

            @task
            def trigger(self):
                self.environment.runner.quit()

            def on_stop(self):
                BaseUser.stop_triggered = True

        runner = Environment(user_classes=[BaseUser]).create_local_runner()
        users = runner.spawn_users({BaseUser.__name__: 1}, wait=False)
        self.assertEqual(1, len(users))
        timeout = gevent.Timeout(0.5)
        timeout.start()
        try:
            runner.greenlet.join()
        except gevent.Timeout:
            self.fail("Got Timeout exception, runner must have hung somehow.")
        finally:
            timeout.cancel()

        self.assertTrue(BaseUser.stop_triggered)

    def test_runner_quit_can_run_on_stop_for_multiple_users_concurrently(self):
        class BaseUser(User):
            stop_count = 0

            @task
            def trigger(self):
                pass

            def on_stop(self):
                gevent.sleep(0.1)
                BaseUser.stop_count += 1

        runner = Environment(user_classes=[BaseUser]).create_local_runner()
        users = runner.spawn_users({BaseUser.__name__: 10}, wait=False)
        self.assertEqual(10, len(users))
        timeout = gevent.Timeout(0.3)
        timeout.start()
        try:
            runner.quit()
        except gevent.Timeout:
            self.fail("Got Timeout exception, runner must have hung somehow.")
        finally:
            timeout.cancel()

        self.assertEqual(10, BaseUser.stop_count)  # verify that all users executed on_stop

    def test_stop_users_with_spawn_rate(self):
        """
        The spawn rate does not have an effect on the rate at which the users are stopped.
        It is expected that the excess users will be stopped as soon as possible in parallel
        (while respecting the stop_timeout).
        """

        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[MyUser])
        runner = LocalRunner(environment)

        # Start load test, wait for users to start, then trigger ramp down
        ts = time.time()
        runner.start(10, 10, wait=False)
        runner.spawning_greenlet.join()
        delta = time.time() - ts
        self.assertTrue(
            0 <= delta <= 0.05, f"Expected user count to increase to 10 instantaneously, instead it took {delta:f}"
        )
        self.assertTrue(
            runner.user_count == 10, "User count has not decreased correctly to 2, it is : %i" % runner.user_count
        )

        ts = time.time()
        runner.start(2, 4, wait=False)
        runner.spawning_greenlet.join()
        delta = time.time() - ts
        self.assertTrue(0 <= delta <= 1.05, f"Expected user count to decrease to 2 in 1s, instead it took {delta:f}")
        self.assertTrue(
            runner.user_count == 2, "User count has not decreased correctly to 2, it is : %i" % runner.user_count
        )

    def test_attributes_populated_when_calling_start(self):
        class MyUser1(User):
            wait_time = constant(0)

            @task
            def my_task(self):
                pass

        class MyUser2(User):
            wait_time = constant(0)

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[MyUser1, MyUser2])
        runner = LocalRunner(environment)

        runner.start(user_count=10, spawn_rate=5, wait=False)
        runner.spawning_greenlet.join()
        self.assertDictEqual({"MyUser1": 5, "MyUser2": 5}, runner.user_classes_count)

        runner.start(user_count=5, spawn_rate=5, wait=False)
        runner.spawning_greenlet.join()
        self.assertDictEqual({"MyUser1": 3, "MyUser2": 2}, runner.user_classes_count)

        runner.quit()

    def test_user_classes_count(self):
        class MyUser1(User):
            wait_time = constant(0)

            @task
            def my_task(self):
                pass

        class MyUser2(User):
            wait_time = constant(0)

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[MyUser1, MyUser2])
        runner = LocalRunner(environment)

        runner.start(user_count=10, spawn_rate=5, wait=False)
        runner.spawning_greenlet.join()
        self.assertDictEqual({"MyUser1": 5, "MyUser2": 5}, runner.user_classes_count)

        runner.start(user_count=5, spawn_rate=5, wait=False)
        runner.spawning_greenlet.join()
        self.assertDictEqual({"MyUser1": 3, "MyUser2": 2}, runner.user_classes_count)

        runner.quit()

    def test_host_class_attribute_from_web(self):
        """If host is left empty from the webUI, we should not use it"""

        class MyUser1(User):
            host = "https://host1.com"

            @task
            def my_task(self):
                pass

        class MyUser2(User):
            host = "https://host2.com"

            @task
            def my_task(self):
                pass

        opts = mocked_options()
        # If left empty on the web, we get an empty string as host
        opts.host = ""
        environment = create_environment([MyUser1, MyUser2], opts)
        runner = LocalRunner(environment)
        # Start the runner to trigger problematic code
        runner.start(user_count=2, spawn_rate=1, wait=False)
        runner.spawning_greenlet.join()

        # Make sure we did not overwrite the host variable
        self.assertEqual(MyUser1.host, "https://host1.com")
        self.assertEqual(MyUser2.host, "https://host2.com")

        runner.quit()

    def test_custom_message(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        test_custom_msg = [False]
        test_custom_msg_data = [{}]

        def on_custom_msg(msg, **kw):
            test_custom_msg[0] = True
            test_custom_msg_data[0] = msg.data

        environment = Environment(user_classes=[MyUser])
        runner = LocalRunner(environment)

        runner.register_message("test_custom_msg", on_custom_msg)
        runner.send_message("test_custom_msg", {"test_data": 123})

        self.assertTrue(test_custom_msg[0])
        self.assertEqual(123, test_custom_msg_data[0]["test_data"])

    def test_concurrent_custom_message(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        test_custom_msg = [False]
        test_custom_msg_data = [{}]

        def on_custom_msg(msg, **kw):
            test_custom_msg[0] = True
            test_custom_msg_data[0] = msg.data

        environment = Environment(user_classes=[MyUser])
        runner = LocalRunner(environment)

        runner.register_message("test_custom_msg", on_custom_msg, concurrent=True)
        runner.send_message("test_custom_msg", {"test_data": 123})

        gevent.sleep(0.5)
        self.assertTrue(test_custom_msg[0])
        self.assertEqual(123, test_custom_msg_data[0]["test_data"])

    def test_undefined_custom_message(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        test_custom_msg = [False]

        def on_custom_msg(msg, **kw):
            test_custom_msg[0] = True

        environment = Environment(user_classes=[MyUser])
        runner = LocalRunner(environment)

        runner.register_message("test_custom_msg", on_custom_msg)
        runner.send_message("test_different_custom_msg")

        self.assertFalse(test_custom_msg[0])
        self.assertEqual(1, len(self.mocked_log.warning))
        msg = self.mocked_log.warning[0]
        self.assertIn("Unknown message type received", msg)

    def test_duplicate_message_handler_registration(self):
        class MyUser(User):
            @task
            def my_task(self):
                pass

        def on_custom_msg(msg, **kw):
            pass

        environment = Environment(user_classes=[MyUser])
        runner = LocalRunner(environment)
        runner.register_message("test_custom_msg", on_custom_msg)
        self.assertRaises(Exception, runner.register_message, "test_custom_msg", on_custom_msg)

    def test_swarm_endpoint_is_non_blocking(self):
        class TestUser1(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        class TestUser2(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        env = Environment(user_classes=[TestUser1, TestUser2])
        local_runner = env.create_local_runner()
        web_ui = env.create_web_ui("127.0.0.1", 0)

        gevent.sleep(0.1)

        ts = time.perf_counter()
        response = requests.post(
            f"http://127.0.0.1:{web_ui.server.server_port}/swarm",
            data={"user_count": 20, "spawn_rate": 5, "host": "https://localhost"},
        )
        self.assertEqual(200, response.status_code)
        self.assertTrue(0 <= time.perf_counter() - ts <= 1, "swarm endpoint is blocking")

        ts = time.perf_counter()
        while local_runner.state != STATE_RUNNING:
            self.assertTrue(time.perf_counter() - ts <= 4, local_runner.state)
            gevent.sleep(0.1)

        self.assertTrue(3 <= time.perf_counter() - ts <= 5)

        self.assertEqual(local_runner.user_count, 20)

        local_runner.stop()
        web_ui.stop()

    def test_can_call_stop_endpoint_if_currently_swarming(self):
        class TestUser1(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        class TestUser2(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        stop_timeout = 5
        env = Environment(user_classes=[TestUser1, TestUser2], stop_timeout=stop_timeout)
        local_runner = env.create_local_runner()
        web_ui = env.create_web_ui("127.0.0.1", 0)

        gevent.sleep(0.1)

        ts = time.perf_counter()
        response = requests.post(
            f"http://127.0.0.1:{web_ui.server.server_port}/swarm",
            data={"user_count": 20, "spawn_rate": 1, "host": "https://localhost"},
        )
        self.assertEqual(200, response.status_code)
        self.assertTrue(0 <= time.perf_counter() - ts <= 1, "swarm endpoint is blocking")

        gevent.sleep(5)

        self.assertEqual(local_runner.state, STATE_SPAWNING)
        self.assertLessEqual(local_runner.user_count, 10)

        ts = time.perf_counter()
        response = requests.get(
            f"http://127.0.0.1:{web_ui.server.server_port}/stop",
        )
        self.assertEqual(200, response.status_code)
        self.assertTrue(stop_timeout <= time.perf_counter() - ts <= stop_timeout + 5, "stop endpoint took too long")

        ts = time.perf_counter()
        while local_runner.state != STATE_STOPPED:
            self.assertTrue(time.perf_counter() - ts <= 2)
            gevent.sleep(0.1)

        self.assertLessEqual(local_runner.user_count, 0)

        local_runner.stop()
        web_ui.stop()

    def test_target_user_count_is_set_before_ramp_up(self):
        """Test for https://github.com/locustio/locust/issues/1883"""

        class MyUser1(User):
            wait_time = constant(0)

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[MyUser1])
        runner = LocalRunner(environment)

        test_start_event_fired = [False]

        @environment.events.test_start.add_listener
        def on_test_start(*args, **kwargs):
            test_start_event_fired[0] = True
            self.assertEqual(runner.target_user_count, 3)

        runner.start(user_count=3, spawn_rate=1, wait=False)

        gevent.sleep(1)

        self.assertEqual(runner.target_user_count, 3)
        self.assertEqual(runner.user_count, 1)
        # However, target_user_classes_count is only updated at the end of the ramp-up/ramp-down
        # due to the way it is implemented.
        self.assertDictEqual({}, runner.target_user_classes_count)

        runner.spawning_greenlet.join()

        self.assertEqual(runner.target_user_count, 3)
        self.assertEqual(runner.user_count, 3)
        self.assertDictEqual({"MyUser1": 3}, runner.target_user_classes_count)

        runner.quit()

        self.assertTrue(test_start_event_fired[0])

    def test_stop_users_count(self):
        user_count = 10

        class BaseUser1(User):
            wait_time = constant(1)

            @task
            def task_a(self):
                pass

        class BaseUser2(BaseUser1):
            wait_time = constant(1)

        runner = Environment(user_classes=[BaseUser1, BaseUser2]).create_local_runner()
        runner.start(user_count=user_count, spawn_rate=10)
        sleep(1)
        self.assertEqual(user_count, runner.user_count)

        runner.stop()
        sleep(1)
        self.assertEqual(0, runner.user_count)

    def test_user_count_starts_from_specified_amount_when_creating_new_test_after_previous_step_has_been_stopped(self):
        """Test for https://github.com/locustio/locust/issues/2135"""

        class MyUser1(User):
            wait_time = constant(0)

            @task
            def my_task(self):
                pass

        env = Environment(user_classes=[MyUser1])
        local_runner = env.create_local_runner()
        web_ui = env.create_web_ui("127.0.0.1", 0)

        gevent.sleep(0.1)

        response = requests.post(
            f"http://127.0.0.1:{web_ui.server.server_port}/swarm",
            data={"user_count": 20, "spawn_rate": 20, "host": "https://localhost"},
        )
        self.assertEqual(200, response.status_code)

        t0 = time.perf_counter()
        while local_runner.user_count != 20:
            self.assertTrue(time.perf_counter() - t0 <= 1, local_runner.user_count)
            gevent.sleep(0.1)

        response = requests.get(
            f"http://127.0.0.1:{web_ui.server.server_port}/stop",
        )
        self.assertEqual(200, response.status_code)

        t0 = time.perf_counter()
        while local_runner.state != STATE_STOPPED:
            self.assertTrue(time.perf_counter() - t0 <= 1, local_runner.state)
            gevent.sleep(0.1)

        t0 = time.perf_counter()
        response = requests.post(
            f"http://127.0.0.1:{web_ui.server.server_port}/swarm",
            data={"user_count": 1, "spawn_rate": 1, "host": "https://localhost"},
        )
        self.assertEqual(200, response.status_code)
        self.assertTrue(time.perf_counter() - t0 <= 1, "Stop endpoint should not be blocking")

        # Make sure user count stays at 1 over 2s (2s is arbitrary, but we
        # wait long enough to make sure it stays constant)
        t0 = time.perf_counter()
        while time.perf_counter() - t0 <= 2:
            self.assertTrue(local_runner.user_count <= 1, local_runner.user_count)
            gevent.sleep(0.1)

        local_runner.stop()
        web_ui.stop()

    def test_custom_dispatcher_class(self):
        @mock.create_autospec
        class MyUsersDispatcher(UsersDispatcher):
            def __init__(self, worker_nodes: list[WorkerNode], user_classes: list[type[User]]) -> None:
                super().__init__(worker_nodes, user_classes)

            def add_worker(self, worker_node: WorkerNode) -> None:
                pass

            def remove_worker(self, worker_node: WorkerNode) -> None:
                pass

            @property
            def dispatch_in_progress(self) -> bool:
                return True

            def new_dispatch(self, target_user_count: int, spawn_rate: float, user_classes: list | None = None) -> None:
                return super().new_dispatch(target_user_count, spawn_rate, user_classes)

        class MyUser1(User):
            wait_time = constant(0)
            fixed_count = 1

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[MyUser1], dispatcher_class=MyUsersDispatcher)
        runner = LocalRunner(environment)

        runner.start(user_count=1, spawn_rate=1)
        sleep(1)
        runner._users_dispatcher.new_dispatch.assert_called_with(1, 1, None)
        runner._users_dispatcher.__iter__.assert_called_once()
        runner.stop()


class TestMasterWorkerRunners(LocustTestCase):
    def test_distributed_integration_run(self):
        """
        Full integration test that starts both a MasterRunner and three WorkerRunner instances
        and makes sure that their stats is sent to the Master. Also validates worker_connect event
        """

        class TestUser(User):
            wait_time = constant(0.1)

            @task
            def incr_stats(self):
                self.environment.events.request.fire(
                    request_type="GET",
                    name="/",
                    response_time=1337,
                    response_length=666,
                    exception=None,
                    context={},
                )

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            # start a Master runner
            master_env = Environment(user_classes=[TestUser])
            worker_connect_events = []

            def on_connect(client_id):
                worker_connect_events.append(client_id)

            master_env.events.worker_connect.add_listener(on_connect)
            master = master_env.create_master_runner("*", 0)
            sleep(0)
            # start 3 Worker runners
            workers = []
            for i in range(3):
                worker_env = Environment(user_classes=[TestUser])
                worker: WorkerRunner = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # give workers time to connect
            sleep(0.1)
            # issue start command that should trigger TestUsers to be spawned in the Workers
            master.start(6, spawn_rate=1000)
            sleep(0.1)
            # check that worker nodes have started locusts
            for worker in workers:
                self.assertEqual(2, worker.user_count)
            # give time for users to generate stats, and stats to be sent to master
            sleep(1)
            master.quit()

            for worker in workers:
                # make sure users are killed
                self.assertEqual(0, worker.user_count)
                # make sure events happened correctly
                self.assertIn(worker.client_id, worker_connect_events)

        # check that stats are present in master
        self.assertGreater(
            master_env.runner.stats.total.num_requests,
            20,
            "For some reason the master node's stats has not come in",
        )

    def test_distributed_rapid_successive_starts_converge_to_latest_generation(self):
        """
        TR-4.1: with start(10)->start(25)->start(7) issued in quick succession while the
        first generation is unconfirmed, only the latest generation confirms cluster-wide;
        the superseded generation is recorded and all workers end at the latest target.
        """

        class TestUser(User):
            wait_time = constant(1000)

            @task
            def incr_stats(self):
                pass

        with (
            mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3),
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "5"),
        ):
            master_env = Environment(user_classes=[TestUser])
            master = master_env.create_master_runner("*", 0)
            sleep(0)
            workers = []
            for i in range(3):
                worker_env = Environment(user_classes=[TestUser])
                workers.append(worker_env.create_worker_runner("127.0.0.1", master.server.port))
            sleep(0.2)

            terminal_results = []
            original_callback = master._on_command_result_terminal

            def collect_terminal(result):
                terminal_results.append((result.generation, result.intent, result.status))
                original_callback(result)

            master._command_ledger._on_terminal = collect_terminal

            # Hold generation 1 unconfirmed so the second start supersedes it
            original_observe = master._command_ledger.observe_ack

            def observe_without_generation_one(worker_id, generation, payload=None):
                if generation == 1:
                    return None
                return original_observe(worker_id, generation, payload)

            master._command_ledger.observe_ack = observe_without_generation_one

            g1 = gevent.spawn(master.start, 6, spawn_rate=1000)
            sleep(0.4)
            self.assertFalse(g1.ready())
            g2 = gevent.spawn(master.start, 15, spawn_rate=1000)
            sleep(0.4)
            g3 = gevent.spawn(master.start, 9, spawn_rate=1000)
            gevent.joinall([g1, g2, g3], timeout=10)
            self.assertTrue(g1.ready() and g2.ready() and g3.ready(), "all start() calls returned")

            self.assertIn((1, "spawn", "superseded"), terminal_results)
            final_result = master.last_command_result
            self.assertIsNotNone(final_result)
            assert final_result is not None
            self.assertEqual(3, final_result.generation)
            self.assertTrue(final_result.confirmed)
            self.assertEqual(9, final_result.target[0])

            sleep(0.2)
            self.assertEqual(9, master.target_user_count)
            self.assertEqual(9, sum(worker.user_count for worker in workers))
            for worker in workers:
                self.assertEqual(3, worker.user_count)

            master.quit()
            sleep(0)
            for worker in workers:
                self.assertEqual(0, worker.user_count)

    def test_concurrent_starts_coalesce_same_target_and_order_new_targets(self):
        """
        TR-5.1: concurrent start() calls with the same target coalesce onto one
        generation/dispatch; concurrent different targets produce strictly ordered
        generations, test_start fires once per running generation, and the cluster
        converges to the last target.
        """

        class TestUser(User):
            wait_time = constant(1000)

            @task
            def incr_stats(self):
                pass

        with (
            mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3),
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "5"),
        ):
            master_env = Environment(user_classes=[TestUser])
            master = master_env.create_master_runner("*", 0)
            sleep(0)
            workers = []
            for i in range(3):
                worker_env = Environment(user_classes=[TestUser])
                workers.append(worker_env.create_worker_runner("127.0.0.1", master.server.port))
            sleep(0.2)

            test_start_count = [0]

            @master_env.events.test_start.add_listener
            def on_master_test_start(*_, **__):
                test_start_count[0] += 1

            # Same target, three concurrent callers: exactly one generation/dispatch
            same_greenlets = [gevent.spawn(master.start, 6, spawn_rate=1000) for _ in range(3)]
            gevent.joinall(same_greenlets, timeout=10)
            self.assertTrue(all(g.ready() and g.value is not None for g in same_greenlets))
            same_results = [g.value for g in same_greenlets]
            self.assertIs(same_results[0], same_results[1])
            self.assertIs(same_results[1], same_results[2])
            same_generation = same_results[0].generation
            sleep(0.2)
            self.assertEqual(6, sum(worker.user_count for worker in workers))
            self.assertEqual(1, test_start_count[0])

            # Different targets dispatched concurrently: ordered generations, last wins
            diff_greenlets = [
                gevent.spawn(master.start, target, spawn_rate=1000) for target in (3, 9, 12)
            ]
            gevent.joinall(diff_greenlets, timeout=15)
            self.assertTrue(all(g.ready() and g.value is not None for g in diff_greenlets))
            generations = [g.value.generation for g in diff_greenlets]
            self.assertEqual(generations, sorted(generations))
            self.assertGreater(generations[-1], same_generation)
            final_result = master.last_command_result
            self.assertIsNotNone(final_result)
            assert final_result is not None
            self.assertTrue(final_result.confirmed)
            self.assertEqual(12, final_result.target[0])

            sleep(0.2)
            self.assertEqual(12, master.target_user_count)
            self.assertEqual(12, sum(worker.user_count for worker in workers))
            # No stop happened in between: one running generation, one test_start
            self.assertEqual(1, test_start_count[0])

            master.quit()

    def test_rapid_start_stop_start_lifecycle_fires_once_per_generation(self):
        """
        TR-5.2: start -> stop -> start fires test_start / test_stopping / test_stop
        exactly twice on master and on every worker - never duplicated, never missing.
        """

        class TestUser(User):
            wait_time = constant(1000)

            @task
            def incr_stats(self):
                pass

        with patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "5"):
            master_env = Environment(user_classes=[TestUser])
            master = master_env.create_master_runner("*", 0)
            sleep(0)

            master_counts = {"test_start": 0, "test_stopping": 0, "test_stop": 0}
            master_env.events.test_start.add_listener(lambda **_: master_counts.__setitem__("test_start", master_counts["test_start"] + 1))
            master_env.events.test_stopping.add_listener(
                lambda **_: master_counts.__setitem__("test_stopping", master_counts["test_stopping"] + 1)
            )
            master_env.events.test_stop.add_listener(lambda **_: master_counts.__setitem__("test_stop", master_counts["test_stop"] + 1))

            worker_envs = []
            for i in range(3):
                worker_env = Environment(user_classes=[TestUser])
                worker_envs.append(worker_env)
                worker_env.create_worker_runner("127.0.0.1", master.server.port)
            sleep(0.2)

            worker_counts = [{"test_start": 0, "test_stopping": 0, "test_stop": 0} for _ in worker_envs]
            for env, counts in zip(worker_envs, worker_counts, strict=True):
                env.events.test_start.add_listener(
                    lambda *_, c=counts, **__: c.__setitem__("test_start", c["test_start"] + 1)
                )
                env.events.test_stopping.add_listener(
                    lambda *_, c=counts, **__: c.__setitem__("test_stopping", c["test_stopping"] + 1)
                )
                env.events.test_stop.add_listener(
                    lambda *_, c=counts, **__: c.__setitem__("test_stop", c["test_stop"] + 1)
                )

            first_start = master.start(6, spawn_rate=1000)
            self.assertIsNotNone(first_start)
            assert first_start is not None
            self.assertTrue(first_start.confirmed)
            first_stop = master.stop()
            self.assertIsNotNone(first_stop)
            assert first_stop is not None
            self.assertTrue(first_stop.is_terminal)
            # worker stop handling is asynchronous
            sleep(0.5)
            second_start = master.start(6, spawn_rate=1000)
            self.assertIsNotNone(second_start)
            assert second_start is not None
            self.assertTrue(second_start.confirmed)
            sleep(0.2)
            second_stop = master.stop()
            self.assertIsNotNone(second_stop)
            sleep(0.5)

            self.assertEqual({"test_start": 2, "test_stopping": 2, "test_stop": 2}, master_counts)
            for counts in worker_counts:
                self.assertEqual({"test_start": 2, "test_stopping": 2, "test_stop": 2}, counts)

            master.quit()

    def test_distributed_rebalanced_integration_run(self):
        """
        Full integration test that starts both a MasterRunner and three WorkerRunner instances
        and makes sure that their stats is sent to the Master.
        """

        class TestUser(User):
            wait_time = constant(0.1)

            @task
            def incr_stats(self):
                self.environment.events.request.fire(
                    request_type="GET",
                    name="/",
                    response_time=1337,
                    response_length=666,
                    exception=None,
                    context={},
                )

        with (
            mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3),
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "0.1"),
        ):
            # start a Master runner
            options = get_parser().parse_args(["--enable-rebalancing"])
            master_env = Environment(user_classes=[TestUser], parsed_options=options)
            master = master_env.create_master_runner("*", 0)
            sleep(0)
            # start 3 Worker runners
            workers = []

            def add_worker():
                worker_env = Environment(user_classes=[TestUser])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            for i in range(3):
                add_worker()

            # give workers time to connect
            sleep(0.1)
            # issue start command that should trigger TestUsers to be spawned in the Workers
            master.start(6, spawn_rate=1000)
            sleep(0.1)
            # check that worker nodes have started locusts
            for worker in workers:
                self.assertEqual(2, worker.user_count)
            # give time for users to generate stats, and stats to be sent to master
            # Add 1 more workers (should be 4 now)
            add_worker()

            @retry(AssertionError, tries=10, delay=0.5)
            def check_rebalanced_true():
                for worker in workers:
                    self.assertTrue(worker.user_count > 0)

            # Check that all workers have a user count > 0 at least
            check_rebalanced_true()
            # Add 2 more workers (should be 6 now)
            add_worker()
            add_worker()

            @retry(AssertionError, tries=10, delay=0.5)
            def check_rebalanced_equals():
                for worker in workers:
                    self.assertEqual(1, worker.user_count)

            # Check that all workers have a user count = 1 now
            check_rebalanced_equals()

            # Simulate that some workers are missing by "killing" them abruptly
            for i in range(3):
                workers[i].greenlet.kill(block=True)

            @retry(AssertionError, tries=10, delay=1)
            def check_master_worker_missing_count():
                self.assertEqual(3, len(master.clients.missing))

            # Check that master detected the missing workers
            check_master_worker_missing_count()

            @retry(AssertionError, tries=10, delay=1)
            def check_remaining_worker_new_user_count():
                for i in range(3, 6):
                    self.assertEqual(2, workers[i].user_count)

            # Check that remaining workers have a new count of user due to rebalancing.
            check_remaining_worker_new_user_count()
            sleep(1)

            # Finally quit and check states of remaining workers.
            master.quit()
            # make sure users are killed on remaining workers
            for i in range(3, 6):
                self.assertEqual(0, workers[i].user_count)

        # check that stats are present in master
        self.assertGreater(
            master_env.runner.stats.total.num_requests,
            20,
            "For some reason the master node's stats has not come in",
        )

    def test_distributed_run_with_custom_args(self):
        """
        Full integration test that starts both a MasterRunner and three WorkerRunner instances
        and makes sure that their stats is sent to the Master.
        """

        class TestUser(User):
            wait_time = constant(0.1)

            @task
            def incr_stats(self):
                self.environment.events.request.fire(
                    request_type="GET",
                    name=self.environment.parsed_options.my_str_argument,
                    response_time=self.environment.parsed_options.my_int_argument,
                    response_length=666,
                    exception=None,
                    context={},
                )

        @locust.events.init_command_line_parser.add_listener
        def _(parser, **kw):
            parser.add_argument("--my-int-argument", type=int)
            parser.add_argument("--my-str-argument", type=str, default="NOOOO")

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            # start a Master runner
            master_env = Environment(user_classes=[TestUser])
            master = master_env.create_master_runner("*", 0)
            master_env.parsed_options = get_parser().parse_args(
                [
                    "--my-int-argument",
                    "42",
                    "--my-str-argument",
                    "cool-string",
                ]
            )
            sleep(0)
            # start 3 Worker runners
            workers = []
            for i in range(3):
                worker_env = Environment(user_classes=[TestUser])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # give workers time to connect
            sleep(0.1)
            # issue start command that should trigger TestUsers to be spawned in the Workers
            master.start(6, spawn_rate=1000)
            sleep(0.1)
            # check that worker nodes have started locusts
            for worker in workers:
                self.assertEqual(2, worker.user_count)
            # give time for users to generate stats, and stats to be sent to master
            sleep(1)
            master.quit()
            # make sure users are killed
            for worker in workers:
                self.assertEqual(0, worker.user_count)

        self.assertEqual(master_env.runner.stats.total.max_response_time, 42)
        self.assertEqual(master_env.runner.stats.get("cool-string", "GET").avg_response_time, 42)

    def test_spawning_complete_and_test_stop_event(self):
        class TestUser(User):
            wait_time = constant(0.1)

            @task
            def my_task(l):
                pass

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            # start a Master runner
            master_env = Environment(user_classes=[TestUser])
            test_stop_count = {"master": 0, "worker": 0}
            spawning_complete_count = {"master": 0, "worker": 0}

            @master_env.events.test_stop.add_listener
            def _(*args, **kwargs):
                test_stop_count["master"] += 1

            @master_env.events.spawning_complete.add_listener
            def _(*args, **kwargs):
                spawning_complete_count["master"] += 1

            master = master_env.create_master_runner("*", 0)
            sleep(0)
            # start a Worker runner
            worker_env = Environment(user_classes=[TestUser])

            @worker_env.events.test_stop.add_listener
            def _(*args, **kwargs):
                test_stop_count["worker"] += 1

            @worker_env.events.spawning_complete.add_listener
            def _(*args, **kwargs):
                spawning_complete_count["worker"] += 1

            worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)

            # give worker time to connect
            sleep(0.1)
            # issue start command that should trigger TestUsers to be spawned in the Workers
            master.start(3, spawn_rate=1)
            sleep(3)
            # check that worker nodes have started locusts
            self.assertEqual(3, worker.user_count)
            # give time for users to generate stats, and stats to be sent to master
            sleep(0.1)
            master_env.events.quitting.fire(environment=master_env, reverse=True)
            master.quit()
            sleep(0.1)
            # make sure users are killed
            self.assertEqual(0, worker.user_count)

        # check the spawning_complete and test_stop events were called one time in master and one time in worker
        self.assertEqual(
            1,
            test_stop_count["master"],
            "The test_stop event was not called exactly one time in the master node",
        )
        self.assertEqual(
            1,
            test_stop_count["worker"],
            "The test_stop event was not called exactly one time in the worker node",
        )
        self.assertEqual(
            1,
            spawning_complete_count["master"],
            "The spawning_complete event was not called exactly one time in the master node",
        )
        self.assertEqual(
            1,
            spawning_complete_count["worker"],
            "The spawning_complete event was not called exactly one time in the worker node",
        )

    def test_distributed_shape(self):
        """
        Full integration test that starts both a MasterRunner and three WorkerRunner instances
        and tests a basic LoadTestShape with scaling up and down users
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        class TestShape(LoadTestShape):
            def tick(self):
                run_time = self.get_run_time()
                if run_time < 2:
                    return 9, 9
                elif run_time < 4:
                    return 21, 21
                elif run_time < 6:
                    return 3, 21
                else:
                    return None

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            test_shape = TestShape()
            master_env = Environment(user_classes=[TestUser], shape_class=test_shape)
            master_env.shape_class.reset_time()
            master = master_env.create_master_runner("*", 0)

            workers = []
            for i in range(3):
                worker_env = Environment(user_classes=[TestUser])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # Give workers time to connect
            sleep(0.1)

            # Start a shape test
            master.start_shape()
            sleep(1)

            # Ensure workers have connected and started the correct amount of users
            for worker in workers:
                self.assertEqual(3, worker.user_count, "Shape test has not reached stage 1")
                self.assertEqual(
                    9, test_shape.get_current_user_count(), "Shape is not seeing stage 1 runner user count correctly"
                )
            self.assertDictEqual(master.reported_user_classes_count, {"TestUser": 9})

            # Ensure new stage with more users has been reached
            sleep(2)
            for worker in workers:
                self.assertEqual(7, worker.user_count, "Shape test has not reached stage 2")
                self.assertEqual(
                    21, test_shape.get_current_user_count(), "Shape is not seeing stage 2 runner user count correctly"
                )
            self.assertDictEqual(master.reported_user_classes_count, {"TestUser": 21})

            # Ensure new stage with less users has been reached
            sleep(2)
            for worker in workers:
                self.assertEqual(1, worker.user_count, "Shape test has not reached stage 3")
                self.assertEqual(
                    3, test_shape.get_current_user_count(), "Shape is not seeing stage 3 runner user count correctly"
                )
            self.assertDictEqual(master.reported_user_classes_count, {"TestUser": 3})

            # Ensure test stops at the end
            sleep(2)
            for worker in workers:
                self.assertEqual(0, worker.user_count, "Shape test has not stopped")
                self.assertEqual(
                    0, test_shape.get_current_user_count(), "Shape is not seeing stopped runner user count correctly"
                )
            self.assertDictEqual(master.reported_user_classes_count, {"TestUser": 0})

            self.assertEqual("stopped", master.state)

    @unittest.skip(reason="a little flaky since #2465, so disabled for now")
    def test_distributed_shape_with_fixed_users(self):
        """
        Full integration test that starts both a MasterRunner and three WorkerRunner instances
        and tests a basic LoadTestShape with scaling up and down users with 'fixed count' users
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        class FixedUser1(User):
            fixed_count = 1

            @task
            def my_task(self):
                pass

        class FixedUser2(User):
            fixed_count = 11

            @task
            def my_task(self):
                pass

        class TestShape(LoadTestShape):
            def tick(self):
                run_time = self.get_run_time()
                if run_time < 1:
                    return 12, 12
                elif run_time < 2:
                    return 36, 24
                elif run_time < 3:
                    return 12, 24
                else:
                    return None

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            test_shape = TestShape()
            master_env = Environment(user_classes=[TestUser, FixedUser1, FixedUser2], shape_class=test_shape)
            master_env.shape_class.reset_time()
            master = master_env.create_master_runner("*", 0)

            workers = []
            for _ in range(3):
                worker_env = Environment(user_classes=[TestUser, FixedUser1, FixedUser2])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # Give workers time to connect
            sleep(0.1)

            # Start a shape test
            master.start_shape()
            sleep(1)

            # Ensure workers have connected and started the correct amount of users (fixed is spawn first)
            for worker in workers:
                self.assertEqual(4, worker.user_count, "Shape test has not reached stage 1")
                self.assertEqual(
                    12, test_shape.get_current_user_count(), "Shape is not seeing stage 1 runner user count correctly"
                )
            self.assertDictEqual(master.reported_user_classes_count, {"FixedUser1": 1, "FixedUser2": 11, "TestUser": 0})

            # Ensure new stage with more users has been reached
            sleep(1)
            for worker in workers:
                self.assertEqual(12, worker.user_count, "Shape test has not reached stage 2")
                self.assertEqual(
                    36, test_shape.get_current_user_count(), "Shape is not seeing stage 2 runner user count correctly"
                )
            self.assertDictEqual(
                master.reported_user_classes_count, {"FixedUser1": 1, "FixedUser2": 11, "TestUser": 24}
            )

            # Ensure new stage with less users has been reached
            # and expected count of the fixed users is present
            sleep(1)
            for worker in workers:
                self.assertEqual(4, worker.user_count, "Shape test has not reached stage 3")
                self.assertEqual(
                    12, test_shape.get_current_user_count(), "Shape is not seeing stage 3 runner user count correctly"
                )
            self.assertDictEqual(master.reported_user_classes_count, {"FixedUser1": 1, "FixedUser2": 11, "TestUser": 0})

            # Ensure test stops at the end
            sleep(0.5)
            for worker in workers:
                self.assertEqual(0, worker.user_count, "Shape test has not stopped")
                self.assertEqual(
                    0, test_shape.get_current_user_count(), "Shape is not seeing stopped runner user count correctly"
                )
            self.assertDictEqual(master.reported_user_classes_count, {"FixedUser1": 0, "FixedUser2": 0, "TestUser": 0})

            try:
                with gevent.Timeout(3.0):
                    while master.state != STATE_STOPPED:
                        sleep(0.1)
            finally:
                self.assertEqual(STATE_STOPPED, master.state)

    @unittest.skip(reason="takes way too long (~45s)")
    def test_distributed_shape_with_stop_timeout(self):
        """
        Full integration test that starts both a MasterRunner and five WorkerRunner instances
        and tests a basic LoadTestShape with scaling up and down users
        """

        class TestUser1(User):
            def start(self, group: Group):
                gevent.sleep(0.5)
                return super().start(group)

            @task
            def my_task(self):
                gevent.sleep(0)

        class TestUser2(User):
            def start(self, group: Group):
                gevent.sleep(0.5)
                return super().start(group)

            @task
            def my_task(self):
                gevent.sleep(600)

        class TestUser3(User):
            def start(self, group: Group):
                gevent.sleep(0.5)
                return super().start(group)

            @task
            def my_task(self):
                gevent.sleep(600)

        class TestShape(LoadTestShape):
            def tick(self):
                run_time = self.get_run_time()
                if run_time < 10:
                    return 15, 3
                elif run_time < 30:
                    return 5, 10
                else:
                    return None

        locust_worker_additional_wait_before_ready_after_stop = 5
        with (
            mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3),
            patch_env(
                "LOCUST_WORKER_ADDITIONAL_WAIT_BEFORE_READY_AFTER_STOP",
                str(locust_worker_additional_wait_before_ready_after_stop),
            ),
        ):
            stop_timeout = 5
            master_env = Environment(
                user_classes=[TestUser1, TestUser2, TestUser3], shape_class=TestShape(), stop_timeout=stop_timeout
            )
            master_env.shape_class.reset_time()
            master = master_env.create_master_runner("*", 0)

            workers = []
            for i in range(5):
                worker_env = Environment(user_classes=[TestUser1, TestUser2, TestUser3])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # Give workers time to connect
            sleep(0.1)

            self.assertEqual(STATE_INIT, master.state)
            self.assertEqual(5, len(master.clients.ready))

            # Re-order `workers` so that it is sorted by `id`.
            # This is required because the dispatch is done
            # on the sorted workers.
            workers = sorted(workers, key=lambda w: w.client_id)

            # Start a shape test
            master.start_shape()

            # First stage
            ts = time.time()
            while master.state != STATE_SPAWNING:
                self.assertTrue(time.time() - ts <= 1, master.state)
                sleep()
            sleep(5 - (time.time() - ts))  # runtime = 5s
            ts = time.time()
            while master.state != STATE_RUNNING:
                self.assertTrue(time.time() - ts <= 1, master.state)
                sleep()
            self.assertEqual(STATE_RUNNING, master.state)
            w1 = {"TestUser1": 1, "TestUser2": 1, "TestUser3": 1}
            w2 = {"TestUser1": 1, "TestUser2": 1, "TestUser3": 1}
            w3 = {"TestUser1": 1, "TestUser2": 1, "TestUser3": 1}
            w4 = {"TestUser1": 1, "TestUser2": 1, "TestUser3": 1}
            w5 = {"TestUser1": 1, "TestUser2": 1, "TestUser3": 1}
            self.assertDictEqual(w1, workers[0].user_classes_count)
            self.assertDictEqual(w2, workers[1].user_classes_count)
            self.assertDictEqual(w3, workers[2].user_classes_count)
            self.assertDictEqual(w4, workers[3].user_classes_count)
            self.assertDictEqual(w5, workers[4].user_classes_count)
            self.assertDictEqual(w1, master.clients[workers[0].client_id].user_classes_count)
            self.assertDictEqual(w2, master.clients[workers[1].client_id].user_classes_count)
            self.assertDictEqual(w3, master.clients[workers[2].client_id].user_classes_count)
            self.assertDictEqual(w4, master.clients[workers[3].client_id].user_classes_count)
            self.assertDictEqual(w5, master.clients[workers[4].client_id].user_classes_count)
            sleep(5 - (time.time() - ts))  # runtime = 10s

            # Fourth stage
            ts = time.time()
            while master.state != STATE_SPAWNING:
                self.assertTrue(time.time() - ts <= 1, master.state)
                sleep()
            sleep(5 - (time.time() - ts))  # runtime = 15s

            # Fourth stage - Excess TestUser1 have been stopped but
            #                TestUser2/TestUser3 have not reached stop timeout yet, so
            #                their number are unchanged
            ts = time.time()
            while master.state != STATE_RUNNING:
                self.assertTrue(time.time() - ts <= 1, master.state)
                sleep()
            delta = time.time() - ts
            w1 = {"TestUser1": 1, "TestUser2": 1, "TestUser3": 1}
            w2 = {"TestUser1": 0, "TestUser2": 1, "TestUser3": 1}
            w3 = {"TestUser1": 0, "TestUser2": 1, "TestUser3": 1}
            w4 = {"TestUser1": 1, "TestUser2": 1, "TestUser3": 1}
            w5 = {"TestUser1": 0, "TestUser2": 1, "TestUser3": 1}
            self.assertDictEqual(w1, workers[0].user_classes_count)
            self.assertDictEqual(w2, workers[1].user_classes_count)
            self.assertDictEqual(w3, workers[2].user_classes_count)
            self.assertDictEqual(w4, workers[3].user_classes_count)
            self.assertDictEqual(w5, workers[4].user_classes_count)
            self.assertDictEqual(w1, master.clients[workers[0].client_id].user_classes_count)
            self.assertDictEqual(w2, master.clients[workers[1].client_id].user_classes_count)
            self.assertDictEqual(w3, master.clients[workers[2].client_id].user_classes_count)
            self.assertDictEqual(w4, master.clients[workers[3].client_id].user_classes_count)
            self.assertDictEqual(w5, master.clients[workers[4].client_id].user_classes_count)
            sleep(1 - delta)  # runtime = 16s

            # Fourth stage - All users are now at the desired number
            ts = time.time()
            while master.state != STATE_RUNNING:
                self.assertTrue(time.time() - ts <= 1, master.state)
                sleep()
            delta = time.time() - ts
            w1 = {"TestUser1": 1, "TestUser2": 0, "TestUser3": 0}
            w2 = {"TestUser1": 0, "TestUser2": 1, "TestUser3": 0}
            w3 = {"TestUser1": 0, "TestUser2": 0, "TestUser3": 1}
            w4 = {"TestUser1": 1, "TestUser2": 0, "TestUser3": 0}
            w5 = {"TestUser1": 0, "TestUser2": 1, "TestUser3": 0}
            self.assertDictEqual(w1, workers[0].user_classes_count)
            self.assertDictEqual(w2, workers[1].user_classes_count)
            self.assertDictEqual(w3, workers[2].user_classes_count)
            self.assertDictEqual(w4, workers[3].user_classes_count)
            self.assertDictEqual(w5, workers[4].user_classes_count)
            self.assertDictEqual(w1, master.clients[workers[0].client_id].user_classes_count)
            self.assertDictEqual(w2, master.clients[workers[1].client_id].user_classes_count)
            self.assertDictEqual(w3, master.clients[workers[2].client_id].user_classes_count)
            self.assertDictEqual(w4, master.clients[workers[3].client_id].user_classes_count)
            self.assertDictEqual(w5, master.clients[workers[4].client_id].user_classes_count)
            sleep(10 - delta)  # runtime = 26s

            # Sleep stop_timeout and make sure the test has stopped
            sleep(5)  # runtime = 31s
            self.assertEqual(STATE_STOPPING, master.state)
            sleep(stop_timeout)  # runtime = 36s

            # We wait for "stop_timeout" seconds to let the workers reconnect as "ready" with the master.
            # The reason for waiting an additional "stop_timeout" when we already waited for "stop_timeout"
            # above is that when a worker receives the stop message, it can take up to "stop_timeout"
            # for the worker to send the "client_stopped" message then an additional "stop_timeout" seconds
            # to send the "client_ready" message.
            ts = time.time()
            while len(master.clients.ready) != len(workers):
                self.assertTrue(
                    time.time() - ts <= stop_timeout + locust_worker_additional_wait_before_ready_after_stop,
                    f"expected {len(workers)} workers to be ready but only {len(master.clients.ready)} workers are",
                )
                sleep()
            sleep(1)

            # Check that no users are running
            w1 = {"TestUser1": 0, "TestUser2": 0, "TestUser3": 0}
            w2 = {"TestUser1": 0, "TestUser2": 0, "TestUser3": 0}
            w3 = {"TestUser1": 0, "TestUser2": 0, "TestUser3": 0}
            w4 = {"TestUser1": 0, "TestUser2": 0, "TestUser3": 0}
            w5 = {"TestUser1": 0, "TestUser2": 0, "TestUser3": 0}
            self.assertDictEqual(w1, workers[0].user_classes_count)
            self.assertDictEqual(w2, workers[1].user_classes_count)
            self.assertDictEqual(w3, workers[2].user_classes_count)
            self.assertDictEqual(w4, workers[3].user_classes_count)
            self.assertDictEqual(w5, workers[4].user_classes_count)
            self.assertDictEqual(w1, master.clients[workers[0].client_id].user_classes_count)
            self.assertDictEqual(w2, master.clients[workers[1].client_id].user_classes_count)
            self.assertDictEqual(w3, master.clients[workers[2].client_id].user_classes_count)
            self.assertDictEqual(w4, master.clients[workers[3].client_id].user_classes_count)
            self.assertDictEqual(w5, master.clients[workers[4].client_id].user_classes_count)

            ts = time.time()
            while master.state != STATE_STOPPED:
                self.assertTrue(time.time() - ts <= 5, master.state)
                sleep()

            master.stop()

    @unittest.skip(reason="takes a lot of time and has randomness to it")
    def test_distributed_shape_fuzzy_test(self):
        """
        Incredibility useful test to find issues with dispatch logic. This test allowed to find
        multiple small corner cases with the new dispatch logic of locust v2.

        The test is disabled by default because it takes a lot of time to run and has randomness to it.
        However, it is advised to run it a few times (you can run it in parallel) when modifying the dispatch logic.
        """

        class BaseUser(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        class TestUser01(BaseUser):
            pass

        class TestUser02(BaseUser):
            pass

        class TestUser03(BaseUser):
            pass

        class TestUser04(BaseUser):
            pass

        class TestUser05(BaseUser):
            pass

        class TestUser06(BaseUser):
            pass

        class TestUser07(BaseUser):
            pass

        class TestUser08(BaseUser):
            pass

        class TestUser09(BaseUser):
            pass

        class TestUser10(BaseUser):
            pass

        class TestUser11(BaseUser):
            pass

        class TestUser12(BaseUser):
            pass

        class TestUser13(BaseUser):
            pass

        class TestUser14(BaseUser):
            pass

        class TestUser15(BaseUser):
            pass

        class TestShape(LoadTestShape):
            def __init__(self):
                super().__init__()

                self.stages = []
                runtime = 0
                for _ in range(100):
                    runtime += random.uniform(3, 15)
                    self.stages.append((runtime, random.randint(1, 100), random.uniform(0.1, 10)))

            def tick(self):
                run_time = self.get_run_time()
                for stage in self.stages:
                    if run_time < stage[0]:
                        return stage[1], stage[2]

        user_classes = [
            TestUser01,
            TestUser02,
            TestUser03,
            TestUser04,
            TestUser05,
            TestUser06,
            TestUser07,
            TestUser08,
            TestUser09,
            TestUser10,
            TestUser11,
            TestUser12,
            TestUser13,
            TestUser14,
            TestUser15,
        ]

        chosen_user_classes = random.sample(user_classes, k=random.randint(1, len(user_classes)))

        for user_class in chosen_user_classes:
            user_class.weight = random.uniform(1, 20)

        locust_worker_additional_wait_before_ready_after_stop = 5
        with (
            mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3),
            patch_env(
                "LOCUST_WORKER_ADDITIONAL_WAIT_BEFORE_READY_AFTER_STOP",
                str(locust_worker_additional_wait_before_ready_after_stop),
            ),
        ):
            stop_timeout = 5
            master_env = Environment(
                user_classes=chosen_user_classes, shape_class=TestShape(), stop_timeout=stop_timeout
            )
            master_env.shape_class.reset_time()
            master = master_env.create_master_runner("*", 0)

            workers = []
            for i in range(random.randint(1, 30)):
                worker_env = Environment(user_classes=chosen_user_classes)
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # Give workers time to connect
            sleep(0.1)

            self.assertEqual(STATE_INIT, master.state)
            self.assertEqual(len(workers), len(master.clients.ready))

            # Start a shape test
            master.start_shape()

            ts = time.time()
            while master.state != STATE_STOPPED:
                self.assertTrue(time.time() - ts <= master_env.shape_class.stages[-1][0] + 60, master.state)
                print(
                    f"{time.time() - ts:.2f}/{master_env.shape_class.stages[-1][0]:.2f} | {master.state} | {sum(master.reported_user_classes_count.values()):.0f} | "
                    + json.dumps(dict(sorted(master.reported_user_classes_count.items(), key=itemgetter(0))))
                )
                sleep(1)

            master.stop()

    def test_distributed_shape_stop_and_restart(self):
        """
        Test stopping and then restarting a LoadTestShape
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        class TestShape(LoadTestShape):
            def tick(self):
                run_time = self.get_run_time()
                if run_time < 10:
                    return 4, 4
                else:
                    return None

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            master_env = Environment(user_classes=[TestUser], shape_class=TestShape())
            master_env.shape_class.reset_time()
            master = master_env.create_master_runner("*", 0)

            workers = []
            for i in range(2):
                worker_env = Environment(user_classes=[TestUser])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # Give workers time to connect
            sleep(0.1)

            # Start a shape test and ensure workers have connected and started the correct amount of users
            master.start_shape()
            sleep(1)
            for worker in workers:
                self.assertEqual(2, worker.user_count, "Shape test has not started correctly")

            # Stop the test and ensure all user count is 0
            master.stop()
            sleep(1)
            for worker in workers:
                self.assertEqual(0, worker.user_count, "Shape test has not stopped")

            # Then restart the test again and ensure workers have connected and started the correct amount of users
            master.start_shape()
            sleep(1)
            for worker in workers:
                self.assertEqual(2, worker.user_count, "Shape test has not started again correctly")
            master.stop()

    def test_distributed_stop_with_stopping_state(self):
        """
        Test stopping state when workers have stopped and now ready for a next test
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            master_env = Environment(user_classes=[TestUser])
            master = master_env.create_master_runner("*", 0)

            workers = []
            for i in range(3):
                worker_env = Environment(user_classes=[TestUser])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

        for worker in workers:
            worker.send_message("client_stopped", None)

        sleep(1)
        for worker in workers:
            self.assertEqual(STATE_INIT, worker.state, "Worker sent a client_stopped, should be ready once stopped")
        self.assertEqual(STATE_STOPPED, master.state)

    def test_distributed_shape_statuses_transition(self):
        """
        Full integration test that starts both a MasterRunner and five WorkerRunner instances
        The goal of this test is to validate the status on the master is correctly transitioned for each of the
        test phases.
        """

        class TestUser1(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        class TestShape(LoadTestShape):
            def tick(self):
                run_time = self.get_run_time()
                if run_time < 5:
                    return 5, 2.5
                elif run_time < 10:
                    return 10, 2.5
                elif run_time < 15:
                    return 15, 2.5
                else:
                    return None

        locust_worker_additional_wait_before_ready_after_stop = 2
        with (
            mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3),
            patch_env(
                "LOCUST_WORKER_ADDITIONAL_WAIT_BEFORE_READY_AFTER_STOP",
                str(locust_worker_additional_wait_before_ready_after_stop),
            ),
        ):
            master_env = Environment(user_classes=[TestUser1], shape_class=TestShape())

            master_env.shape_class.reset_time()
            master = master_env.create_master_runner("*", 0)

            workers = []
            for i in range(5):
                worker_env = Environment(user_classes=[TestUser1])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # Give workers time to connect
            sleep(0.1)

            self.assertEqual(STATE_INIT, master.state)
            self.assertEqual(5, len(master.clients.ready))

            statuses = []

            ts = time.perf_counter()

            master.start_shape()

            while master.state != STATE_STOPPED:
                # +5s buffer to let master stop
                self.assertTrue(
                    time.perf_counter() - ts <= 30 + locust_worker_additional_wait_before_ready_after_stop + 5,
                    master.state,
                )
                statuses.append((time.perf_counter() - ts, master.state, master.user_count))
                sleep(0.1)

            self.assertEqual(statuses[0][1], STATE_INIT)

            stage = 1
            tolerance = 1  # in s
            for (t1, state1, user_count1), (t2, state2, user_count2) in zip(statuses[:-1], statuses[1:]):
                if state1 == STATE_SPAWNING and state2 == STATE_RUNNING and stage == 1:
                    self.assertTrue(2.5 - tolerance <= t2 <= 2.5 + tolerance)
                elif state1 == STATE_RUNNING and state2 == STATE_SPAWNING and stage == 1:
                    self.assertTrue(5 - tolerance <= t2 <= 5 + tolerance)
                    stage += 1
                elif state1 == STATE_SPAWNING and state2 == STATE_RUNNING and stage == 2:
                    self.assertTrue(7.5 - tolerance <= t2 <= 7.5 + tolerance)
                elif state1 == STATE_RUNNING and state2 == STATE_SPAWNING and stage == 2:
                    self.assertTrue(10 - tolerance <= t2 <= 10 + tolerance)
                    stage += 1
                elif state1 == STATE_SPAWNING and state2 == STATE_RUNNING and stage == 3:
                    self.assertTrue(12.5 - tolerance <= t2 <= 12.5 + tolerance)
                elif state1 == STATE_RUNNING and state2 == STATE_SPAWNING and stage == 3:
                    self.assertTrue(15 - tolerance <= t2 <= 15 + tolerance)
                    stage += 1
                elif state1 == STATE_RUNNING and state2 == STATE_STOPPED and stage == 3:
                    self.assertTrue(15 - tolerance <= t2 <= 15 + tolerance)

    def test_swarm_endpoint_is_non_blocking(self):
        class TestUser1(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        class TestUser2(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            master_env = Environment(user_classes=[TestUser1, TestUser2])
            master = master_env.create_master_runner("*", 0)
            web_ui = master_env.create_web_ui("127.0.0.1", 0)

            workers = []
            for i in range(2):
                worker_env = Environment(user_classes=[TestUser1, TestUser2])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # Give workers time to connect
            sleep(0.1)

            self.assertEqual(STATE_INIT, master.state)
            self.assertEqual(len(master.clients.ready), len(workers))

            ts = time.perf_counter()
            response = requests.post(
                f"http://127.0.0.1:{web_ui.server.server_port}/swarm",
                data={"user_count": 20, "spawn_rate": 5, "host": "https://localhost"},
            )
            self.assertEqual(200, response.status_code)
            self.assertTrue(0 <= time.perf_counter() - ts <= 1, "swarm endpoint is blocking")

            ts = time.perf_counter()
            while master.state != STATE_RUNNING:
                self.assertTrue(time.perf_counter() - ts <= 4, master.state)
                gevent.sleep(0.1)

            self.assertTrue(3 <= time.perf_counter() - ts <= 5)

            self.assertEqual(master.user_count, 20)

            master.stop()
            web_ui.stop()

    def test_can_call_stop_endpoint_if_currently_swarming(self):
        class TestUser1(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        class TestUser2(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            stop_timeout = 5
            master_env = Environment(user_classes=[TestUser1, TestUser2], stop_timeout=stop_timeout)
            master = master_env.create_master_runner("*", 0)
            web_ui = master_env.create_web_ui("127.0.0.1", 0)

            workers = []
            for i in range(2):
                worker_env = Environment(user_classes=[TestUser1, TestUser2])
                worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)
                workers.append(worker)

            # Give workers time to connect
            sleep(0.1)

            self.assertEqual(STATE_INIT, master.state)
            self.assertEqual(len(master.clients.ready), len(workers))

            ts = time.perf_counter()
            response = requests.post(
                f"http://127.0.0.1:{web_ui.server.server_port}/swarm",
                data={"user_count": 20, "spawn_rate": 1, "host": "https://localhost"},
            )
            self.assertEqual(200, response.status_code)
            self.assertTrue(0 <= time.perf_counter() - ts <= 1, "swarm endpoint is blocking")

            gevent.sleep(5)

            self.assertEqual(master.state, STATE_SPAWNING)
            self.assertLessEqual(master.user_count, 10)

            ts = time.perf_counter()
            response = requests.get(
                f"http://127.0.0.1:{web_ui.server.server_port}/stop",
            )
            self.assertEqual(200, response.status_code)
            self.assertTrue(stop_timeout <= time.perf_counter() - ts <= stop_timeout + 5, "stop endpoint took too long")

            ts = time.perf_counter()
            while master.state != STATE_STOPPED:
                self.assertTrue(time.perf_counter() - ts <= 2)
                gevent.sleep(0.1)

            self.assertLessEqual(master.user_count, 0)

            master.stop()
            web_ui.stop()

    def test_target_user_count_is_set_before_ramp_up(self):
        """Test for https://github.com/locustio/locust/issues/1883"""

        class MyUser1(User):
            wait_time = constant(0)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            # start a Master runner
            master_env = Environment(user_classes=[MyUser1])
            master = master_env.create_master_runner("*", 0)

            test_start_event_fired = [False]

            @master_env.events.test_start.add_listener
            def on_test_start(*args, **kwargs):
                test_start_event_fired[0] = True
                self.assertEqual(master.target_user_count, 3)

            sleep(0)

            # start 1 worker runner
            worker_env = Environment(user_classes=[MyUser1])
            worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)

            # give worker time to connect
            sleep(0.1)

            gevent.spawn(master.start, 3, spawn_rate=1)

            sleep(1)

            self.assertEqual(master.target_user_count, 3)
            self.assertEqual(master.user_count, 1)
            # However, target_user_classes_count is only updated at the end of the ramp-up/ramp-down
            # due to the way it is implemented.
            self.assertDictEqual({}, master.target_user_classes_count)

            sleep(2)

            self.assertEqual(master.target_user_count, 3)
            self.assertEqual(master.user_count, 3)
            self.assertDictEqual({"MyUser1": 3}, master.target_user_classes_count)

            master.quit()

            # make sure users are killed
            self.assertEqual(0, worker.user_count)

            self.assertTrue(test_start_event_fired[0])

    def test_long_running_test_start_is_run_to_completion_on_worker(self):
        """Test for https://github.com/locustio/locust/issues/1986"""

        class MyUser1(User):
            wait_time = constant(0)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=0.3):
            master_env = Environment(user_classes=[MyUser1])
            master = master_env.create_master_runner("*", 0)

            sleep(0)

            # start 1 worker runner
            worker_env = Environment(user_classes=[MyUser1])
            worker = worker_env.create_worker_runner("127.0.0.1", master.server.port)

            test_start_exec_count = 0

            @worker_env.events.test_start.add_listener
            def on_test_start(*_, **__):
                nonlocal test_start_exec_count
                test_start_exec_count += 1
                sleep(3)

            # give worker time to connect
            sleep(0.1)

            gevent.spawn(master.start, 3, spawn_rate=1)

            t0 = time.perf_counter()
            while master.user_count != 3:
                self.assertLessEqual(time.perf_counter() - t0, 5, "Expected 3 users to be spawned")
                sleep(0.1)

            master.quit()

            # make sure users are killed
            self.assertEqual(0, worker.user_count)

            self.assertEqual(test_start_exec_count, 1)

    def test_heartbeat_event(self) -> None:
        """
        Tests that heartbeat event is fired during a test
        """

        class TestUser(User):
            wait_time = constant(0.1)

            @task
            def noop(self) -> None:
                pass

        with mock.patch("locust.runners.HEARTBEAT_INTERVAL", new=1):
            # start a Master runner
            master_env = Environment(user_classes=[TestUser])
            worker_connect_events = []
            timestamp_start: list[float] = [time.time() + 3600.0]

            def on_connect(client_id: str) -> None:
                worker_connect_events.append(client_id)
                timestamp_start[0] = time.time()

            master_env.events.worker_connect.add_listener(on_connect)
            master = master_env.create_master_runner("*", 0)
            sleep(0)
            worker_env = Environment(user_classes=[TestUser])
            worker: WorkerRunner = worker_env.create_worker_runner("127.0.0.1", master.server.port)

            with (
                mock.patch.object(
                    worker.environment.events.heartbeat_received,
                    "fire",
                    wraps=worker.environment.events.heartbeat_received.fire,
                ) as worker_heartbeat_received_mock,
                mock.patch.object(
                    master.environment.events.heartbeat_sent,
                    "fire",
                    wraps=master.environment.events.heartbeat_sent.fire,
                ) as master_heartbeat_sent_mock,
            ):
                # give workers time to connect
                sleep(0.1)
                # issue start command that should trigger TestUsers to be spawned in the Workers
                master.start(2, spawn_rate=2)
                sleep(0.1)
                # check that worker nodes have started locusts
                self.assertEqual(2, worker.user_count)

                # give time for nodes to send and receive 5 heartbeats, HEARTBEAT_INTERVAL mocked to 1 second, so
                # sleep 5 seconds - 1 second that represents the overhead from connecting
                sleep(5 - 1)
                master.quit()

                # make sure users are killed
                self.assertEqual(0, worker.user_count)
                # make sure events happened correctly
                self.assertIn(worker.client_id, worker_connect_events)

                timestamp_stop = time.time()

                self.assertEqual(worker_heartbeat_received_mock.call_count, 5)
                self.assertEqual(master_heartbeat_sent_mock.call_count, 5)

                for call_args, call_kwargs in [
                    *worker_heartbeat_received_mock.call_args_list,
                    *master_heartbeat_sent_mock.call_args_list,
                ]:
                    self.assertEqual(call_args, ())  # args
                    self.assertEqual(call_kwargs, {"client_id": worker.client_id, "timestamp": mock.ANY})  # kwargs
                    self.assertGreaterEqual(call_kwargs["timestamp"], timestamp_start[0])
                    self.assertLessEqual(call_kwargs["timestamp"], timestamp_stop)

    def test_usage_monitor_event(self) -> None:
        """
        Tests that usage_monitor event is fired during a test
        """

        class TestUser(User):
            wait_time = constant(0.1)

            @task
            def noop(self) -> None:
                pass

        with mock.patch("locust.runners.CPU_MONITOR_INTERVAL", new=1):
            # start a Master runner
            master_env = Environment(user_classes=[TestUser])
            worker_connect_events = []

            def on_connect(client_id: str) -> None:
                worker_connect_events.append(client_id)

            master_env.events.worker_connect.add_listener(on_connect)
            master = master_env.create_master_runner("*", 0)
            sleep(0)
            worker_env = Environment(user_classes=[TestUser])
            worker: WorkerRunner = worker_env.create_worker_runner("127.0.0.1", master.server.port)

            with (
                mock.patch.object(
                    worker.environment.events.usage_monitor, "fire", wraps=worker.environment.events.usage_monitor.fire
                ) as worker_usage_monitor_mock,
                mock.patch.object(
                    master.environment.events.usage_monitor, "fire", wraps=master.environment.events.usage_monitor.fire
                ) as master_usage_monitor_mock,
            ):
                # give workers time to connect
                sleep(0.1)
                # issue start command that should trigger TestUsers to be spawned in the Workers
                master.start(2, spawn_rate=2)
                sleep(0.1)
                # check that worker nodes have started locusts
                self.assertEqual(2, worker.user_count)

                # give time for nodes to send 5 usage_monitor events, CPU_MONITOR_INTERVAL mocked to 1 second, so
                # sleep 5 seconds
                sleep(5)
                master.quit()

                # make sure users are killed
                self.assertEqual(0, worker.user_count)
                # make sure events happened correctly
                self.assertIn(worker.client_id, worker_connect_events)

                self.assertEqual(worker_usage_monitor_mock.call_count, 5)
                self.assertEqual(master_usage_monitor_mock.call_count, 5)

                for call_args, call_kwargs in master_usage_monitor_mock:
                    self.assertEqual(call_args, ())  # args
                    self.assertEqual(
                        call_kwargs, {"environment": master_env, "cpu_usage": mock.ANY, "memory_usage": mock.ANY}
                    )  # kwargs
                    self.assertTrue(isinstance(call_kwargs["cpu_usage"], float))
                    self.assertTrue(isinstance(call_kwargs["memory_usage"], int))

                for call_args, call_kwargs in worker_usage_monitor_mock:
                    self.assertEqual(call_args, ())  # args
                    self.assertEqual(
                        call_kwargs, {"environment": worker_env, "cpu_usage": mock.ANY, "memory_usage": mock.ANY}
                    )  # kwargs
                    self.assertTrue(isinstance(call_kwargs["cpu_usage"], float))
                    self.assertTrue(isinstance(call_kwargs["memory_usage"], int))


class TestMasterRunner(LocustRunnerTestCase):
    def setUp(self):
        super().setUp()
        self.environment = Environment(events=locust.events, catch_exceptions=False)

    def tearDown(self):
        super().tearDown()

    def get_runner(self, user_classes=None):
        if user_classes is not None:
            self.environment.user_classes = user_classes
        return self.environment.create_master_runner("*", 5557)

    def test_worker_connect(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "zeh_fake_client1"))
            self.assertEqual(1, len(master.clients))
            self.assertTrue(
                "zeh_fake_client1" in master.clients, "Could not find fake client in master instance's clients dict"
            )
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "zeh_fake_client2"))
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "zeh_fake_client3"))
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "zeh_fake_client4"))
            self.assertEqual(4, len(master.clients))

            server.mocked_send(Message("quit", None, "zeh_fake_client3"))
            self.assertEqual(3, len(master.clients))

    def test_worker_connect_with_special_versions(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            # legacy handshake payloads (no generation support) are rejected
            server.mocked_send(Message("client_ready", None, "1.x_style_client_should_not_be_allowed"))
            self.assertEqual(1, len(self.mocked_log.error))
            self.assertEqual(0, len(master.clients))
            server.mocked_send(Message("client_ready", "abcd", "legacy_string_payload_should_not_be_allowed"))
            self.assertEqual(2, len(self.mocked_log.error))
            self.assertEqual(0, len(master.clients))
            server.mocked_send(Message("client_ready", -1, "legacy_int_payload_should_not_be_allowed"))
            self.assertEqual(3, len(self.mocked_log.error))
            self.assertEqual(0, len(master.clients))
            # the -1 version check bypass still works with the new dict payload
            server.mocked_send(
                Message(
                    "client_ready",
                    {"version": -1, "generation": -1, "state": STATE_INIT},
                    "version_check_bypass_should_not_warn",
                )
            )
            self.assertEqual(1, len(master.clients))
            self.assertEqual(0, len(self.mocked_log.warning))
            # a different patch version only produces a debug/warning log
            server.mocked_send(
                Message(
                    "client_ready",
                    {"version": __version__ + "1", "generation": -1, "state": STATE_INIT},
                    "difference_in_patch_version_should_not_warn",
                )
            )
            self.assertEqual(2, len(master.clients))
            self.assertEqual(0, len(self.mocked_log.warning))

    def test_worker_handshake_carries_generation_and_ack_reports_master_generation(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            master._command_ledger.generation = 5
            master._command_ledger.intent = runners.INTENT_SPAWN
            master._command_ledger.target = (10, 5.0, ())
            server.mocked_send(
                Message(
                    "client_ready",
                    {"version": __version__, "generation": 2, "state": STATE_RUNNING},
                    "reconnecting_worker",
                )
            )
            node = master.clients["reconnecting_worker"]
            self.assertEqual(2, node.last_generation)
            ack_messages = server.get_messages("ack")
            self.assertEqual(1, len(ack_messages))
            self.assertEqual(5, ack_messages[0].data["generation"])
            self.assertEqual(runners.INTENT_SPAWN, ack_messages[0].data["intent"])
            self.assertEqual((10, 5.0, ()), tuple(ack_messages[0].data["target"]))

    def test_duplicate_client_ready_preserves_running_worker_information(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            server.mocked_send(
                Message(
                    "client_ready",
                    {"version": __version__, "generation": 4, "state": STATE_RUNNING},
                    "worker1",
                )
            )
            node = master.clients["worker1"]
            node.user_classes_count = {"TestUser": 7}
            node.last_target = {"TestUser": 7}
            # worker resets its RPC connection and re-registers with the same id
            server.mocked_send(
                Message(
                    "client_ready",
                    {"version": __version__, "generation": 4, "state": STATE_RUNNING},
                    "worker1",
                )
            )
            node = master.clients["worker1"]
            self.assertEqual(STATE_RUNNING, node.state)
            self.assertEqual({"TestUser": 7}, node.user_classes_count)
            self.assertEqual({"TestUser": 7}, node.last_target)
            self.assertEqual(4, node.last_generation)

    def test_command_stale_from_worker_is_logged(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            server.mocked_send(
                Message(
                    "client_ready",
                    {"version": __version__, "generation": -1, "state": STATE_INIT},
                    "worker1",
                )
            )
            master._command_ledger.submit(runners.INTENT_SPAWN, target=(3, 1.0, ()), expected_workers=["worker1"])
            server.mocked_send(Message("command_stale", {"generation": 3, "state": STATE_RUNNING}, "worker1"))
            self.assertEqual(3, master.clients["worker1"].last_generation)
            self.assertTrue(any("stale command" in m for m in self.mocked_log.info))

    def test_worker_stats_report_median(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client"))

            master.stats.get("/", "GET").log(100, 23455)
            master.stats.get("/", "GET").log(800, 23455)
            master.stats.get("/", "GET").log(700, 23455)

            data = {"user_count": 1}
            self.environment.events.report_to_master.fire(client_id="fake_client", data=data)
            master.stats.clear_all()

            server.mocked_send(Message("stats", data, "fake_client"))
            s = master.stats.get("/", "GET")
            self.assertEqual(700, s.median_response_time)

    def test_worker_stats_report_with_none_response_times(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client"))

            master.stats.get("/mixed", "GET").log(0, 23455)
            master.stats.get("/mixed", "GET").log(800, 23455)
            master.stats.get("/mixed", "GET").log(700, 23455)
            master.stats.get("/mixed", "GET").log(None, 23455)
            master.stats.get("/mixed", "GET").log(None, 23455)
            master.stats.get("/mixed", "GET").log(None, 23455)
            master.stats.get("/mixed", "GET").log(None, 23455)
            master.stats.get("/onlyNone", "GET").log(None, 23455)

            data = {"user_count": 1}
            self.environment.events.report_to_master.fire(client_id="fake_client", data=data)
            master.stats.clear_all()

            server.mocked_send(Message("stats", data, "fake_client"))
            s1 = master.stats.get("/mixed", "GET")
            self.assertEqual(700, s1.median_response_time)
            self.assertEqual(500, s1.avg_response_time)
            s2 = master.stats.get("/onlyNone", "GET")
            self.assertEqual(0, s2.median_response_time)
            self.assertEqual(0, s2.avg_response_time)

    def test_master_marks_downed_workers_as_missing(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client"))
            sleep(6)
            # print(master.clients['fake_client'].__dict__)
            assert master.clients["fake_client"].state == STATE_MISSING

    def test_last_worker_quitting_stops_test(self):
        class TestUser(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        with (
            mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server,
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "0.1"),
        ):
            master = self.get_runner(user_classes=[TestUser])
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client1"))
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client2"))

            master.start(1, 2)
            server.mocked_send(Message("spawning", None, "fake_client1"))
            server.mocked_send(Message("spawning", None, "fake_client2"))

            server.mocked_send(Message("quit", None, "fake_client1"))
            sleep(0.1)
            self.assertEqual(1, len(master.clients.all))
            self.assertNotEqual(STATE_STOPPED, master.state, "Not all workers quit but test stopped anyway.")

            server.mocked_send(Message("quit", None, "fake_client2"))
            sleep(0.1)
            self.assertEqual(0, len(master.clients.all))
            self.assertEqual(STATE_STOPPED, master.state, "All workers quit but test didn't stop.")

    def test_quit_from_missing_worker_does_not_stop_test_if_other_workers_are_running(self):
        """
        worker_count already excludes missing workers (it only counts ready/spawning/running
        workers), so subtracting len(self.clients.missing) again when handling a "quit" message
        double-counts missing workers. If a previously-missing worker belatedly sends "quit"
        while at least one other worker is still actively running, the test must not be stopped.
        """
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            master.clients["fake_client1"] = WorkerNode("fake_client1")
            master.clients["fake_client1"].state = STATE_RUNNING
            master.clients["fake_client2"] = WorkerNode("fake_client2")
            master.clients["fake_client2"].state = STATE_MISSING
            master.clients["fake_client3"] = WorkerNode("fake_client3")
            master.clients["fake_client3"].state = STATE_MISSING
            master.state = STATE_RUNNING

            self.assertEqual(1, master.worker_count)

            # A worker that was marked missing eventually sends a belated "quit" message.
            # fake_client1 is still actively running, so the test must keep running.
            server.mocked_send(Message("quit", None, "fake_client2"))
            self.assertNotIn(
                master.state,
                [STATE_STOPPED, STATE_STOPPING],
                "A missing worker quitting stopped the test even though another worker is still running.",
            )

    @mock.patch("locust.runners.HEARTBEAT_INTERVAL", new=0.1)
    def test_last_worker_missing_stops_test(self):
        class TestUser(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        with (
            mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server,
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "0.1"),
        ):
            master = self.get_runner(user_classes=[TestUser])
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client1"))
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client2"))
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client3"))

            master.start(3, 3)
            server.mocked_send(Message("spawning", None, "fake_client1"))
            server.mocked_send(Message("spawning", None, "fake_client2"))
            server.mocked_send(Message("spawning", None, "fake_client3"))

            sleep(0.2)
            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client1",
                )
            )
            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client2",
                )
            )
            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client3",
                )
            )

            sleep(0.2)
            self.assertEqual(0, len(master.clients.missing))
            self.assertEqual(3, master.worker_count)
            self.assertNotIn(
                master.state, [STATE_STOPPED, STATE_STOPPING], "Not all workers went missing but test stopped anyway."
            )

            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client1",
                )
            )

            sleep(0.4)
            self.assertEqual(2, len(master.clients.missing))
            self.assertEqual(1, master.worker_count)
            self.assertNotIn(
                master.state, [STATE_STOPPED, STATE_STOPPING], "Not all workers went missing but test stopped anyway."
            )

            sleep(0.2)
            self.assertEqual(3, len(master.clients.missing))
            self.assertEqual(0, master.worker_count)
            self.assertEqual(STATE_STOPPED, master.state, "All workers went missing but test didn't stop.")

    @mock.patch("locust.runners.HEARTBEAT_INTERVAL", new=0.1)
    @mock.patch("locust.runners.HEARTBEAT_DEAD_INTERNAL", new=-3)
    def test_worker_missing_after_heartbeat_dead_interval(self):
        class TestUser(User):
            @task
            def my_task(self):
                gevent.sleep(600)

        with (
            mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server,
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "0.1"),
        ):
            master = self.get_runner(user_classes=[TestUser])
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client1"))
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client2"))
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client3"))

            master.start(3, 3)
            server.mocked_send(Message("spawning", None, "fake_client1"))
            server.mocked_send(Message("spawning", None, "fake_client2"))
            server.mocked_send(Message("spawning", None, "fake_client3"))

            sleep(0.1)
            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client1",
                )
            )
            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client2",
                )
            )
            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client3",
                )
            )

            sleep(0.1)
            # initially all workers are in active state
            self.assertEqual(0, len(master.clients.missing))
            self.assertEqual(3, master.worker_count)
            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client1",
                )
            )

            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client2",
                )
            )

            sleep(0.6)
            # 4 intervals are passed since all 3 heart beats all workers are in missing state
            self.assertEqual(3, len(master.clients.missing))
            self.assertEqual(0, master.worker_count)

            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client1",
                )
            )

            server.mocked_send(
                Message(
                    "heartbeat",
                    {"state": STATE_RUNNING, "current_cpu_usage": 50, "current_memory_usage": 200, "count": 1},
                    "fake_client2",
                )
            )
            sleep(0.2)
            # heartbeat received from two workers so they are active, for fake_client3 HEARTBEAT_DEAD_INTERNAL has been breached, so it will be removed from worker list
            self.assertEqual(0, len(master.clients.missing))
            self.assertEqual(2, master.worker_count)
            master.stop()

    def test_master_total_stats(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client"))
            stats = RequestStats()
            stats.log_request("GET", "/1", 100, 3546)
            stats.log_request("GET", "/1", 800, 56743)
            stats2 = RequestStats()
            stats2.log_request("GET", "/2", 700, 2201)
            server.mocked_send(
                Message(
                    "stats",
                    {
                        "stats": stats.serialize_stats(),
                        "stats_total": stats.total.serialize(),
                        "errors": stats.serialize_errors(),
                        "user_count": 1,
                    },
                    "fake_client",
                )
            )
            server.mocked_send(
                Message(
                    "stats",
                    {
                        "stats": stats2.serialize_stats(),
                        "stats_total": stats2.total.serialize(),
                        "errors": stats2.serialize_errors(),
                        "user_count": 2,
                    },
                    "fake_client",
                )
            )
            self.assertEqual(700, master.stats.total.median_response_time)

    def test_master_total_stats_with_none_response_times(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client"))
            stats = RequestStats()
            stats.log_request("GET", "/1", 100, 3546)
            stats.log_request("GET", "/1", 800, 56743)
            stats.log_request("GET", "/1", None, 56743)
            stats2 = RequestStats()
            stats2.log_request("GET", "/2", 700, 2201)
            stats2.log_request("GET", "/2", None, 2201)
            stats3 = RequestStats()
            stats3.log_request("GET", "/3", None, 2201)
            server.mocked_send(
                Message(
                    "stats",
                    {
                        "stats": stats.serialize_stats(),
                        "stats_total": stats.total.serialize(),
                        "errors": stats.serialize_errors(),
                        "user_count": 1,
                    },
                    "fake_client",
                )
            )
            server.mocked_send(
                Message(
                    "stats",
                    {
                        "stats": stats2.serialize_stats(),
                        "stats_total": stats2.total.serialize(),
                        "errors": stats2.serialize_errors(),
                        "user_count": 2,
                    },
                    "fake_client",
                )
            )
            server.mocked_send(
                Message(
                    "stats",
                    {
                        "stats": stats3.serialize_stats(),
                        "stats_total": stats3.total.serialize(),
                        "errors": stats3.serialize_errors(),
                        "user_count": 2,
                    },
                    "fake_client",
                )
            )
            self.assertEqual(700, master.stats.total.median_response_time)

    def test_master_current_response_times(self):
        start_time = 1
        with mock.patch("time.time") as mocked_time:
            mocked_time.return_value = start_time
            with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
                master = self.get_runner()
                self.environment.stats.reset_all()
                mocked_time.return_value += 1.0234
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client"))
                stats = RequestStats()
                stats.log_request("GET", "/1", 100, 3546)
                stats.log_request("GET", "/1", 800, 56743)
                server.mocked_send(
                    Message(
                        "stats",
                        {
                            "stats": stats.serialize_stats(),
                            "stats_total": stats.total.get_stripped_report(),
                            "errors": stats.serialize_errors(),
                            "user_count": 1,
                        },
                        "fake_client",
                    )
                )
                mocked_time.return_value += 1
                stats2 = RequestStats()
                stats2.log_request("GET", "/2", 400, 2201)
                server.mocked_send(
                    Message(
                        "stats",
                        {
                            "stats": stats2.serialize_stats(),
                            "stats_total": stats2.total.get_stripped_report(),
                            "errors": stats2.serialize_errors(),
                            "user_count": 2,
                        },
                        "fake_client",
                    )
                )
                mocked_time.return_value += 4
                self.assertEqual(400, master.stats.total.get_current_response_time_percentile(0.5))
                self.assertEqual(800, master.stats.total.get_current_response_time_percentile(0.95))

                # let 10 second pass, do some more requests, send it to the master and make
                # sure the current response time percentiles only accounts for these new requests
                mocked_time.return_value += 10.10023
                stats.log_request("GET", "/1", 20, 1)
                stats.log_request("GET", "/1", 30, 1)
                stats.log_request("GET", "/1", 3000, 1)
                server.mocked_send(
                    Message(
                        "stats",
                        {
                            "stats": stats.serialize_stats(),
                            "stats_total": stats.total.get_stripped_report(),
                            "errors": stats.serialize_errors(),
                            "user_count": 2,
                        },
                        "fake_client",
                    )
                )
                self.assertEqual(30, master.stats.total.get_current_response_time_percentile(0.5))
                self.assertEqual(3000, master.stats.total.get_current_response_time_percentile(0.95))

    @mock.patch("locust.runners.HEARTBEAT_INTERVAL", new=600)
    def test_rebalance_locust_users_on_worker_connect(self):
        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "zeh_fake_client1"))
            self.assertEqual(1, len(master.clients))
            self.assertTrue(
                "zeh_fake_client1" in master.clients, "Could not find fake client in master instance's clients dict"
            )

            master.start(100, 20)
            self.assertEqual(7, len(server.get_messages()))
            spawn_messages = server.get_messages("spawn")
            for i, msg in enumerate(server.get_messages("spawn")):
                self.assertDictEqual({"TestUser": int((i + 1) * 20)}, msg.data["user_classes_count"])

            # Normally, this attribute would be updated when the
            # master receives the report from the worker.
            master.clients["zeh_fake_client1"].user_classes_count = {"TestUser": 100}

            # let another worker connect
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "zeh_fake_client2"))
            self.assertEqual(2, len(master.clients))
            sleep(0.1)  # give time for messages to be sent to clients
            spawn_messages = server.get_messages("spawn")
            self.assertEqual({"TestUser": 50}, spawn_messages[-1].data["user_classes_count"])
            self.assertEqual({"TestUser": 50}, spawn_messages[-2].data["user_classes_count"])

    def test_sends_spawn_data_to_ready_running_spawning_workers(self):
        """Sends spawn job to running, ready, or spawning workers"""

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])
            master.clients[1] = WorkerNode("1")
            master.clients[2] = WorkerNode("2")
            master.clients[3] = WorkerNode("3")
            master.clients[1].state = STATE_INIT
            master.clients[2].state = STATE_SPAWNING
            master.clients[3].state = STATE_RUNNING
            master.start(user_count=5, spawn_rate=5)
            self.assertEqual(3, len(server.get_messages("spawn")))
            self.assertEqual(3, len(server.get_messages("spawning_complete")))

    def test_new_dispatch_excludes_missing_workers(self):
        class TestUser(User):
            @task
            def my_task(self):
                pass

        with (
            mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server,
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "0"),
        ):
            master = self.get_runner(user_classes=[TestUser])
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "missing"))
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "healthy"))

            master.start(2, 100)
            master.clients["missing"].state = STATE_MISSING
            master.stop(send_stop_to_client=False)
            master.update_state(STATE_STOPPED)

            previous_spawn_count = len(server.get_messages("spawn"))

            master.start(2, 100)

            spawn_messages = server.get_messages("spawn")[previous_spawn_count:]
            self.assertEqual(1, len(spawn_messages))
            self.assertEqual("healthy", spawn_messages[0].node_id)
            self.assertEqual({"TestUser": 2}, spawn_messages[0].data["user_classes_count"])

    def test_start_event(self):
        """
        Tests that test_start event is fired
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])

            run_count = [0]

            @self.environment.events.test_start.add_listener
            def on_test_start(*a, **kw):
                run_count[0] += 1

            for i in range(5):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))

            master.start(7, 7)
            self.assertEqual(15, len(server.get_messages()))
            self.assertEqual(1, run_count[0])

            # change number of users and check that test_start isn't fired again
            master.start(7, 7)
            self.assertEqual(1, run_count[0])

            # stop and start to make sure test_start is fired again
            master.stop()
            master.start(3, 3)
            self.assertEqual(2, run_count[0])

            master.quit()

    def test_stop_event(self):
        """
        Tests that test_stop event is fired
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])

            @self.environment.events.test_stopping.add_listener
            def on_test_stopping(*_, **__):
                self.runner_stopping = True

            @self.environment.events.test_stop.add_listener
            def on_test_stop(*_, **__):
                self.runner_stopped = True

            for i in range(5):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))

            master.start(7, 7)
            self.assertEqual(15, len(server.get_messages()))
            master.stop()
            self.assertTrue(self.runner_stopping)
            self.assertTrue(self.runner_stopped)

            self.reset_state()
            for i in range(5):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))
            master.start(7, 7)
            master.stop()
            master.quit()
            self.assertTrue(self.runner_stopping)
            self.assertTrue(self.runner_stopped)

    def test_stop_event_quit(self):
        """
        Tests that test_stop event is fired when quit() is called directly
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])

            @self.environment.events.test_stopping.add_listener
            def on_test_stopping(*_, **__):
                self.runner_stopping = True

            @self.environment.events.test_stop.add_listener
            def on_test_stop(*_, **__):
                self.runner_stopped = True

            for i in range(5):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))

            master.start(7, 7)
            self.assertEqual(15, len(server.get_messages()))
            master.quit()
            self.assertTrue(self.runner_stopping)
            self.assertTrue(self.runner_stopped)

    def test_spawn_zero_locusts(self):
        class MyTaskSet(TaskSet):
            @task
            def my_task(self):
                pass

        class MyTestUser(User):
            tasks = [MyTaskSet]
            wait_time = constant(0.1)

        environment = Environment(user_classes=[MyTestUser])
        runner = LocalRunner(environment)

        timeout = gevent.Timeout(2.0)
        timeout.start()

        try:
            runner.start(0, 1, wait=True)
            runner.spawning_greenlet.join()
        except gevent.Timeout:
            self.fail("Got Timeout exception. A locust seems to have been spawned, even though 0 was specified.")
        finally:
            timeout.cancel()

    def test_spawn_uneven_locusts(self):
        """
        Tests that we can accurately spawn a certain number of locusts, even if it's not an
        even number of the connected workers
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])

            for i in range(5):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))

            master.start(7, 7)
            self.assertEqual(15, len(server.outbox))

            num_users = sum(sum(msg.data["user_classes_count"].values()) for msg in server.get_messages("spawn"))
            self.assertEqual(7, num_users)

    def test_spawn_fewer_locusts_than_workers(self):
        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])

            for i in range(5):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))

            master.start(2, 2)
            self.assertEqual(15, len(server.outbox))

            num_users = sum(sum(msg.data["user_classes_count"].values()) for msg in server.get_messages("spawn"))

            self.assertEqual(2, num_users, "Total number of locusts that would have been spawned is not 2")

    def test_spawn_correct_worker_indexes(self):
        """
        Tests that workers would receive a monotonic sequence of ordinal IDs.
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])

            USERS_COUNT = 5

            for i in range(USERS_COUNT):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))

            master.start(USERS_COUNT, USERS_COUNT)
            self.assertEqual(USERS_COUNT * 3, len(server.outbox))

            indexes = [msg.data["index"] for msg in server.get_messages("ack")]
            self.assertEqual(USERS_COUNT, len(indexes), "Total number of locusts/workers is not 5")

            indexes.sort()
            for i in range(USERS_COUNT):
                self.assertEqual(indexes[i], i, "Worker index mismatch")

    def test_custom_shape_scale_interval(self):
        class MyUser(User):
            @task
            def my_task(self):
                pass

        class TestShape(LoadTestShape):
            def __init__(self):
                super().__init__()
                self._users_num = [1, 1, 1, 2, 2, 3, 3, 3, 4]
                self._index = 0

            def tick(self):
                if self._index >= len(self._users_num):
                    return None
                users_num = self._users_num[self._index]
                self._index += 1
                return users_num, users_num

        self.environment.shape_class = TestShape()

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[MyUser])
            for i in range(5):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))

            # Start the shape_worker
            self.environment.shape_class.reset_time()
            master.start_shape()

            # Wait for shape_worker to update user_count
            sleep(0.5)
            num_users = sum(sum(msg.data["user_classes_count"].values()) for msg in server.get_messages("spawn"))
            self.assertEqual(
                1, num_users, "Total number of users in first stage of shape test is not 1: %i" % num_users
            )

            # Wait for shape_worker to update user_count again
            sleep(1.5)
            num_users = sum(sum(msg.data["user_classes_count"].values()) for msg in server.get_messages("spawn"))
            self.assertEqual(
                1, num_users, "Total number of users in second stage of shape test is not 1: %i" % num_users
            )

            # Wait for shape_worker to update user_count few times but not reach the end yet
            sleep(2.5)
            num_users = sum(sum(msg.data["user_classes_count"].values()) for msg in server.get_messages("spawn"))
            self.assertEqual(
                3, num_users, "Total number of users in second stage of shape test is not 3: %i" % num_users
            )

            # Wait to ensure shape_worker has stopped the test
            sleep(3)
            self.assertEqual("stopped", master.state, "The test has not been stopped by the shape class")

    def test_custom_shape_scale_up(self):
        class MyUser(User):
            @task
            def my_task(self):
                pass

        class TestShape(LoadTestShape):
            def tick(self):
                run_time = self.get_run_time()
                if run_time < 2:
                    return 1, 1
                elif run_time < 4:
                    return 2, 2
                else:
                    return None

        self.environment.shape_class = TestShape()

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[MyUser])
            for i in range(5):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))

            # Start the shape_worker
            self.environment.shape_class.reset_time()
            master.start_shape()
            sleep(0.5)

            # Wait for shape_worker to update user_count
            num_users = sum(sum(msg.data["user_classes_count"].values()) for msg in server.get_messages("spawn"))
            self.assertEqual(
                1, num_users, "Total number of users in first stage of shape test is not 1: %i" % num_users
            )

            # Wait for shape_worker to update user_count again
            sleep(2)
            num_users = sum(sum(msg.data["user_classes_count"].values()) for msg in server.get_messages("spawn"))
            self.assertEqual(
                3, num_users, "Total number of users in second stage of shape test is not 3: %i" % num_users
            )

            # Wait to ensure shape_worker has stopped the test
            sleep(3)
            self.assertEqual("stopped", master.state, "The test has not been stopped by the shape class")

    def test_custom_shape_scale_down(self):
        class MyUser(User):
            @task
            def my_task(self):
                pass

        class TestShape(LoadTestShape):
            def tick(self):
                run_time = self.get_run_time()
                if run_time < 2:
                    return 5, 5
                elif run_time < 4:
                    return 1, 5
                else:
                    return None

        self.environment.shape_class = TestShape()

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[MyUser])
            for i in range(5):
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client%i" % i))

            # Start the shape_worker
            self.environment.shape_class.reset_time()
            master.start_shape()
            sleep(0.5)

            # Wait for shape_worker to update user_count
            num_users = sum(sum(msg.data["user_classes_count"].values()) for msg in server.get_messages("spawn"))
            self.assertEqual(
                5, num_users, "Total number of users in first stage of shape test is not 5: %i" % num_users
            )

            # Wait for shape_worker to update user_count again
            sleep(2)
            msgs = defaultdict(dict)
            for msg in server.get_messages("spawn"):
                msgs[msg.node_id][msg.data["timestamp"]] = sum(msg.data["user_classes_count"].values())
            # Count users for the last received messages
            num_users = sum(v[max(v.keys())] for v in msgs.values())
            self.assertEqual(
                1, num_users, "Total number of users in second stage of shape test is not 1: %i" % num_users
            )

            # Wait to ensure shape_worker has stopped the test
            sleep(3)
            self.assertEqual("stopped", master.state, "The test has not been stopped by the shape class")

    def test_exception_in_task(self):
        class MyUser(User):
            @task
            def will_error(self):
                raise HeyAnException(":(")

        self.environment.user_classes = [MyUser]
        runner = self.environment.create_local_runner()

        l = MyUser(self.environment)

        self.assertRaises(HeyAnException, l.run)
        self.assertRaises(HeyAnException, l.run)
        self.assertEqual(1, len(runner.exceptions))

        hash_key, exception = runner.exceptions.popitem()
        self.assertIn("traceback", exception)
        self.assertIn("HeyAnException", exception["traceback"])
        self.assertEqual(2, exception["count"])

    def test_exception_is_caught(self):
        """Test that exceptions are stored, and execution continues"""

        class MyTaskSet(TaskSet):
            def __init__(self, *a, **kw):
                super().__init__(*a, **kw)
                self._task_queue = deque([self.will_error, self.will_stop])

            @task(1)
            def will_error(self):
                raise HeyAnException(":(")

            @task(1)
            def will_stop(self):
                raise StopUser()

        class MyUser(User):
            wait_time = constant(0.01)
            tasks = [MyTaskSet]

        # set config to catch exceptions in locust users
        self.environment.catch_exceptions = True
        self.environment.user_classes = [MyUser]
        runner = LocalRunner(self.environment)
        l = MyUser(self.environment)

        # make sure HeyAnException isn't raised
        l.run()
        l.run()
        # make sure we got two entries in the error log
        self.assertEqual(2, len(self.mocked_log.error))

        # make sure exception was stored
        self.assertEqual(1, len(runner.exceptions))
        hash_key, exception = runner.exceptions.popitem()
        self.assertTrue("traceback" in exception)
        self.assertTrue("HeyAnException" in exception["traceback"])
        self.assertEqual(2, exception["count"])

    def test_master_reset_connection(self):
        """Test that connection will be reset when network issues found"""
        with mock.patch("locust.runners.FALLBACK_INTERVAL", new=0.1):
            with mock.patch("locust.rpc.rpc.Server", mocked_rpc(raise_on_close=False)) as server:
                master = self.get_runner()
                self.assertEqual(0, len(master.clients))
                server.mocked_send(Message("client_ready", NETWORK_BROKEN, "fake_client"))
                self.assertTrue(master.connection_broken)
                server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client"))
                sleep(1)
                self.assertFalse(master.connection_broken)
                self.assertEqual(1, len(master.clients))
                master.quit()

    def test_reset_connection_after_RPCError(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc(raise_on_close=False)) as server:
            master = self.get_runner()
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client"))
            sleep(0.2)
            self.assertFalse(master.connection_broken)
            self.assertEqual(1, len(master.clients))

            # Trigger RPCError
            server.mocked_send(Message("lets_trigger_RPCError", NETWORK_BROKEN, "fake_client"))
            self.assertTrue(master.connection_broken)
            sleep(1)
            self.assertFalse(master.connection_broken)
            master.quit()

    def test_attributes_populated_when_calling_start(self):
        class MyUser1(User):
            @task
            def my_task(self):
                pass

        class MyUser2(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[MyUser1, MyUser2])

            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "fake_client1"))

            master.start(7, 7)
            self.assertEqual({"MyUser1": 4, "MyUser2": 3}, master.target_user_classes_count)
            self.assertEqual(7, master.target_user_count)
            self.assertEqual(7, master.spawn_rate)

            master.start(10, 10)
            self.assertEqual({"MyUser1": 5, "MyUser2": 5}, master.target_user_classes_count)
            self.assertEqual(10, master.target_user_count)
            self.assertEqual(10, master.spawn_rate)

            master.start(1, 3)
            self.assertEqual({"MyUser1": 1, "MyUser2": 0}, master.target_user_classes_count)
            self.assertEqual(1, master.target_user_count)
            self.assertEqual(3, master.spawn_rate)

    def test_custom_message_send(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner()
            for i in range(5):
                master.clients[i] = WorkerNode(str(i))
            master.send_message("test_custom_msg", {"test_data": 123})

            messages = server.get_messages()

            self.assertEqual(5, len(messages))
            for msg in messages:
                self.assertEqual("test_custom_msg", msg.type)
                self.assertEqual(123, msg.data["test_data"])

    def test_custom_message_receive(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            test_custom_msg = [False]
            test_custom_msg_data = [{}]

            def on_custom_msg(msg, **kw):
                test_custom_msg[0] = True
                test_custom_msg_data[0] = msg.data

            master = self.get_runner()
            master.register_message("test_custom_msg", on_custom_msg)

            server.mocked_send(Message("test_custom_msg", {"test_data": 123}, "dummy_id"))

            self.assertTrue(test_custom_msg[0])
            self.assertEqual(123, test_custom_msg_data[0]["test_data"])

    def test_undefined_custom_message_receive(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            test_custom_msg = [False]

            def on_custom_msg(msg, **kw):
                test_custom_msg[0] = True

            master = self.get_runner()
            master.register_message("test_custom_msg", on_custom_msg)

            server.mocked_send(Message("unregistered_custom_msg", {}, "dummy_id"))

            self.assertFalse(test_custom_msg[0])
            self.assertEqual(1, len(self.mocked_log.warning))
            msg = self.mocked_log.warning[0]
            self.assertIn("Unknown message type received from worker", msg)

    def test_wait_for_workers_report_after_ramp_up(self):
        def assert_cache_hits():
            self.assertEqual(master._wait_for_workers_report_after_ramp_up.cache_info().hits, 0)
            master._wait_for_workers_report_after_ramp_up()
            self.assertEqual(master._wait_for_workers_report_after_ramp_up.cache_info().hits, 1)

        master = self.get_runner()

        master._wait_for_workers_report_after_ramp_up.cache_clear()
        self.assertEqual(master._wait_for_workers_report_after_ramp_up(), 1.0)
        assert_cache_hits()

        master._wait_for_workers_report_after_ramp_up.cache_clear()
        with patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "5.7"):
            self.assertEqual(master._wait_for_workers_report_after_ramp_up(), 5.7)
            assert_cache_hits()

        master._wait_for_workers_report_after_ramp_up.cache_clear()
        with (
            mock.patch("locust.runners.WORKER_REPORT_INTERVAL", new=1.5),
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "5.7 * WORKER_REPORT_INTERVAL"),
        ):
            self.assertEqual(master._wait_for_workers_report_after_ramp_up(), 5.7 * 1.5)
            assert_cache_hits()

        master._wait_for_workers_report_after_ramp_up.cache_clear()

    def test_master_discard_first_client_ready(self):
        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "dummy_client"))
            # discard first client_ready msg
            server.queue.get()
            master = self.get_runner()
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "dummy_client"))

            messages = server.get_messages()

            self.assertEqual(1, len(master.clients))
            self.assertEqual("ack", messages[0].type)
            self.assertEqual(1, len(messages))
            self.assertEqual(0, messages[0].data["index"])

    def test_worker_sends_bad_message_to_master(self):
        """
        Validate master sends reconnect message to worker when it receives a bad message.
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "zeh_fake_client1"))
            self.assertEqual(1, len(master.clients))
            self.assertTrue(
                "zeh_fake_client1" in master.clients, "Could not find fake client in master instance's clients dict"
            )

            master.start(10, 10)
            sleep(0.1)
            server.mocked_send(Message("stats", BAD_MESSAGE, "zeh_fake_client1"))
            messages = server.get_messages()
            self.assertEqual(messages[0].type, "ack")
            self.assertEqual(messages[1].type, "spawn")
            self.assertEqual(messages[2].type, "spawning_complete")
            self.assertEqual(messages[3].type, "reconnect")
            self.assertEqual(messages[4].type, "ack")

            # Expected message order in outbox: ack, spawn, reconnect, ack
            self.assertEqual(
                "reconnect", messages[3].type, "Master didn't send worker reconnect message when expected."
            )

    def test_worker_sends_unrecognized_message_to_master(self):
        """
        Validate master ignores message from worker when it cannot parse address info.
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "zeh_fake_client1"))
            self.assertEqual(1, len(master.clients))
            self.assertTrue(
                "zeh_fake_client1" in master.clients, "Could not find fake client in master instance's clients dict"
            )

            master.start(10, 10)
            sleep(0.1)
            server.mocked_send(Message("stats", UNRECOGNIZED_MESSAGE, "zeh_fake_client1"))
            self.assertEqual(3, len(server.get_messages()))

    def test_unknown_host_sends_message_to_master(self):
        """
        Validate master ignores message that is sent from unknown host
        """

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server:
            master = self.get_runner(user_classes=[TestUser])
            server.mocked_send(Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "zeh_fake_client1"))
            self.assertEqual(1, len(master.clients))
            self.assertTrue(
                "zeh_fake_client1" in master.clients, "Could not find fake client in master instance's clients dict"
            )

            master.start(10, 10)
            sleep(0.1)
            server.mocked_send(Message("stats", UNRECOGNIZED_HOST_MESSAGE, "unknown_host"))
            self.assertEqual(3, len(server.get_messages()))

    def _master_with_fake_workers(self, server, user_class, count):
        master = self.get_runner(user_classes=[user_class])
        for i in range(count):
            server.mocked_send(
                Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, f"w{i}")
            )
        return master

    def test_command_result_times_out_with_missing_list_then_confirms_on_drop(self):
        """TR-4.2: a missing ack times out with an accurate missing list; dropping the
        missing worker while still pending lets the result converge to confirmed."""

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with (
            mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server,
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "2"),
        ):
            master = self._master_with_fake_workers(server, TestUser, 2)

            def slow_start():
                return master.start(2, 2)

            start_greenlet = gevent.spawn(slow_start)
            sleep(0.2)
            result = master.last_command_result
            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual({"w0", "w1"}, result.expected_workers)

            # only w0 acknowledges the generation
            server.mocked_send(
                Message(
                    "spawning_complete",
                    {"user_classes_count": {"TestUser": 1}, "user_count": 1, "generation": result.generation},
                    "w0",
                )
            )
            sleep(0.05)
            self.assertEqual({"w1"}, result.missing_workers)
            self.assertFalse(result.is_terminal)

            # w1 is declared dead (heartbeat removal): the in-flight result can now confirm
            master._command_ledger.drop_worker("w1")
            start_greenlet.join(timeout=1)
            self.assertTrue(result.confirmed)
            self.assertEqual({"w0"}, result.acked_workers)

            # stop(): fake workers never ack, but a reported user count of zero converges
            # the stop generation immediately (matching the historical behaviour)
            stop_result = master.stop()
            self.assertIsNotNone(stop_result)
            assert stop_result is not None
            self.assertTrue(stop_result.is_terminal)
            self.assertEqual("stop", stop_result.intent)

    def test_command_stale_triggers_resend_of_current_generation(self):
        """TR-4.3: command_stale from a worker makes the master re-dispatch the current
        intent to exactly that worker, and its later ack converges the result."""

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with (
            mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server,
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "5"),
        ):
            master = self._master_with_fake_workers(server, TestUser, 1)

            start_greenlet = gevent.spawn(master.start, 1, 1)
            sleep(0.2)
            result = master.last_command_result
            assert result is not None
            generation = result.generation

            spawn_messages = [m for m in server.get_messages("spawn") if m.node_id == "w0"]
            self.assertEqual(1, len(spawn_messages))
            self.assertEqual(generation, spawn_messages[0].data["generation"])
            self.assertEqual({"TestUser": 1}, master.clients["w0"].last_target)

            # worker reports it discarded the command (its watermark is older)
            server.outbox.clear()
            server.mocked_send(Message("command_stale", {"generation": generation - 1, "state": "running"}, "w0"))
            sleep(0.05)
            resent = server.get_messages("spawn")
            self.assertEqual(1, len(resent))
            self.assertEqual(generation, resent[0].data["generation"])
            self.assertEqual("w0", resent[0].node_id)

            # the worker now accepts and acknowledges the generation
            server.mocked_send(
                Message(
                    "command_ack",
                    {
                        "generation": generation,
                        "intent": "spawn",
                        "state": "running",
                        "user_classes_count": {"TestUser": 1},
                        "user_count": 1,
                    },
                    "w0",
                )
            )
            start_greenlet.join(timeout=1)
            self.assertTrue(result.confirmed)

    def test_dispatched_commands_carry_generations_and_same_intent_reuses_it(self):
        """TR-4.4: every spawn/stop/quit carries an integer generation; re-dispatching the
        same (already terminal) intent reuses the generation number instead of bumping."""

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with (
            mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server,
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "0.05"),
        ):
            master = self._master_with_fake_workers(server, TestUser, 2)

            result1 = master.start(4, 4)
            self.assertIsNotNone(result1)
            assert result1 is not None
            g1 = result1.generation
            self.assertIsInstance(g1, int)
            spawns_g1 = server.get_messages("spawn")
            self.assertTrue(spawns_g1)
            for message in spawns_g1:
                self.assertEqual(g1, message.data["generation"])
                self.assertIsInstance(message.data["generation"], int)

            # re-dispatch the exact same target after terminal: same generation number
            result2 = master.start(4, 4)
            self.assertIsNotNone(result2)
            assert result2 is not None
            self.assertEqual(g1, result2.generation)
            self.assertIsNot(result1, result2)
            for message in server.get_messages("spawn"):
                self.assertEqual(g1, message.data["generation"])

            # a changed target allocates a new generation
            result3 = master.start(6, 6)
            assert result3 is not None
            self.assertEqual(g1 + 1, result3.generation)

            stop_result = master.stop()
            assert stop_result is not None
            self.assertEqual(g1 + 2, stop_result.generation)
            stop_messages = server.get_messages("stop")
            self.assertTrue(stop_messages)
            for message in stop_messages:
                self.assertEqual(stop_result.generation, message.data["generation"])

            # quit allocates the highest generation and carries it on the wire
            server.outbox.clear()
            master.quit()
            sleep(0)
            quit_messages = server.get_messages("quit")
            self.assertTrue(quit_messages)
            for message in quit_messages:
                self.assertEqual(stop_result.generation + 1, message.data["generation"])

    def test_concurrent_stops_share_result_and_fire_lifecycle_once(self):
        """TR-5.3: concurrent/re-entrant stop() calls send one stop generation, fire
        test_stopping/test_stop once, and all return the same result object."""

        class TestUser(User):
            @task
            def my_task(self):
                pass

        with (
            mock.patch("locust.rpc.rpc.Server", mocked_rpc()) as server,
            patch_env("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP", "2"),
        ):
            master = self._master_with_fake_workers(server, TestUser, 2)

            counts = {"stopping": 0, "stop": 0}

            @self.environment.events.test_stopping.add_listener
            def on_test_stopping(*_, **__):
                counts["stopping"] += 1

            @self.environment.events.test_stop.add_listener
            def on_test_stop(*_, **__):
                counts["stop"] += 1

            start_greenlet = gevent.spawn(master.start, 2, 2)
            sleep(0.2)
            self.assertEqual(STATE_SPAWNING, master.state)

            # Pretend users are still running so the first stop parks in its wait loop,
            # letting the concurrent calls observe STATE_STOPPING and join the result.
            with mock.patch.object(
                type(master), "user_count", new_callable=mock.PropertyMock, return_value=5
            ):
                stop_greenlets = [gevent.spawn(master.stop) for _ in range(3)]
                sleep(0.3)
                # exactly one stop generation was dispatched, one message per worker
                stop_messages = [m for m in server.outbox if m.type == "stop"]
                self.assertEqual(2, len(stop_messages))
                self.assertEqual(1, len({m.data["generation"] for m in stop_messages}))
                self.assertEqual(STATE_STOPPING, master.state)
                self.assertEqual(1, counts["stopping"])
            # once the reported user count drops to zero the joined calls all converge
            gevent.joinall(stop_greenlets, timeout=10)
            self.assertTrue(all(g.ready() and g.value is not None for g in stop_greenlets))
            shared_result = stop_greenlets[0].value
            for g in stop_greenlets:
                self.assertIs(shared_result, g.value)
            self.assertTrue(shared_result.confirmed)
            self.assertEqual({"stopping": 1, "stop": 1}, counts)

            # once all workers report ready the master transitions to STOPPED and a
            # further stop is a no-op
            master.check_stopped()
            self.assertEqual(STATE_STOPPED, master.state)
            self.assertIsNone(master.stop())
            self.assertEqual({"stopping": 1, "stop": 1}, counts)

            start_greenlet.join(timeout=5)

    def test_local_runner_concurrent_starts_and_stops_fire_lifecycle_once_per_generation(self):
        """TR-5.4: LocalRunner concurrent starts converge to the last target with the
        lifecycle firing at most once per running generation, even across stop/start."""

        class TestUser(User):
            wait_time = constant(0)

            @task
            def my_task(self):
                pass

        environment = Environment(user_classes=[TestUser])
        runner = environment.create_local_runner()

        counts = {"start": 0, "stopping": 0, "stop": 0}

        @environment.events.test_start.add_listener
        def on_test_start(*_, **__):
            counts["start"] += 1

        @environment.events.test_stopping.add_listener
        def on_test_stopping(*_, **__):
            counts["stopping"] += 1

        @environment.events.test_stop.add_listener
        def on_test_stop(*_, **__):
            counts["stop"] += 1

        greenlets = [gevent.spawn(runner.start, target, spawn_rate=1000) for target in (10, 20, 30)]
        gevent.joinall(greenlets, timeout=5)

        @retry(AssertionError, tries=20, delay=0.1)
        def assert_final_target():
            self.assertEqual(30, runner.user_count)

        assert_final_target()
        self.assertEqual(1, counts["start"])

        runner.stop()
        self.assertEqual(0, runner.user_count)
        self.assertEqual(1, counts["stopping"])
        self.assertEqual(1, counts["stop"])

        # a re-entrant stop is a no-op
        runner.stop()
        self.assertEqual(1, counts["stopping"])
        self.assertEqual(1, counts["stop"])

        # second running generation
        runner.start(10, spawn_rate=1000)

        @retry(AssertionError, tries=20, delay=0.1)
        def assert_second_target():
            self.assertEqual(10, runner.user_count)

        assert_second_target()
        runner.stop()
        self.assertEqual({"start": 2, "stopping": 2, "stop": 2}, counts)


class TestWorkerRunner(LocustTestCase):
    def setUp(self):
        super().setUp()
        # self._report_to_master_event_handlers = [h for h in events.report_to_master._handlers]

    def tearDown(self):
        # events.report_to_master._handlers = self._report_to_master_event_handlers
        super().tearDown()

    def get_runner(self, client, environment=None, user_classes=None, auto_connect=True):
        if auto_connect:
            client.mocked_send(Message("ack", {"index": 0}, "dummy_client_id"))
        if environment is None:
            environment = self.environment
        user_classes = user_classes or []
        environment.user_classes = user_classes
        return WorkerRunner(environment, master_host="localhost", master_port=5557)

    def test_worker_stop_timeout(self):
        class MyTestUser(User):
            _test_state = 0

            @task
            def the_task(self):
                MyTestUser._test_state = 1
                gevent.sleep(0.2)
                MyTestUser._test_state = 2

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            worker = self.get_runner(environment=Environment(), user_classes=[MyTestUser], client=client)
            messages = client.get_messages()
            self.assertEqual(1, len(messages))
            self.assertEqual("client_ready", messages[0].type)
            client.mocked_send(
                Message(
                    "spawn",
                    {
                        "timestamp": 1605538584,
                        "user_classes_count": {"MyTestUser": 1},
                        "host": "",
                        "stop_timeout": 1,
                        "parsed_options": {},
                    },
                    "dummy_client_id",
                )
            )
            self.assertTrue(client.get_messages("spawning"))
            # wait for worker to spawn locusts
            # self.assertIn("spawning", [m.type for m in messages])
            worker.spawning_greenlet.join()
            self.assertEqual(1, len(worker.user_greenlets))
            # check that locust has started running
            gevent.sleep(0.01)
            self.assertEqual(1, MyTestUser._test_state)
            # send stop message
            client.mocked_send(Message("stop", None, "dummy_client_id"))
            worker.user_greenlets.join()
            # check that locust user got to finish
            self.assertEqual(2, MyTestUser._test_state)

    def test_worker_without_stop_timeout(self):
        class MyTestUser(User):
            _test_state = 0

            @task
            def the_task(self):
                MyTestUser._test_state = 1
                gevent.sleep(0.2)
                MyTestUser._test_state = 2

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            worker = self.get_runner(environment=Environment(), user_classes=[MyTestUser], client=client)
            messages = client.get_messages()
            self.assertEqual(1, len(messages))
            self.assertEqual("client_ready", messages[0].type)
            client.mocked_send(
                Message(
                    "spawn",
                    {
                        "timestamp": 1605538584,
                        "user_classes_count": {"MyTestUser": 1},
                        "host": "",
                        "stop_timeout": None,
                        "parsed_options": {},
                    },
                    "dummy_client_id",
                )
            )
            # print("outbox:", client.outbox)
            # wait for worker to spawn locusts
            self.assertTrue(client.get_messages("spawning"))
            worker.spawning_greenlet.join()
            self.assertEqual(1, len(worker.user_greenlets))
            # check that locust has started running
            gevent.sleep(0.01)
            self.assertEqual(1, MyTestUser._test_state)
            # send stop message
            client.mocked_send(Message("stop", None, "dummy_client_id"))
            worker.user_greenlets.join()
            # check that locust user did not get to finish
            self.assertEqual(1, MyTestUser._test_state)

    def spawn_message(self, generation, counts):
        return Message(
            "spawn",
            {
                "timestamp": 1605538584 + generation,
                "generation": generation,
                "user_classes_count": counts,
                "host": "",
                "stop_timeout": None,
                "parsed_options": {},
            },
            "dummy_client_id",
        )

    def test_spawn_message_with_older_generation_is_rejected(self):
        class MyUser(User):
            wait_time = constant(1)

            def start(self, group: Group):
                # We do this so that the spawning does not finish
                # too quickly
                gevent.sleep(0.1)
                return super().start(group)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)

            client.mocked_send(self.spawn_message(1, {"MyUser": 10}))
            sleep(0.6)
            self.assertEqual(STATE_SPAWNING, worker.state)
            worker.spawning_greenlet.join()
            self.assertEqual(10, worker.user_count)

            # Send same generation with the same target: no-op but re-acknowledged
            client.outbox.clear()
            client.mocked_send(self.spawn_message(1, {"MyUser": 10}))
            sleep(0)
            # Still 10 users, no new spawning dispatch, but an ack was produced
            self.assertEqual(10, worker.user_count)
            self.assertTrue(client.get_messages("command_ack"))
            self.assertFalse(client.get_messages("spawning"))

            # Send older generation: must be discarded and reported as stale
            client.outbox.clear()
            client.mocked_send(self.spawn_message(0, {"MyUser": 2}))
            sleep(0)
            # Still 10 users
            self.assertEqual(10, worker.user_count)
            self.assertTrue(client.get_messages("command_stale"))
            self.assertFalse(client.get_messages("spawning"))

            # Send newer generation: applies
            client.mocked_send(self.spawn_message(2, {"MyUser": 2}))
            worker.spawning_greenlet.join()
            self.assertEqual(2, worker.user_count)

            worker.quit()

    def test_same_generation_rebalance_applies_without_lifecycle_refire(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            environment = Environment()
            test_start_count = [0]

            @environment.events.test_start.add_listener
            def on_test_start(**kw):
                test_start_count[0] += 1

            worker = self.get_runner(environment=environment, user_classes=[MyUser], client=client)

            client.mocked_send(self.spawn_message(0, {"MyUser": 5}))
            worker.spawning_greenlet.join()
            self.assertEqual(5, worker.user_count)
            self.assertEqual(1, test_start_count[0])

            # Same generation, changed target: apply the delta, no lifecycle refire
            client.mocked_send(self.spawn_message(0, {"MyUser": 2}))
            worker.spawning_greenlet.join()
            self.assertEqual(2, worker.user_count)
            self.assertEqual(1, test_start_count[0])

            # Same generation, same target: no-op with re-ack
            client.outbox.clear()
            client.mocked_send(self.spawn_message(0, {"MyUser": 2}))
            sleep(0)
            self.assertEqual(2, worker.user_count)
            self.assertEqual(1, test_start_count[0])
            self.assertTrue(client.get_messages("command_ack"))

            worker.quit()

    def test_stale_commands_have_no_side_effects(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            environment = Environment()
            test_stop_count = [0]

            @environment.events.test_stop.add_listener
            def on_test_stop(**kw):
                test_stop_count[0] += 1

            worker = self.get_runner(environment=environment, user_classes=[MyUser], client=client)

            # Establish running at generation 2
            client.mocked_send(self.spawn_message(2, {"MyUser": 1}))
            worker.spawning_greenlet.join()
            self.assertEqual(1, worker.user_count)

            # Stale stop: no side effects, worker keeps its users, stale reported
            client.outbox.clear()
            client.mocked_send(Message("stop", {"generation": 1}, "dummy_client_id"))
            sleep(0)
            self.assertEqual(1, worker.user_count)
            self.assertEqual(0, test_stop_count[0])
            self.assertTrue(client.get_messages("command_stale"))
            self.assertFalse(client.get_messages("command_ack"))
            self.assertFalse(client.get_messages("client_stopped"))
            self.assertEqual(STATE_RUNNING, worker.worker_state)

            # Stale spawn: likewise discarded
            client.outbox.clear()
            client.mocked_send(self.spawn_message(1, {"MyUser": 50}))
            sleep(0)
            self.assertEqual(1, worker.user_count)
            self.assertTrue(client.get_messages("command_stale"))

            # Current generation stop: applies and stops exactly once
            client.mocked_send(Message("stop", {"generation": 3}, "dummy_client_id"))
            sleep(0.05)
            self.assertEqual(0, worker.user_count)
            self.assertEqual(1, test_stop_count[0])

            # Stale quit cannot kill a live worker (it is discarded); current quit works
            client.mocked_send(
                Message(
                    "spawn",
                    {
                        "generation": 4,
                        "user_classes_count": {"MyUser": 1},
                        "host": "",
                        "stop_timeout": None,
                        "parsed_options": {},
                    },
                    "dummy_client_id",
                )
            )
            worker.spawning_greenlet.join()
            self.assertEqual(1, worker.user_count)
            client.outbox.clear()
            client.mocked_send(Message("quit", {"generation": 2}, "dummy_client_id"))
            sleep(0)
            self.assertTrue(client.get_messages("command_stale"))
            self.assertEqual(1, worker.user_count)
            # The stale quit did not kill the worker; a newer command is still processed
            client.outbox.clear()
            client.mocked_send(self.spawn_message(5, {"MyUser": 2}))
            worker.spawning_greenlet.join()
            self.assertEqual(2, worker.user_count)

            worker.quit()

    def test_duplicate_stop_is_idempotent(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            environment = Environment()
            counts = {"stopping": 0, "stop": 0}

            @environment.events.test_stopping.add_listener
            def on_test_stopping(**kw):
                counts["stopping"] += 1

            @environment.events.test_stop.add_listener
            def on_test_stop(**kw):
                counts["stop"] += 1

            worker = self.get_runner(environment=environment, user_classes=[MyUser], client=client)
            client.mocked_send(self.spawn_message(0, {"MyUser": 1}))
            worker.spawning_greenlet.join()

            client.mocked_send(Message("stop", {"generation": 1}, "dummy_client_id"))
            sleep(0.05)
            self.assertEqual(1, counts["stopping"])
            self.assertEqual(1, counts["stop"])

            # Duplicate stop at the same generation: ack again, no lifecycle refire
            client.outbox.clear()
            client.mocked_send(Message("stop", {"generation": 1}, "dummy_client_id"))
            sleep(0.05)
            self.assertEqual(1, counts["stopping"])
            self.assertEqual(1, counts["stop"])
            self.assertTrue(client.get_messages("command_ack"))
            self.assertFalse(client.get_messages("client_stopped"))

    def test_worker_messages_sent_to_master(self):
        """
        Ensure that worker includes both "user_count" and "user_classes_count"
        when reporting to the master.
        """

        class MyUser(User):
            wait_time = constant(1)

            def start(self, group: Group):
                # We do this so that the spawning does not finish
                # too quickly
                gevent.sleep(0.1)
                return super().start(group)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)

            client.mocked_send(
                Message(
                    "spawn",
                    {
                        "timestamp": 1605538584,
                        "user_classes_count": {"MyUser": 10},
                        "host": "",
                        "stop_timeout": None,
                        "parsed_options": {},
                    },
                    "dummy_client_id",
                )
            )
            sleep(0.6)
            self.assertEqual(STATE_SPAWNING, worker.state)
            worker.spawning_greenlet.join()
            self.assertEqual(10, worker.user_count)

            sleep(2)

            message = client.get_messages("stats")[-1]
            self.assertIsNotNone(message)
            self.assertIn("user_count", message.data)
            self.assertIn("user_classes_count", message.data)
            self.assertEqual(message.data["user_count"], 10)
            self.assertEqual(message.data["user_classes_count"]["MyUser"], 10)

            message = client.get_messages("spawning_complete")[0]
            self.assertIsNotNone(message)
            self.assertIn("user_count", message.data)
            self.assertIn("user_classes_count", message.data)
            self.assertEqual(message.data["user_count"], 10)
            self.assertEqual(message.data["user_classes_count"]["MyUser"], 10)

            worker.quit()

    def test_worker_heartbeat_messages_sent_to_master(self):
        """
        Validate content of the heartbeat payload sent to the master.
        """

        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)

            t0 = time.perf_counter()
            while len(client.get_messages("heartbeat")) == 0:
                self.assertLessEqual(time.perf_counter() - t0, 3)
                sleep(0.1)

            message = client.get_messages("heartbeat")[-1]
            self.assertEqual(len(message.data), 4)
            self.assertIn("state", message.data)
            self.assertIn("current_cpu_usage", message.data)
            self.assertIn("current_memory_usage", message.data)
            self.assertIn("generation", message.data)

            worker.quit()

    def test_reset_rpc_connection_to_master(self):
        """
        Validate worker resets RPC connection to master on "reconnect" message.
        """

        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc(raise_on_close=False)) as client:
            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)
            client.mocked_send(
                Message(
                    "spawn",
                    {
                        "timestamp": 1605538584,
                        "user_classes_count": {"MyUser": 10},
                        "host": "",
                        "stop_timeout": None,
                        "parsed_options": {},
                    },
                    "dummy_client_id",
                )
            )
            sleep(0.6)
            self.assertEqual(STATE_RUNNING, worker.state)
            with self.assertLogs("locust.runners") as capture:
                with mock.patch("locust.rpc.rpc.Client.close") as close:
                    client.mocked_send(
                        Message(
                            "reconnect",
                            None,
                            "dummy_client_id",
                        )
                    )
                    sleep(0)
                    worker.spawning_greenlet.join()
                    worker.quit()
                    close.assert_called_once()
            self.assertIn(
                "WARNING:locust.runners:Received reconnect message from master. Resetting RPC connection.",
                capture.output,
            )

    def test_change_user_count_during_spawning(self):
        class MyUser(User):
            wait_time = constant(1)

            def start(self, group: Group):
                # We do this so that the spawning does not finish
                # too quickly
                gevent.sleep(0.1)
                return super().start(group)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)

            client.mocked_send(
                Message(
                    "spawn",
                    {
                        "timestamp": 1605538584,
                        "user_classes_count": {"MyUser": 10},
                        "host": "",
                        "stop_timeout": None,
                        "parsed_options": {},
                    },
                    "dummy_client_id",
                )
            )
            sleep(0.6)
            self.assertEqual(STATE_SPAWNING, worker.state)
            client.mocked_send(
                Message(
                    "spawn",
                    {
                        "timestamp": 1605538585,
                        "user_classes_count": {"MyUser": 9},
                        "host": "",
                        "stop_timeout": None,
                        "parsed_options": {},
                    },
                    "dummy_client_id",
                )
            )
            sleep(0)
            worker.spawning_greenlet.join()
            self.assertEqual(9, len(worker.user_greenlets))
            worker.quit()

    def test_computed_properties(self):
        class MyUser1(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        class MyUser2(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            worker = self.get_runner(environment=Environment(), user_classes=[MyUser1, MyUser2], client=client)

            client.mocked_send(
                Message(
                    "spawn",
                    {
                        "timestamp": 1605538584,
                        "user_classes_count": {"MyUser1": 10, "MyUser2": 10},
                        "host": "",
                        "stop_timeout": None,
                        "parsed_options": {},
                    },
                    "dummy_client_id",
                )
            )
            worker.spawning_greenlet.join()
            self.assertDictEqual(worker.user_classes_count, {"MyUser1": 10, "MyUser2": 10})
            self.assertDictEqual(worker.target_user_classes_count, {"MyUser1": 10, "MyUser2": 10})
            self.assertEqual(worker.target_user_count, 20)

            client.mocked_send(
                Message(
                    "spawn",
                    {
                        "timestamp": 1605538585,
                        "user_classes_count": {"MyUser1": 1, "MyUser2": 2},
                        "host": "",
                        "stop_timeout": None,
                        "parsed_options": {},
                    },
                    "dummy_client_id",
                )
            )
            worker.spawning_greenlet.join()
            self.assertDictEqual(worker.user_classes_count, {"MyUser1": 1, "MyUser2": 2})
            self.assertDictEqual(worker.target_user_classes_count, {"MyUser1": 1, "MyUser2": 2})
            self.assertEqual(worker.target_user_count, 3)

            worker.quit()

    def test_custom_message_send(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)
            client.outbox.clear()
            worker.send_message("test_custom_msg", {"test_data": 123})
            messages = client.get_messages()
            self.assertEqual("test_custom_msg", messages[0].type)
            self.assertEqual(123, messages[0].data["test_data"])
            worker.quit()

    def test_custom_message_receive(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            test_custom_msg = [False]
            test_custom_msg_data = [{}]

            def on_custom_msg(msg, **kw):
                test_custom_msg[0] = True
                test_custom_msg_data[0] = msg.data

            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)
            worker.register_message("test_custom_msg", on_custom_msg)

            client.mocked_send(Message("test_custom_msg", {"test_data": 123}, "dummy_client_id"))

            self.assertTrue(test_custom_msg[0])
            self.assertEqual(123, test_custom_msg_data[0]["test_data"])
            worker.quit()

    def test_undefined_custom_message_receive(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            test_custom_msg = [False]

            def on_custom_msg(msg, **kw):
                test_custom_msg[0] = True

            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)
            worker.register_message("test_custom_msg", on_custom_msg)

            client.mocked_send(Message("unregistered_custom_msg", {}, "dummy_id"))

            self.assertFalse(test_custom_msg[0])
            self.assertEqual(1, len(self.mocked_log.warning))
            msg = self.mocked_log.warning[0]
            self.assertIn("Unknown message type received", msg)

    def test_start_event(self):
        class MyTestUser(User):
            _test_state = 0

            @task
            def the_task(self):
                MyTestUser._test_state = 1
                gevent.sleep(0.2)
                MyTestUser._test_state = 2

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            environment = Environment()
            run_count = [0]

            @environment.events.test_start.add_listener
            def on_test_start(*args, **kw):
                run_count[0] += 1

            worker = self.get_runner(environment=environment, user_classes=[MyTestUser], client=client)
            messages = client.get_messages()
            self.assertEqual(1, len(messages))
            self.assertEqual("client_ready", messages[0].type)
            client.mocked_send(self.spawn_message(0, {"MyTestUser": 1}))
            # wait for worker to spawn locusts
            self.assertTrue(client.get_messages("spawning"))
            worker.spawning_greenlet.join()
            self.assertEqual(1, len(worker.user_greenlets))
            self.assertEqual(1, run_count[0])

            # check that locust has started running
            gevent.sleep(0.01)
            self.assertEqual(1, MyTestUser._test_state)

            # change number of users and check that test_start isn't fired again
            client.mocked_send(self.spawn_message(0, {"MyTestUser": 1}))
            self.assertEqual(1, run_count[0])

            # stop and start to make sure test_start is fired again
            client.mocked_send(Message("stop", {"generation": 1}, "dummy_client_id"))
            client.mocked_send(self.spawn_message(2, {"MyTestUser": 1}))
            gevent.sleep(0.01)
            self.assertEqual(2, run_count[0])

            client.mocked_send(Message("stop", {"generation": 3}, "dummy_client_id"))

    def test_stop_event(self):
        class MyTestUser(User):
            _test_state = 0

            @task
            def the_task(self):
                MyTestUser._test_state = 1
                gevent.sleep(0.2)
                MyTestUser._test_state = 2

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            environment = Environment()
            run_count = [0]

            @environment.events.test_stop.add_listener
            def on_test_stop(*args, **kw):
                run_count[0] += 1

            worker = self.get_runner(environment=environment, user_classes=[MyTestUser], client=client)
            messages = client.get_messages()
            self.assertEqual(1, len(messages))
            self.assertEqual("client_ready", messages[0].type)
            client.mocked_send(self.spawn_message(0, {"MyTestUser": 1}))

            # wait for worker to spawn locusts
            self.assertTrue(client.get_messages("spawning"))
            worker.spawning_greenlet.join()
            self.assertEqual(1, len(worker.user_greenlets))

            # check that locust has started running
            gevent.sleep(0.01)
            self.assertEqual(1, MyTestUser._test_state)

            # stop and make sure test_stop is fired
            client.mocked_send(Message("stop", {"generation": 1}, "dummy_client_id"))
            gevent.sleep(0.01)
            self.assertEqual(1, run_count[0])

            # stop with the same generation and make sure the event isn't fired again
            client.mocked_send(Message("stop", {"generation": 1}, "dummy_client_id"))
            gevent.sleep(0.01)
            self.assertEqual(1, run_count[0])

            # start and stop to check that the event is fired again
            client.mocked_send(self.spawn_message(2, {"MyTestUser": 1}))
            worker.spawning_greenlet.join()
            client.mocked_send(Message("stop", {"generation": 3}, "dummy_client_id"))
            gevent.sleep(0.01)
            self.assertEqual(2, run_count[0])

    def test_worker_connect_success(self):
        class MyTestUser(User):
            @task
            def the_task(self):
                pass

        with mock.patch("locust.runners.CONNECT_TIMEOUT", new=1):
            with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
                worker = self.get_runner(environment=Environment(), user_classes=[MyTestUser], client=client)
                messages = client.get_messages()

                self.assertEqual("client_ready", messages[0].type)
                self.assertEqual(1, len(messages))
                self.assertTrue(worker.connected)

    def test_worker_connect_failure(self):
        class MyTestUser(User):
            @task
            def the_task(self):
                pass

        with mock.patch("locust.runners.CONNECT_TIMEOUT", new=0.01):
            with mock.patch("locust.runners.CONNECT_RETRY_COUNT", new=1):
                with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
                    with self.assertRaises(ConnectionError):
                        self.get_runner(
                            environment=Environment(), user_classes=[MyTestUser], client=client, auto_connect=False
                        )
                    self.assertEqual(2, len(client.outbox))

    def test_send_logs(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            short_time = 0.05

            log_handler = LogReader()
            log_handler.name = "log_reader"
            log_handler.setLevel(logging.INFO)
            logger = logging.getLogger("root")
            logger.addHandler(log_handler)
            log_line = "some log info"
            logger.info(log_line)

            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)

            gevent.sleep(short_time)

            messages = client.get_messages()

            self.assertEqual("logs", messages[3].type)
            self.assertEqual(log_line, messages[3].data.get("logs", [])[0])
            self.assertEqual(worker.client_id, messages[3].data.get("worker_id"))
            worker.quit()

    def test_quit_worker_logs(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        with mock.patch("locust.rpc.rpc.Client", mocked_rpc()) as client:
            short_time = 0.05

            log_handler = LogReader()
            log_handler.name = "log_reader"
            log_handler.setLevel(logging.INFO)
            logger = logging.getLogger("root")
            logger.addHandler(log_handler)
            log_line = "spamming log"

            for _ in range(71):
                logger.info(log_line)

            worker = self.get_runner(environment=Environment(), user_classes=[MyUser], client=client)

            gevent.sleep(short_time)

            message = client.get_messages("logs")[0]

            self.assertEqual(
                "The worker attempted to send more than 70 log lines in one interval. Further log sending was disabled for this worker.",
                message.data.get("logs", [])[-1],
            )
            self.assertEqual(worker.client_id, message.data.get("worker_id"))
            worker.quit()
            logger.removeHandler(log_handler)


class TestMessageSerializing(unittest.TestCase):
    def test_message_serialize(self):
        msg = Message("client_ready", {"version": __version__, "generation": -1, "state": "ready"}, "my_id")
        rebuilt = Message.unserialize(msg.serialize())
        self.assertEqual(msg.type, rebuilt.type)
        self.assertEqual(msg.data, rebuilt.data)
        self.assertEqual(msg.node_id, rebuilt.node_id)


class TestStopTimeout(LocustTestCase):
    def test_stop_timeout(self):
        short_time = 0.05

        class MyTaskSet(TaskSet):
            @task
            def my_task(self):
                MyTaskSet.state = "first"
                gevent.sleep(short_time)
                MyTaskSet.state = "second"  # should only run when run time + stop_timeout is > short_time
                gevent.sleep(short_time)
                MyTaskSet.state = "third"  # should only run when run time + stop_timeout is > short_time * 2

        class MyTestUser(User):
            tasks = [MyTaskSet]

        environment = Environment(user_classes=[MyTestUser])
        runner = environment.create_local_runner()
        runner.start(1, 1, wait=False)
        gevent.sleep(short_time / 2)
        runner.quit()
        self.assertEqual("first", MyTaskSet.state)

        # exit with timeout
        environment = Environment(user_classes=[MyTestUser], stop_timeout=short_time / 2)
        runner = environment.create_local_runner()
        runner.start(1, 1, wait=False)
        gevent.sleep(short_time)
        runner.quit()
        self.assertEqual("second", MyTaskSet.state)

        # allow task iteration to complete, with some margin
        environment = Environment(user_classes=[MyTestUser], stop_timeout=short_time * 3)
        runner = environment.create_local_runner()
        runner.start(1, 1, wait=False)
        gevent.sleep(short_time)
        timeout = gevent.Timeout(short_time * 2)
        timeout.start()
        try:
            runner.quit()
            runner.greenlet.join()
        except gevent.Timeout:
            self.fail("Got Timeout exception. Some locusts must have kept running after iteration finish")
        finally:
            timeout.cancel()
        self.assertEqual("third", MyTaskSet.state)

    def test_stop_timeout_during_on_start(self):
        short_time = 0.05

        class MyTaskSet(TaskSet):
            finished_on_start = False
            my_task_run = False

            def on_start(self):
                gevent.sleep(short_time)
                MyTaskSet.finished_on_start = True

            @task
            def my_task(self):
                MyTaskSet.my_task_run = True

        class MyTestUser(User):
            tasks = [MyTaskSet]

        environment = create_environment([MyTestUser], mocked_options())
        environment.stop_timeout = short_time
        runner = environment.create_local_runner()
        runner.start(1, 1)
        gevent.sleep(short_time / 2)
        runner.quit()

        self.assertTrue(MyTaskSet.finished_on_start)
        self.assertFalse(MyTaskSet.my_task_run)

    def test_stop_timeout_exit_during_wait(self):
        short_time = 0.05

        class MyTaskSet(TaskSet):
            @task
            def my_task(self):
                pass

        class MyTestUser(User):
            tasks = [MyTaskSet]
            wait_time = constant(1)

        environment = Environment(user_classes=[MyTestUser], stop_timeout=short_time)
        runner = environment.create_local_runner()
        runner.start(1, 1)
        gevent.sleep(short_time)  # sleep to make sure locust has had time to start waiting
        timeout = gevent.Timeout(short_time)
        timeout.start()
        try:
            runner.quit()
            runner.greenlet.join()
        except gevent.Timeout:
            self.fail("Got Timeout exception. Waiting locusts should stop immediately, even when using stop_timeout.")
        finally:
            timeout.cancel()

    def test_stop_timeout_with_interrupt(self):
        short_time = 0.05

        class MySubTaskSet(TaskSet):
            @task
            def a_task(self):
                gevent.sleep(0)
                self.interrupt(reschedule=True)

        class MyTaskSet(TaskSet):
            tasks = [MySubTaskSet]

        class MyTestUser(User):
            tasks = [MyTaskSet]

        environment = create_environment([MyTestUser], mocked_options())
        environment.stop_timeout = short_time
        runner = environment.create_local_runner()
        runner.start(1, 1, wait=True)
        gevent.sleep(0)
        timeout = gevent.Timeout(short_time)
        timeout.start()
        try:
            runner.quit()
            runner.greenlet.join()
        except gevent.Timeout:
            self.fail("Got Timeout exception. Interrupted locusts should exit immediately during stop_timeout.")
        finally:
            timeout.cancel()

    def test_stop_timeout_with_interrupt_no_reschedule(self):
        state = [0]

        class MySubTaskSet(TaskSet):
            @task
            def a_task(self):
                gevent.sleep(0.1)
                state[0] = 1
                self.interrupt(reschedule=False)

        class MyTestUser(User):
            tasks = [MySubTaskSet]
            wait_time = constant(3)

        options = mocked_options()
        options.stop_timeout = 0.3
        environment = create_environment([MyTestUser], options)
        runner = environment.create_local_runner()
        runner.start(1, 1, wait=True)
        gevent.sleep(0)
        timeout = gevent.Timeout(0.11)
        timeout.start()
        try:
            runner.quit()
            runner.greenlet.join()
        except gevent.Timeout:
            self.fail("Got Timeout exception. Interrupted locusts should exit immediately during stop_timeout.")
        finally:
            timeout.cancel()
        self.assertEqual(1, state[0])

    def test_kill_locusts_with_stop_timeout(self):
        short_time = 0.05

        class MyTaskSet(TaskSet):
            @task
            def my_task(self):
                MyTaskSet.state = "first"
                gevent.sleep(short_time)
                MyTaskSet.state = "second"  # should only run when run time + stop_timeout is > short_time
                gevent.sleep(short_time)
                MyTaskSet.state = "third"  # should only run when run time + stop_timeout is > short_time * 2

        class MyTestUser(User):
            tasks = [MyTaskSet]

        environment = create_environment([MyTestUser], mocked_options())
        runner = environment.create_local_runner()
        runner.start(1, 1)
        gevent.sleep(short_time / 2)
        runner.stop_users({MyTestUser.__name__: 1})
        self.assertEqual("first", MyTaskSet.state)
        runner.quit()
        environment.runner = None

        environment.stop_timeout = short_time / 2  # exit with timeout
        runner = environment.create_local_runner()
        runner.start(1, 1)
        gevent.sleep(short_time)
        runner.stop_users({MyTestUser.__name__: 1})
        self.assertEqual("second", MyTaskSet.state)
        runner.quit()
        environment.runner = None

        environment.stop_timeout = short_time * 3  # allow task iteration to complete, with some margin
        runner = environment.create_local_runner()
        runner.start(1, 1)
        gevent.sleep(short_time)
        timeout = gevent.Timeout(short_time * 2)
        timeout.start()
        try:
            runner.stop_users({MyTestUser.__name__: 1})
            runner.user_greenlets.join()
        except gevent.Timeout:
            self.fail("Got Timeout exception. Some locusts must have kept running after iteration finish")
        finally:
            timeout.cancel()
        self.assertEqual("third", MyTaskSet.state)

    def test_users_can_call_runner_quit_with_stop_timeout(self):
        class BaseUser(User):
            wait_time = constant(1)

            @task
            def trigger(self):
                self.environment.runner.quit()

        runner = Environment(user_classes=[BaseUser]).create_local_runner()
        runner.environment.stop_timeout = 1
        runner.spawn_users({BaseUser.__name__: 1}, wait=False)
        timeout = gevent.Timeout(0.5)
        timeout.start()
        try:
            runner.greenlet.join()
        except gevent.Timeout:
            self.fail("Got Timeout exception, runner must have hung somehow.")
        finally:
            timeout.cancel()

    def test_gracefully_handle_exceptions_in_listener(self):
        class MyUser(User):
            wait_time = constant(1)

            @task
            def my_task(self):
                pass

        test_stop_run = [0]
        environment = Environment(user_classes=[MyUser])

        def on_test_stop_ok(*args, **kwargs):
            test_stop_run[0] += 1

        def on_test_stop_fail(*args, **kwargs):
            assert False

        environment.events.test_stop.add_listener(on_test_stop_ok)
        environment.events.test_stop.add_listener(on_test_stop_fail)
        environment.events.test_stop.add_listener(on_test_stop_ok)

        runner = LocalRunner(environment)
        runner.start(user_count=3, spawn_rate=3, wait=False)
        self.assertEqual(0, test_stop_run[0])
        runner.stop()
        self.assertEqual(2, test_stop_run[0])

    def test_stop_timeout_with_ramp_down(self):
        class MyTaskSet(TaskSet):
            @task
            def my_task(self):
                gevent.sleep(1)

        class MyTestUser(User):
            tasks = [MyTaskSet]

        environment = Environment(user_classes=[MyTestUser], stop_timeout=2)
        runner = environment.create_local_runner()

        # Start load test, wait for users to start, then trigger ramp down
        ts = time.perf_counter()
        runner.start(10, 10, wait=False)
        runner.spawning_greenlet.join()
        delta = time.perf_counter() - ts
        self.assertTrue(
            0 <= delta <= 0.05, f"Expected user count to increase to 10 instantaneously, instead it took {delta:f}"
        )
        self.assertTrue(
            runner.user_count == 10, "User count has not decreased correctly to 2, it is : %i" % runner.user_count
        )

        ts = time.perf_counter()
        runner.start(2, 4, wait=False)
        runner.spawning_greenlet.join()
        delta = time.perf_counter() - ts
        self.assertTrue(2 <= delta <= 2.05, f"Expected user count to decrease to 2 in 2s, instead it took {delta:f}")
        self.assertTrue(
            runner.user_count == 2, "User count has not decreased correctly to 2, it is : %i" % runner.user_count
        )


class TestClusterCommandPrimitives(unittest.TestCase):
    def test_result_confirms_when_all_expected_workers_ack(self):
        result = runners.ClusterCommandResult(1, runners.INTENT_SPAWN, expected_workers=["w1", "w2"])
        self.assertEqual(runners.COMMAND_PENDING, result.status)
        result.ack("w1")
        self.assertFalse(result.confirmed)
        self.assertEqual({"w2"}, result.missing_workers)
        result.ack("w2", payload={"user_count": 3})
        self.assertTrue(result.confirmed)
        self.assertEqual(set(), result.missing_workers)
        self.assertEqual({"user_count": 3}, result.ack_payloads["w2"])
        # late acks do not change a terminal result
        self.assertFalse(result.ack("w3"))
        self.assertTrue(result.confirmed)

    def test_result_wait_blocks_until_completion(self):
        result = runners.ClusterCommandResult(1, runners.INTENT_SPAWN, expected_workers=["w1"])
        gevent.spawn_later(0.01, lambda: result.ack("w1"))
        returned = result.wait(timeout=2)
        self.assertIs(result, returned)
        self.assertTrue(result.confirmed)

    def test_result_timeout_records_missing_snapshot(self):
        result = runners.ClusterCommandResult(1, runners.INTENT_SPAWN, expected_workers=["w1", "w2"])
        result.ack("w1")
        self.assertTrue(result.finish(runners.COMMAND_TIMEOUT))
        self.assertEqual(runners.COMMAND_TIMEOUT, result.status)
        self.assertEqual({"w2"}, result.missing_workers)
        # terminal transition happens only once
        self.assertFalse(result.finish(runners.COMMAND_CONFIRMED))
        self.assertEqual(runners.COMMAND_TIMEOUT, result.status)

    def test_result_dropping_last_missing_worker_completes(self):
        result = runners.ClusterCommandResult(1, runners.INTENT_SPAWN, expected_workers=["w1", "w2"])
        result.ack("w1")
        result.drop_expected("w2")
        self.assertTrue(result.confirmed)

    def test_result_empty_expected_set_does_not_auto_confirm(self):
        result = runners.ClusterCommandResult(1, runners.INTENT_SPAWN)
        result.ack("w1")
        self.assertFalse(result.confirmed)
        self.assertEqual(runners.COMMAND_PENDING, result.status)

    def test_ledger_increments_only_on_intent_change_and_coalesces_pending(self):
        ledger = runners.ClusterCommandLedger()
        r1 = ledger.submit(runners.INTENT_SPAWN, target=("t1",), expected_workers=["w1"])
        self.assertEqual(1, r1.generation)
        # same intent+target while pending -> same generation, same result, workers unioned
        r1_again = ledger.submit(runners.INTENT_SPAWN, target=("t1",), expected_workers=["w2"])
        self.assertIs(r1, r1_again)
        self.assertEqual({"w1", "w2"}, r1.expected_workers)
        # different target -> new generation, old result superseded
        r2 = ledger.submit(runners.INTENT_SPAWN, target=("t2",), expected_workers=["w1"])
        self.assertEqual(2, r2.generation)
        self.assertEqual(runners.COMMAND_SUPERSEDED, r1.status)
        self.assertEqual({"w1", "w2"}, r1.missing_workers)
        self.assertEqual(r2, ledger.current)
        # stop is a different intent -> generation 3
        r3 = ledger.submit(runners.INTENT_STOP, expected_workers=["w1"])
        self.assertEqual(3, r3.generation)
        self.assertEqual(runners.COMMAND_SUPERSEDED, r2.status)

    def test_ledger_reuses_generation_when_same_intent_resubmitted_after_terminal(self):
        ledger = runners.ClusterCommandLedger()
        r1 = ledger.submit(runners.INTENT_SPAWN, target=("t1",), expected_workers=["w1"])
        r1.ack("w1")
        self.assertTrue(r1.confirmed)
        # rebalance / reconnect re-dispatch of the very same cluster intent
        r2 = ledger.submit(runners.INTENT_SPAWN, target=("t1",), expected_workers=["w1"])
        self.assertIsNot(r1, r2)
        self.assertEqual(1, r2.generation)
        self.assertEqual(runners.COMMAND_PENDING, r2.status)

    def test_ledger_observe_ack_routes_only_to_current_generation(self):
        ledger = runners.ClusterCommandLedger()
        r1 = ledger.submit(runners.INTENT_SPAWN, target=("t1",), expected_workers=["w1"])
        self.assertIsNone(ledger.observe_ack("w1", 99))
        self.assertEqual(set(), r1.acked_workers)
        self.assertIs(r1, ledger.observe_ack("w1", 1))
        self.assertTrue(r1.confirmed)
        # acks for a terminal generation are ignored
        self.assertIsNone(ledger.observe_ack("w1", 1))

    def test_ledger_drop_worker_removes_from_pending_result(self):
        ledger = runners.ClusterCommandLedger()
        r1 = ledger.submit(runners.INTENT_SPAWN, target=("t1",), expected_workers=["w1", "w2"])
        ledger.observe_ack("w1", 1)
        ledger.drop_worker("w2")
        self.assertTrue(r1.confirmed)

    def test_generation_payloads_survive_message_round_trip(self):
        for msg_type, data in [
            ("spawn", {"timestamp": 1.0, "generation": 7, "user_classes_count": {"A": 3}}),
            ("stop", {"generation": 8}),
            ("quit", {"generation": 9}),
            ("command_stale", {"generation": 6, "state": STATE_RUNNING}),
            ("client_ready", runners.make_client_ready_data(__version__, 5, STATE_RUNNING)),
        ]:
            roundtrip = Message.unserialize(Message(msg_type, data, "w1").serialize())
            self.assertEqual(msg_type, roundtrip.type)
            self.assertEqual("w1", roundtrip.node_id)
            self.assertEqual(data, roundtrip.data)


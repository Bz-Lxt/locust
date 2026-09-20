.. _running-distributed:

===========================
Distributed load generation
===========================

A single process running Locust can simulate a reasonably high throughput. For a simple test plan and small payloads it can make more than a thousand requests per second, possibly over ten thousand if you use :ref:`FastHttpUser <increase-performance>`.

But if your test plan is complex or you want to run even more load, you'll need to scale out to multiple processes, maybe even multiple machines. Fortunately, Locust supports distributed runs out of the box.

To do this, you start one instance of Locust with the ``--master`` flag and one or more using the ``--worker`` flag. The master instance runs Locust's web interface, and tells the workers when to spawn/stop Users. The worker instances run your Users and send statistics back to the master. The master instance doesn't run any Users itself.

To simplify startup, you can use the ``--processes`` flag. It will launch a master process and the specified number of worker processes. It can also be used in combination with ``--worker``, then it will only launch workers. This feature relies on `fork() <https://linux.die.net/man/3/fork>`_ so it doesn't work on Windows.

.. note::
    Because Python cannot fully utilize more than one core per process (see `GIL <https://realpython.com/python-gil/>`_), you need to run one worker instance per processor core in order to have access to all your computing power.

.. note::
    There is almost no limit to how many Users you can run per worker. Locust/gevent can run thousands or even tens of thousands of Users per process just fine, as long as their total request rate (RPS) is not too high.

    If Locust *is* getting close to running out of CPU resources, it will log a warning. If there is no warning but you are still unable to generate the expected load, then the problem must be :ref:`increaserr`.

Single machine
==============

It is really simple to launch a master and 4 worker processes::

    locust --processes 4

You can even auto-detect the number of logical cores in your machine and launch one worker for each of them::

    locust --processes -1

Multiple machines
=================

Start locust in master mode on one machine::

    locust -f my_locustfile.py --master

And then on each worker machine:

.. code-block:: bash

    locust -f - --worker --master-host <your master> --processes 4

.. note::
    The ``-f -`` argument tells Locust to get the locustfile from master instead of from its local filesystem. This only works for single locustfiles.

Multiple machines, using locust-swarm
=====================================

.. note::
    locust-swarm is no longer actively maintained.

When you make changes to the locustfile you'll need to restart all Locust processes. `locust-swarm <https://github.com/SvenskaSpel/locust-swarm>`_ automates this for you. It also solves the issue of firewall/network access from workers to master using SSH tunnels (this is often a problem if the master is running on your workstation and workers are running in some datacenter).

.. code-block:: bash

    pip install locust-swarm

    swarm -f my_locustfile.py --loadgen-list worker-server1,worker-server2 <any other regular locust parameters>


Options for distributed load generation
=======================================

``--master-host <hostname or ip>``
----------------------------------

Optionally used together with ``--worker`` to set the hostname/IP of the master node (defaults
to localhost)

``--master-port <port number>``
-------------------------------

Optionally used together with ``--worker`` to set the port number of the master node (defaults to 5557).

``--master-bind-host <ip>``
---------------------------

Optionally used together with ``--master``. Determines which network interface the master node
will bind to. Defaults to * (all available interfaces).

``--master-bind-port <port number>``
------------------------------------

Optionally used together with ``--master``. Determines what network ports that the master node will
listen to. Defaults to 5557.

``--expect-workers <number of workers>``
----------------------------------------

Used when starting the master node with ``--headless``. The master node will then wait until X worker
nodes has connected before the test is started.

Communicating across nodes
=============================================

When running Locust in distributed mode, you may want to communicate between master and worker nodes in 
order to coordinate the test. This can be easily accomplished with custom messages using the built in messaging hooks:

.. code-block:: python

    from locust import events
    from locust.runners import MasterRunner, WorkerRunner

    # Fired when the worker receives a message of type 'test_users'
    def setup_test_users(environment, msg, **kwargs):
        for user in msg.data:
            print(f"User {user['name']} received")
        environment.runner.send_message('acknowledge_users', f"Thanks for the {len(msg.data)} users!")

    # Fired when the master receives a message of type 'acknowledge_users'
    def on_acknowledge(msg, **kwargs):
        print(msg.data)

    @events.init.add_listener
    def on_locust_init(environment, **_kwargs):
        if not isinstance(environment.runner, MasterRunner):
            environment.runner.register_message('test_users', setup_test_users)
        if not isinstance(environment.runner, WorkerRunner):
            environment.runner.register_message('acknowledge_users', on_acknowledge)

    @events.test_start.add_listener
    def on_test_start(environment, **_kwargs):
        if not isinstance(environment.runner, WorkerRunner):
            users = [
                {"name": "User1"},
                {"name": "User2"},
                {"name": "User3"},
            ]
            environment.runner.send_message('test_users', users)  

Note that when running locally (i.e. non-distributed), this functionality will be preserved; 
the messages will simply be handled by the runner that sends them.

.. note::
    Using the default options while registering a message handler will run the listener function
    in a **blocking** way, resulting in the heartbeat and other messages being delayed for the amount
    of the execution.
    If you think that your message handler will need to run for more than a second then you can register it
    as **concurrent**. Locust will then make it run in its own greenlet. Note that these greenlets will never 
    be join():ed.

    .. code-block::

        environment.runner.register_message('test_users', setup_test_users, concurrent=True)

For more details, see the `complete example <https://github.com/locustio/locust/tree/master/examples/custom_messages.py>`_.


Draining a worker node (rolling maintenance)
=============================================

During a long-running distributed test you may want to take a single worker out of
service, for example to restart or upgrade it, without stopping the whole test or
losing the total user load. The master can *drain* a specific online worker:

- The drained worker stays connected and keeps reporting stats, but all of its users
  are stopped. The stopping honors ``stop_timeout``, so users stuck in long tasks are
  force-stopped once that timeout has elapsed.
- The users it was running are replenished, **per user class**, on the other online
  workers. Spawning is rate limited (``spawn_rate`` users/s, rounded per round) and is
  sent as higher absolute user targets, so the existing user sessions on the other
  workers are never stopped or restarted.
- Drain requests are processed one worker at a time. You can queue several workers and
  they will be drained sequentially.
- Draining is idempotent: repeating a drain request for an already drained, queued or
  draining worker is a no-op, and a worker that disconnects and reconnects while marked
  as drained will not receive users again (and is not replenished twice).
- Stopping the test (``master.stop()`` / the Stop button) immediately interrupts any
  drain in progress.
- At least one other online (non-drained, non-queued) worker must exist; the last
  load-bearing worker cannot be drained.

A drained worker can be made schedulable again with *undrain*. This only re-admits the
worker into the scheduling pool — it does **not** move users back onto it; the worker
will pick up users on the next ramp-up, rebalance or ``start()`` with a higher user
count. A queued (not yet started) drain can be cancelled with undrain. Undraining a
worker that is currently being drained raises/returns a conflict instead; wait for the
drain to finish first.

Python API
----------

.. code-block:: python

    from locust import events
    from locust.runners import MasterRunner

    @events.init.add_listener
    def on_locust_init(environment, **kwargs):
        if environment.runner and isinstance(environment.runner, MasterRunner):
            # replenish the drained worker's users at 20 users/s (defaults to the
            # current test's spawn rate when omitted)
            environment.runner.drain_worker("worker_id_1", spawn_rate=20)

            # put it back into the pool of schedulable workers (no users are moved back)
            environment.runner.undrain_worker("worker_id_1")

``drain_worker`` raises ``LookupError`` for an unknown worker id, ``ValueError`` when no
test is running, when the spawn rate is invalid, or when no other online worker can take
the users, and ``undrain_worker`` raises ``WorkerDrainInProgress`` while the drain is
still running.

REST API
--------

Worker ids are listed by the ``id`` field of each entry in ``workers`` from
``GET /stats/requests`` (drained workers also carry ``"drained": true``).

.. code-block:: bash

    # drain a worker, replenishing its users at up to 20 users/s
    curl -X POST http://localhost:8089/drain -d "worker_id=worker_id_1" -d "spawn_rate=20"

    # make the worker schedulable again
    curl -X POST http://localhost:8089/undrain -d "worker_id=worker_id_1"

Both endpoints are only available on master runs and require authentication when
``--web-login`` is enabled. They respond with JSON ``{"success": ..., "message": ...}``.
Status codes: ``200`` success (also for idempotent repeats/no-ops), ``400`` invalid
request (missing/invalid parameters, non-master run, no other online worker),
``404`` unknown worker id, ``409`` undrain requested while that worker is draining.


Running distributed with Docker
=============================================

See :ref:`running-in-docker`


Running Locust distributed without the web UI
=============================================

See :ref:`running-distributed-without-web-ui`


Increase Locust's performance
=============================

If you're planning to run large-scale load tests, you might be interested to use the alternative
HTTP client that's shipped with Locust. You can read more about it here: :ref:`increase-performance`.

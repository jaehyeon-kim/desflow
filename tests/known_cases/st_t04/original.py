"""ST-T04: Three processes wait for the same timeout. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_timeout.py test_shared_timeout
    env, log, L = new()
    olog = []

    def child(k, timeout):
        yield timeout
        olog.append((k, env.now))
        L(k, "child", {"event_type": "woke", "at": env.now})

    timeout = env.timeout(1)
    arrival(env, "kids", lambda k: child(k, timeout), initial=3)
    r = go(env, log)
    assert olog == [("kids-1", 1), ("kids-2", 1), ("kids-3", 1)]
    return r

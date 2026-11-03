"""ST-U01: A child started after a delay. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_start_delayed: start_delayed(env, pem(env), delay=5)
    import simpy.util

    env, log, L = new()

    def pem(env, k):
        assert env.now == 5
        L(k, "child", {"event_type": "started", "at": env.now})
        yield env.timeout(1)

    def parent(k):
        L(k, "parent", {"event_type": "parent"})
        simpy.util.start_delayed(env, pem(env, "child-" + k.rsplit("-", 1)[1]), delay=5)
        return
        yield

    arrival(env, "p", parent, initial=1)
    return go(env, log)

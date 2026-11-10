"""ST-P04: Two child calls that return the time. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_process.py test_return_value
    env, log, L = new()

    def child(k):
        yield env.timeout(1)
        L(k, "child", {"now": env.now})
        return env.now

    def parent(k):
        result1 = yield env.process(child(k))
        L(k, "parent", {"event_type": "first"})
        result2 = yield env.process(child(k))
        assert [result1, result2] == [1, 2]
        L(k, "second", {"event_type": "second", "r1": result1, "r2": result2})

    arrival(env, "p", parent, initial=1)
    return go(env, log)

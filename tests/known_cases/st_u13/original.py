"""ST-U13: Any of a list of ten timeouts. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_any_of
    env, log, L = new()

    def pem(k):
        events = [env.timeout(i, value=i) for i in range(10)]
        results = yield env.any_of(events)
        assert results == {events[0]: 0}
        assert env.now == 0
        L(k, "pem", {"event_type": "first", "at": env.now})

    arrival(env, "a", pem, initial=1)
    return go(env, log)

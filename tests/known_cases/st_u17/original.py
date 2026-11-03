"""ST-U17: Empty any-of and all-of conditions succeed at once. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def p(k):
        results = yield env.any_of([])
        assert results == {}  # test_empty_any_of
        L(k, "empty_any", {"event_type": "any_empty"})
        results = yield env.all_of([])
        assert results == {}  # test_empty_all_of
        L(k, "empty_all", {"event_type": "all_empty"})

    arrival(env, "a", p, initial=1)
    return go(env, log)

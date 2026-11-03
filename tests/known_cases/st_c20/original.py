"""ST-C20: An all-of condition over no events succeeds at once. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_condition.py test_all_of_empty_list and test_any_of_empty_list (ST-C20, C21)
    env, log, L = new()

    def a(k):
        evt = env.all_of([])
        assert evt.triggered
        yield evt
        L(k, "empty_all", {"event_type": "all_empty", "at": env.now})
        evt = env.any_of([])
        assert evt.triggered
        yield evt
        L(k, "empty_any", {"event_type": "any_empty", "at": env.now})

    arrival(env, "a", a, initial=1)
    return go(env, log)

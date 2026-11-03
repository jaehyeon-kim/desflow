"""P16: Set a constant attribute. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        L(k, "t", {"event_type": "set"})
        L(k, "u", {"event_type": "read", "stage": 2})
        yield env.timeout(0)

    arrival(env, "a", t, initial=1)
    return go(env, log)

"""P18: Read a global variable. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def t(k):
        L(k, "t", {"event_type": "read", "limit": 5})
        yield env.timeout(0)

    VARS["limit"] = 5  # the variable the blueprint reads
    arrival(env, "a", t, gap=1, max_events=3)
    return go(env, log)

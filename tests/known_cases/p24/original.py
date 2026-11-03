"""P24: A loop with a counter. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = (
        new()
    )  # c_routing.py C1: for i in range(3): yield env.timeout(1), starting at 0

    def work(k):
        for tries in range(1, 4):
            yield env.timeout(1)
            L(k, "work", {"event_type": "tried"})
        L(k, "done", {"event_type": "done", "tries": tries})

    arrival(env, "a", work, initial=1)
    return go(env, log)

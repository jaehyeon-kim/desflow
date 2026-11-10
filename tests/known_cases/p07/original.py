"""P07: An arrival rate that is a function of time. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = (
        new()
    )  # yaml-only/programs/A4.py: while True: yield timeout(gap(env.now)); run(until=8)

    def t(k):
        L(k, "t", {"event_type": "arrived"})
        yield env.timeout(0)

    arrival(env, "a", t, gap=lambda: 1 + env.now / 4)
    return go(env, log, 8)

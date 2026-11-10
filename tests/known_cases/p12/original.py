"""P12: A timeout of 0 or a draw clipped at 0. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = (
        new()
    )  # a2raw.py: SimPy timeout(0) goes on at the same time; a_arrivals.py A2: a constant gap 0 and a service 0

    def t(k):
        yield env.timeout(0)
        L(k, "t", {"event_type": "waited", "at": env.now})
        yield env.timeout(0)
        L(k, "u", {"event_type": "served", "at": env.now})

    arrival(env, "a", t, gap=0, max_events=3)
    return go(env, log)

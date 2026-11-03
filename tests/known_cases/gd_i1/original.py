"""GD-I1: A car that parks and drives for ever. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def car(k):
        while True:
            yield env.timeout(5)
            L(k, "parking", {"event_type": "parked"})
            yield env.timeout(2)
            L(k, "driving", {"event_type": "drove"})

    arrival(env, "car", car, initial=1)
    return go(env, log, 15)

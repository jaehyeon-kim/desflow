"""GD-I2: A car that waits for a charging child flow. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def charge(k):
        yield env.timeout(5)
        L(k, "charging", {"event_type": "charged"})

    def car(k):
        while True:
            yield env.process(charge(k))
            L(k, "start_parking", {"event_type": "start_driving"})
            yield env.timeout(2)
            L(k, "driving", {"event_type": "parked_again"})

    arrival(env, "car", car, initial=1)
    return go(env, log, 15)

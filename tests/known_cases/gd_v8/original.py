"""GD-V8: Read the values of an all-of condition in order. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # events.rst, fetch_values_of_multiple_events(): r1, r2 = (yield t1 & t2).values(); the assert of the guide is kept
    env, log, L = new()

    def fetch_values_of_multiple_events(k):
        t1, t2 = env.timeout(1, value="spam"), env.timeout(2, value="eggs")
        r1, r2 = (yield t1 & t2).values()
        assert r1 == "spam" and r2 == "eggs"
        L(k, "both", {"values": [r1, r2]})

    arrival(env, "a", fetch_values_of_multiple_events, initial=1)
    return go(env, log)

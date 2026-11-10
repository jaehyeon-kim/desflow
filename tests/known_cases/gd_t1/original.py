"""GD-T1: Events at one time run in the order they were scheduled. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()

    def f(n, tk):
        def g(k):
            yield env.timeout(n)
            L(k, tk, {"event_type": "finished"})

        return g

    arrival(env, "a", f(1, "finish_at_one"), initial=3)
    arrival(env, "b", f(2, "finish_at_two"), initial=2)
    return go(env, log)

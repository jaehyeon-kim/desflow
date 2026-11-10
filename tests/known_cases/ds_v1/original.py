"""DS-V1: A service time computed for each entity. The plain SimPy program: from the design's SimPy program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # research/yaml-only/programs/V1.py run([("a", 2), ("b", 4)]): two cars at one instant on one pump, each fills for fuel / 2.
    env, log, L = new()
    pump = simpy.Resource(env, 1)

    def car(fuel):
        def f(k):
            with pump.request() as r:
                yield r
                yield env.timeout(fuel / 2)
                L(k, "car", {"event_type": "done", "fuel": fuel})

        return f

    arrival(env, "a", car(2), gap=1, max_events=1, first=0)
    arrival(env, "b", car(4), gap=1, max_events=1, first=0)
    return go(env, log)

"""GD-I4: Four cars share a charging station of two spots. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    bcs = simpy.Resource(env, 2)
    kc = [-1]
    dc = [-2]

    def keyfn():
        kc[0] += 1
        return f"Car {kc[0]}"

    def drive(k):
        dc[0] += 2
        dt = dc[0]
        yield env.timeout(dt)
        L(k, "drive", {"event_type": "arrived"})
        with bcs.request() as r:
            yield r
            yield env.timeout(5)
            L(k, "charge", {"event_type": "charged"})

    arrival(env, "cars", drive, initial=4, keyfn=keyfn)
    return go(env, log)

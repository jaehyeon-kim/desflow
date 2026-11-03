"""P32: Hold several resources. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    m1 = simpy.Resource(env, 1)
    m2 = simpy.Resource(env, 1)

    # register audit2/patterns/mine/g_d1.py P32 (a then b, nested requests; records the moment both are held) with review-r1/patterns/r11_misc2.py values: cap 1 each, service 2, arrivals at 0 and 1
    def job(k):
        L(
            k, "job", S("queued")
        )  # the blueprint publishes a queued record for every request
        with m1.request() as r1:
            yield r1
            with m2.request() as r2:
                yield r2
                L(k, "job", S("started"))
                yield env.timeout(2)
                L(k, "job", {"event_type": "done"})

    arrival(env, "a", job, gap=1, max_events=2, first=0)
    return go(env, log)


program.grants = True

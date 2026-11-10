"""P73: Abort a renege. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # audit2 g_d2.py "P40 at the release instant (I5)": patience 2.5 = service 2.5, one unit, ten entities at 0
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def job(k):
        with m.request() as r:
            got = yield r | env.timeout(2.5)
            if r not in got:
                L(k, "job", S("reneged"))
                return
            yield env.timeout(2.5)
            L(k, "job", {"event_type": "served"})

    arrival(env, "o", job, initial=10)
    return go(env, log)

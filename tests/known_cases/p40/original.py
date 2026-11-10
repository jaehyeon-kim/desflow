"""P40: Renege on a fixed or attribute patience. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # register audit2/patterns/mine/g_d2.py "P40 yaml patience at the release instant (I5)": patience 2.5, service 2.5, entities at one instant
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def job(k):
        with m.request() as r:
            res = yield r | env.timeout(2.5)
            if r not in res:
                L(k, "job", S("reneged"))
                return
            yield env.timeout(2.5)
            L(k, "job", {"event_type": "served"})

    arrival(env, "a", job, initial=3)
    log, err = go(env, log)
    # g_d2.py: patience 2.5 ends at the release instant 2.5, so the one in service is served and every later one reneges (the grant does not win the tie)
    assert sum(1 for x in log if x[3].get("event_type") == "served") == 1 and all(
        x[3].get("status") == "reneged" for x in log[1:]
    ), log
    return log, err

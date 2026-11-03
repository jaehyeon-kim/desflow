"""P34: A capacity computed from the state. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new, set_capacity

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def _grow(cap_of):
    """register r7_res2.py P33 and P34: capacity 1, jobs of 5 at 0 and 1, a flow at 2 changes the capacity (SimPy has no set_capacity: g_d1.py CapRes, _capacity and a wake-up)"""
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def job(k):
        with m.request() as r:
            yield r
            yield env.timeout(5)
            L(k, "job", {"event_type": "done"})

    def grow(k):
        L(k, "grow", {"event_type": "grow"})
        set_capacity(m, cap_of(m))
        yield env.timeout(0)

    arrival(env, "a", job, gap=1, max_events=2, first=0)
    arrival(env, "boss", grow, gap=2, max_events=1)
    log, err = go(env, log)
    # the second job waits from 1 and starts when the capacity grows at 2 (it would start at 5 otherwise), so it is done at 7
    assert [(x[0], x[1]) for x in log if x[2] == "job"] == [(5, "a-1"), (7, "a-2")], log
    return log, err


def program():
    return _grow(lambda m: len(m.queue) + 1)

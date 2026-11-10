"""P43: Renege when a signal fires. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # register audit2/patterns/mine/p43_fix.py: a waiter yields `request | bell event`; when the bell wins it goes to other. Capacity 1, service 2.5, bell at 2.0
    env, log, L = new()
    m = simpy.Resource(env, 1)
    bells = [env.event()]

    def job(k):
        with m.request() as r:
            res = yield r | bells[0]
            if r not in res:
                L(k, "job", S("interrupted", cause=None))
                L(k, "other", {"event_type": "other"})
                return
            yield env.timeout(2.5)
            L(k, "job", {"event_type": "served"})

    def ring(k):
        L(k, "ring", {"event_type": "bell"})
        old = bells[0]
        bells[0] = env.event()
        old.succeed()
        yield env.timeout(0)

    arrival(env, "a", job, initial=3)
    arrival(env, "bell", ring, gap=2, max_events=1)
    return go(env, log)

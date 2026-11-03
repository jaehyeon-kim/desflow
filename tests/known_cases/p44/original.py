"""P44: One deadline across queues. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # register patterns/m_renege.py M1 "simpy system-wide deadline": a and b of capacity 1, three customers at 0, deadline = arrival + 5, a for 2 then b for 4
    env, log, L = new()
    a = simpy.Resource(env, 1)
    b = simpy.Resource(env, 1)

    def c(k):
        deadline = env.now + 5
        with a.request() as r:
            res = yield r | env.timeout(deadline - env.now)
            if r not in res:
                L(k, "first", S("reneged"))
                return
            yield env.timeout(2)
            L(k, "first", {"event_type": "first_done"})
        with b.request() as r:
            res = yield r | env.timeout(max(0, deadline - env.now))
            if r not in res:
                L(k, "second", S("reneged"))
                return
            yield env.timeout(4)
            L(k, "second", {"event_type": "done"})

    arrival(env, "c", c, initial=3)
    return go(env, log)

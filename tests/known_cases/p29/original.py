"""P29: Seize two units of a pool. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    c = simpy.Container(
        env, 3, init=3
    )  # review-r1/patterns/r5_res.py "P29 simpy container": jobs (t0, n) = (0,2), (0.5,2), (0.6,1); get(n), record the grant, timeout(1), put(n)

    def j(i, t0, n):
        k = f"a-{i}"
        yield env.timeout(t0)
        L(k, "job", S("queued"))
        yield c.get(n)
        L(k, "job", {"status": "started"})
        yield env.timeout(1)
        yield c.put(n)
        L(k, "job", {"event_type": "served"})

    for i, (t0, n) in enumerate([(0, 2), (0.5, 2), (0.6, 1)], 1):
        env.process(j(i, t0, n))
    return go(env, log)


program.grants = True

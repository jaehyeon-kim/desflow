"""P08: Replay of a trace of arrival times. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import S, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    m = simpy.Resource(
        env, 1
    )  # audit2/patterns/mine/g_a2.py p08_simpy, rows scen(10)['ties'] = 0,0,2,2,4,4,6,6,8,8: each row waits for its time, takes the cap-1 resource, serves 1

    def job(i, t):
        yield env.timeout(t)
        k = f"o-{i}"
        L(
            k, "k", S("queued")
        )  # the blueprint publishes a queued record for every request
        with m.request() as r:
            yield r
            L(k, "k", {"status": "started"})
            yield env.timeout(1)
            L(k, "k", {"event_type": "served"})

    for i, t in enumerate([2.0 * (j // 2) for j in range(10)], 1):
        env.process(job(i, t))
    return go(env, log)


program.grants = True

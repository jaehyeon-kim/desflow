"""P46: Choose the shortest queue or the first free. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # register audit2/patterns/mine/g_c.py P46 shortest line: two machines of capacity 1, service 3, entities at one instant
    env, log, L = new()
    m1 = simpy.Resource(env, 1)
    m2 = simpy.Resource(env, 1)

    def job(k):
        L(k, "pick", {"event_type": "pick"})
        m = m1 if len(m1.queue) + m1.count <= len(m2.queue) + m2.count else m2
        with m.request() as r:
            yield r
            yield env.timeout(3)
            L(
                k,
                "w1" if m is m1 else "w2",
                {"event_type": "served_1" if m is m1 else "served_2"},
            )

    arrival(env, "a", job, initial=3)
    return go(env, log)

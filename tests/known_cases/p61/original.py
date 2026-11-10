"""P61: Blocking with a finite buffer. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # patterns/g_flow.py G3: four jobs at 0; m1 (1 s) -> buffer of 1 -> m2 (5 s); the put blocks while the job holds m1; the get comes before the m2 request
    env, log, L = new()
    m1 = simpy.Resource(env, 1)
    buf = simpy.Store(env, 1)
    m2 = simpy.Resource(env, 1)

    def job(k):
        with m1.request() as r:
            yield r
            yield env.timeout(1)
            yield buf.put(k)
            L(k, "s1", {})
        yield buf.get()
        L(k, "s2g", {})
        with m2.request() as r:
            yield r
            yield env.timeout(5)
            L(k, "s2", {"done": 1})

    arrival(env, "orders", job, initial=4)
    return go(env, log, 30)

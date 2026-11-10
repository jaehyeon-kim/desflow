"""P06: A piecewise arrival rate. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    st = {
        "gap": 1.3
    }  # audit2/patterns/mine/g_a2.py p06_simpy: one process per step, created before the source; gap 1.3, steps (2.6, 0.5) and (5, 2), until 15

    def step(at, v):
        yield env.timeout(at)
        st["gap"] = v

    def t(k):
        L(k, "t", {"event_type": "arrived"})
        yield env.timeout(0)

    for at, v in ((2.6, 0.5), (5, 2)):
        env.process(step(at, v))
    arrival(env, "o", t, gap=lambda: st["gap"])
    return go(env, log, 15)

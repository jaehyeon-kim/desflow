"""GD-S2: Wait until a condition holds. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    tank = simpy.Container(env, 100, 0)

    def waituntil(env, cond_func, delay=1):
        while not cond_func():
            yield env.timeout(delay)

    def waiter(k):
        yield env.process(waituntil(env, lambda: tank.level >= 30))
        L(k, "waiter", {"event_type": "waituntil_done"})

    def fill(k):
        yield tank.put(10)
        L(k, "fill", {"event_type": "filled"})

    arrival(env, "w", waiter, initial=1)
    arrival(env, "f", fill, gap=1.5, max_events=4)
    return go(env, log)

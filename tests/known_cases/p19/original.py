"""P19: A global counter that is not a plain increment. The plain SimPy program: from the program the gap register ran."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    total = simpy.Container(env, float("inf"), 0)
    lock = simpy.Container(env, 1, 1)

    def add(k):
        yield lock.get(1)
        L(k, "add", {"event_type": "locked"})
        yield total.put(3 * 2)
        L(k, "put_total", {"event_type": "added"})
        nm = "big" if total.level >= 10 else "small"
        yield lock.put(1)
        L(k, nm, {"event_type": nm})

    arrival(env, "a", add, gap=5, max_events=4)
    return go(env, log)

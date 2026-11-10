"""GD-S4: Return the first needle found. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    s = simpy.Store(env)
    s.items += [{"needle": 0, "n": 1}, {"needle": 0, "n": 2}, {"needle": 1, "n": 3}]

    def is_needle(item):
        return item["needle"] == 1

    def find_first_needle(k):
        while True:
            item = yield s.get()
            L(k, "find", {"event_type": "looked", "item": item})
            if is_needle(item):
                return item

    def proc(k):
        needle = yield env.process(find_first_needle(k))
        L(k, "found", {"event_type": "found", "needle": needle})

    arrival(env, "a", proc, initial=1)
    return go(env, log)

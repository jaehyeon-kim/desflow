"""P67: Stop when a condition holds. The plain SimPy program: from the program the gap register ran."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    # tests/conformance/test_c6_end_conditions.py _reference_condition: restock +2 every 3, a ping every 4, stop when the stock holds 5 (at 9)
    stock = simpy.Container(env, capacity=20)
    full = env.event()

    def restock(k):
        yield stock.put(2)
        L(k, "restock", {"event": "restocked"})
        if stock.level >= 5 and not full.triggered:
            full.succeed()

    def ping(k):
        L(k, "ping", {"event": "ping"})
        yield env.timeout(0)

    arrival(env, "restocks", restock, gap=3)
    arrival(env, "pings", ping, gap=4)
    return go(env, log, full)

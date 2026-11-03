"""P59: Split into a drawn number of parts. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # patterns/g_flow.py G2: one order at 0 with n=3 splits into n new entities (spawn count from an attribute); until 10
    env, log, L = new()
    n = [0]

    def item(k):
        L(k, "item", {"i": 1})
        yield env.timeout(0)

    def split(k):
        L(k, "split", {"e": 1})
        for _ in range(3):
            n[0] += 1
            env.process(item(f"item-{n[0]}"))
        yield env.timeout(0)

    arrival(env, "orders", split, initial=1)
    return go(env, log, 10)

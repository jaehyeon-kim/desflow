"""P60: Children inherit attributes. The plain SimPy program: from the program the gap register ran."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # patterns/g_flow.py G2b (three spawns from one order, n=3) with the children keeping a copy of the parent's attributes (yaml-only/programs/A7.py dict(attrs))
    env, log, L = new()
    n = [0]

    def item(k, a):
        L(k, "item", {"i": 1, "n": a["n"]})
        yield env.timeout(0)

    def split(k):
        attrs = {"n": 3}
        L(k, "split", {"e": 1})
        for _ in range(3):
            n[0] += 1
            env.process(item(f"item-{n[0]}", dict(attrs)))
        yield env.timeout(0)

    arrival(env, "orders", split, initial=1)
    return go(env, log, 10)

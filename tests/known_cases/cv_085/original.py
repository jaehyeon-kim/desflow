"""CV-085: Count the items of a store and test membership. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    q = simpy.Store(env)
    q.items += ["a", "b"]

    def a(k):
        L(k, "look", {"event_type": "look", "n": len(q.items), "has_a": "a" in q.items})
        yield env.timeout(0)

    arrival(env, "a", a, initial=1)
    log, err = go(env, log)
    assert [(p["n"], p["has_a"]) for _, _, _, p in log] == [
        (2, True)
    ]  # stated: count 2 and membership of 'a'
    return log, err

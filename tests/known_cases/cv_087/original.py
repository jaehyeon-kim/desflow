"""CV-087: Remove an item by value. The plain SimPy program: from the design's SimPy program."""

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
        q.items.remove("b")
        L(k, "take", {"event_type": "removed"})
        yield env.timeout(0)

    arrival(env, "a", a, initial=1)
    log, err = go(env, log)
    assert list(q.items) == ["a"] and [r[2] for r in log] == [
        "take"
    ]  # stated: remove('b') leaves only 'a' in the store
    return log, err

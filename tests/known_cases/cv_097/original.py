"""CV-097: Read the value of a get. The plain SimPy program: from the design's SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    s = simpy.Store(env)
    s.items += [{"v": 7}]

    def a(k):
        g = s.get()
        v = yield g
        L(k, "take", {"event_type": "got", "v": v["v"]})

    arrival(env, "a", a, initial=1)
    log, err = go(env, log)
    assert [p["v"] for _, _, _, p in log] == [7] and len(
        s.items
    ) == 0  # stated: the yielded value of a Store get is the stored item
    return log, err

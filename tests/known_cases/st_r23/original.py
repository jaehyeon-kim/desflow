"""ST-R23: A priority store served by item value. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new, start

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_priority_store_item_priority: one process puts 'bcadefg'; three getters wait 1, 2, 3
    env, log, L = new()
    pstore = simpy.PriorityStore(env, 3)
    olog = []

    def putter():
        for n, s in enumerate("bcadefg"):
            yield pstore.put(s)
            L(f"puts-{n + 1}", "put_it", {"event_type": "put"})

    def getter(wait, k):
        yield env.timeout(wait)
        item = yield pstore.get()
        olog.append(item)
        L(k, "get_it", {"event_type": "got", "item": item})

    start(env, lambda: env.process(putter()))
    for w in (1, 2, 3):
        start(env, lambda w=w: env.process(getter(w, f"getters-{w}")))
    r = go(env, log)
    assert olog == ["a", "b", "c"]
    return r

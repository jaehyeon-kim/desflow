"""ST-R24: Items of equal priority come out in insertion order. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new, start

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_priority_store_stable_order: three unorderable items with priority 0 come out in insertion order
    env, log, L = new()
    pstore = simpy.PriorityStore(env, 3)
    olog = []
    items = [object() for _ in range(3)]

    def putter():
        for n, item in enumerate(items):
            yield pstore.put(simpy.PriorityItem(0, item))
            L(f"puts-{n + 1}", "put_it", {"event_type": "put"})

    def getter(wait, k):
        yield env.timeout(wait)
        _, item = yield pstore.get()
        olog.append(item)
        L(k, "get_it", {"event_type": "got", "item": items.index(item)})

    start(env, lambda: env.process(putter()))
    for w in (1, 2, 3):
        start(env, lambda w=w: env.process(getter(w, f"getters-{w}")))
    r = go(env, log)
    assert olog == items
    return r

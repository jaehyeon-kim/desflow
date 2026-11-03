"""ST-R19: A putter and a getter on a store of two. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_store
    env, log, L = new()
    store = simpy.Store(env, capacity=2)
    item = {"id": 1}

    def putter(k, store, item):
        yield store.put(item)
        L(k, "putter", {"event_type": "put"})

    def getter(k, store, orig_item):
        item = yield store.get()
        assert item is orig_item
        L(k, "getter", {"event_type": "got", "same": item is orig_item})

    arrival(env, "p", lambda k: putter(k, store, item), initial=1)
    arrival(env, "g", lambda k: getter(k, store, item), initial=1)
    return go(env, log)

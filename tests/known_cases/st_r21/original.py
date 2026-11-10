"""ST-R21: A third put waits on a full store. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_store_capacity: one process yields three puts into a store of capacity 2
    env, log, L = new()
    capacity = 2
    for bad in (0, -1):
        try:
            simpy.Store(env, bad)
            raise AssertionError("DID NOT RAISE")
        except ValueError as e:
            assert '"capacity" must be > 0' in str(e)
    store = simpy.Store(env, capacity)

    def puts():
        for i in range(capacity + 1):
            yield store.put(i)
            L(f"puts-{i + 1}", "put_it", {"event_type": "put"})

    env.process(puts())
    r = go(env, log)
    assert len(store.items) == capacity
    return r

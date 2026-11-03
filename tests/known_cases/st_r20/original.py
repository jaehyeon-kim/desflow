"""ST-R20: A store with no capacity is unbounded. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_initial_store_capacity (Store, FilterStore)
    env, log, L = new()
    a = simpy.Store(env)
    b = simpy.FilterStore(env)

    def read(k):
        assert a.capacity == float("inf") and b.capacity == float("inf")
        L(
            k,
            "read",
            {
                "event_type": "capacity",
                "plain": None if a.capacity == float("inf") else a.capacity,
                "filtered": None if b.capacity == float("inf") else b.capacity,
            },
        )
        yield env.timeout(0)

    arrival(env, "a", read, initial=1)
    return go(env, log)

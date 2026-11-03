"""GD-RS8: A producer and two consumers on a store of two. The plain SimPy program: from the SimPy program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # resources.rst, the producer / consumer example on a Store(capacity=2); env.run(until=5)
    env, log, L = new()

    def producer(k, env, store):
        for i in range(100):
            yield env.timeout(2)
            yield store.put(f"spam {i}")
            L(k, "produce", {"event_type": "produced"})

    def consumer(k, env, store):
        while True:
            yield env.timeout(1)
            L(k, "consume", {"event_type": "requesting"})
            item = yield store.get()
            L(k, "take", {"event_type": "got", "item": item})

    store = simpy.Store(env, capacity=2)
    arrival(env, "producer", lambda k: producer(k, env, store), initial=1)
    arrival(env, "consumers", lambda k: consumer(k, env, store), initial=2)
    return go(env, log, 5)

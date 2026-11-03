"""ST-R22: A get inside a with block is cancelled on leaving. The plain SimPy program: from the SimPy test's program."""

import simpy
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    env, log, L = new()
    # test_store_cancel
    store = simpy.Store(env, capacity=1)

    def acquire_implicit_cancel(k):
        with store.get():
            yield env.timeout(1)
            L(k, "acquire", {"event_type": "waited"})
            # implicit cancel() when exiting with-block
        L(k, "left", {"event_type": "left_block", "getters": len(store.get_queue)})

    arrival(env, "a", acquire_implicit_cancel, initial=1)
    r = go(env, log)
    assert len(store.get_queue) == 0
    return r

"""ST-R25: A filter get waits for a matching item. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_filter_store: one process asks for 'b' without waiting, then puts 'a' and 'b'
    env, log, L = new()

    def pem(env):
        store = simpy.FilterStore(env, capacity=2)
        get_event = store.get(lambda item: item == "b")
        get_event.callbacks.append(
            lambda e: L("g-1", "want_b", {"event_type": "got_b"})
        )  # the record of the get
        yield store.put("a")
        L("p-1", "put_a", {"event_type": "put_a"})
        assert not get_event.triggered
        yield store.put("b")
        L("p-1", "put_b", {"event_type": "put_b"})
        assert get_event.triggered

    env.process(pem(env))
    return go(env, log)

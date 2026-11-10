"""ST-R25: A filter get waits for a matching item.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def pem(env):
        store = simpy.FilterStore(env, capacity=2)
        get_event = store.get(lambda item: item == "b")
        get_event.callbacks.append(
            lambda e: L("g-1", "want_b", {"event_type": "got_b"})
        )
        yield store.put("a")
        L("p-1", "put_a", {"event_type": "put_a"})
        yield store.put("b")
        L("p-1", "put_b", {"event_type": "put_b"})

    env.process(pem(env))

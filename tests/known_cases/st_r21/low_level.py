"""ST-R21: A third put waits on a full store.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    capacity = 2
    for bad in (0, -1):
        try:
            simpy.Store(env, bad)
            raise AssertionError("DID NOT RAISE")
        except ValueError:
            pass
    store = simpy.Store(env, capacity)

    def puts():
        for i in range(capacity + 1):
            yield store.put(i)
            L(f"puts-{i + 1}", "put_it", {"event_type": "put"})

    env.process(puts())

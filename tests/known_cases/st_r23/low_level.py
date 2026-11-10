"""ST-R23: A priority store served by item value.

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

    env.process(putter())
    for w in (1, 2, 3):
        (lambda w=w: env.process(getter(w, f"getters-{w}")))()

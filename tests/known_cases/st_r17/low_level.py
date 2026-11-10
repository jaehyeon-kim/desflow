"""ST-R17: Read the queues of a container of capacity 1.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from typing import Any
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def arrival(
    env, name, make, initial=0, gap=None, max_events=None, first=None, keyfn=None
):
    """Starts `initial` entities now, in order, then one every `gap`, the first
    `first` after the start. `make(key)` returns the process of the entity `key`."""
    count = [0]

    def key():
        count[0] += 1
        return f"{name}-{count[0]}" if keyfn is None else keyfn()

    def begin(label):
        PROCS[label] = env.process(make(label))

    for _ in range(initial):
        begin(key())
    if gap is not None:

        def source():
            made = 0
            if first is not None and first > 0:
                yield env.timeout(first)
            while max_events is None or made < max_events:
                if not (first is not None and made == 0):
                    yield env.timeout(gap() if callable(gap) else gap)
                made += 1
                begin(key())

        env.process(source())


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    container = simpy.Container(env, 1)
    who = {}

    def proc(k, wait, what, task):
        who[env.active_process] = k
        yield env.timeout(wait)
        with getattr(container, what)(1) as req:
            yield req
            L(k, task, {"event_type": "got" if what == "get" else "put"})

    arrival(env, "g", lambda k: proc(k, 0, "get", "get_it"), initial=size)
    arrival(env, "p", lambda k: proc(k, 1, "put", "put_it"), initial=3)

    def look(k):
        L(
            k,
            "look",
            {
                "event_type": "look",
                "put_queue": [who[e.proc] for e in container.put_queue],
                "get_queue": [who[e.proc] for e in container.get_queue],
            },
        )

    def drive():
        env.run(until=1)
        list(PROCS.values())
        PROCS["g-1"]
        look("look-1")
        env.run(until=2)
        look("look-2")

    return drive

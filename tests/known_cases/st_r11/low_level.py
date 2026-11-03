"""ST-R11: Read the users and queue of a resource.

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


def build(env: DynamicRealtimeEnvironment, size: int = 3) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    res = simpy.Resource(env, 1)
    who = {}

    def process(k):
        who[env.active_process] = k
        with res.request() as req:
            yield req
            yield env.timeout(1)
            L(k, "pem", {"event_type": "used"})

    arrival(env, "users", process, initial=size)

    def look(k):
        L(
            k,
            "look",
            {
                "event_type": "look",
                "users": [who[e.proc] for e in res.users],
                "queue": [who[e.proc] for e in res.queue],
            },
        )

    def drive():
        env.run(until=1)
        list(PROCS.values())
        look("look-1")
        env.run(until=2)
        look("look-2")

    return drive

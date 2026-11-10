"""GD-E1: Run the model in segments to a series of times.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from typing import Any

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


def _ticking_process(env, L):
    # environments.rst has no model; one process that ticks every 1 gives the loops something to run
    def ticking(k):
        while True:
            yield env.timeout(1)
            L(k, "ticking", {"event_type": "tick"})

    return ticking


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    arrival(env, "a", _ticking_process(env, L), initial=size)

    def drive():
        for i in range(1, 100):
            env.run(until=i)

    return drive

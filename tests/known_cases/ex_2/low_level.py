"""EX-2: Carwash where a car keeps its machine during the wash.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 20


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


def build(env: DynamicRealtimeEnvironment, size: int = 4) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    m = simpy.Resource(env, 2)
    seq = [0]

    def keyfn():
        k = f"Car {seq[0]}"
        seq[0] += 1
        return k

    def wash(k):
        yield env.timeout(5)
        L(k, "wash", {"event_type": "washed", "dirt_removed_pct": 70})

    def car(k):
        with m.request() as request:
            yield request
            L(k, "car", {"event_type": "enters"})
            yield env.process(wash(k))
            L(k, "washing", {"event_type": "leaves"})

    arrival(env, "cars", car, initial=size, gap=lambda: 7, keyfn=keyfn)

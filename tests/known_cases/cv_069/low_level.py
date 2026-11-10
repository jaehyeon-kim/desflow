"""CV-069: Release a unit and wait for the release.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

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


def build(env: DynamicRealtimeEnvironment, size: int = 2) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    res = simpy.Resource(env, 2)

    def take(k):
        q = res.request()
        yield q
        yield env.timeout(1)
        L(k, "take", {"event_type": "held"})
        yield res.release(q)
        L(k, "zero", {"event_type": "after_release"})

    def watch(k):
        yield env.timeout(1)
        L(k, "watch", {"event_type": "o"})
        yield env.timeout(0)
        L(k, "again", {"event_type": "o_again"})

    arrival(env, "h", take, initial=size)
    arrival(env, "o", watch, initial=2)

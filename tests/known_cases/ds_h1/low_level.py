"""DS-H1: A unit held across several steps.

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


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    m = simpy.PreemptiveResource(env, 1)
    x, d = (2, 3)

    def job(k):
        with m.request(priority=5) as r:
            yield r
            L(k, "take", {"event_type": "got"})
            yield env.timeout(x)
            L(k, "a", {"event_type": "a"})
            yield env.timeout(d)
            L(k, "b", {"event_type": "b"})

    def other(k):
        with m.request(priority=9, preempt=False) as r:
            yield r
            L(k, "waiter", {"event_type": "waiter_granted"})

    arrival(env, "job", job, initial=size)
    arrival(env, "other", other, initial=1)

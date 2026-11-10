"""P46: Choose the shortest queue or the first free.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
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


def build(env: DynamicRealtimeEnvironment, size: int = 3) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    m1 = simpy.Resource(env, 1)
    m2 = simpy.Resource(env, 1)

    def job(k):
        L(k, "pick", {"event_type": "pick"})
        m = m1 if len(m1.queue) + m1.count <= len(m2.queue) + m2.count else m2
        with m.request() as r:
            yield r
            yield env.timeout(3)
            L(
                k,
                "w1" if m is m1 else "w2",
                {"event_type": "served_1" if m is m1 else "served_2"},
            )

    arrival(env, "a", job, initial=size)

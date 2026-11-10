"""P74: Handle an unfinished request.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = None


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


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

    m = simpy.Resource(env, 1)

    def holder(k):
        with m.request() as r:
            yield r
            yield env.timeout(5)
            L(k, "hold", {"event_type": "held"})

    def late(k):
        with m.request() as req:
            res = yield (req | env.timeout(4))
            if req in res:
                L(k, "job", {"event_type": "served"})
            else:
                L(k, "job", S("reneged"))
                L(k, "elsewhere", {"event_type": "elsewhere"})

    arrival(env, "h", holder, initial=size)
    arrival(env, "late", late, gap=1, max_events=1)

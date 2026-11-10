"""DS-I6: Go on after reneging or balking.

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def pair(n, patience, limit):
        m = simpy.Resource(env, 1)

        def hold(k):
            with m.request() as r:
                yield r
                yield env.timeout(5)

        def elsewhere(k, why):
            L(k, "elsewhere", {"event_type": "elsewhere", "why": why})
            yield env.timeout(0)

        def late(k):
            if limit is not None and len(m.queue) >= limit:
                L(k, "late%d" % n, S("balked"))
                yield env.process(elsewhere(k, "balked"))
                return
            with m.request() as req:
                res = yield (req | env.timeout(patience))
                if req in res:
                    L(k, "late%d" % n, {"event_type": "served"})
                else:
                    L(k, "late%d" % n, S("reneged"))
                    yield env.process(elsewhere(k, "reneged"))

        arrival(env, "h%d" % n, hold, gap=1, max_events=1, first=0)
        arrival(env, "l%d" % n, late, gap=1, max_events=1, first=1)

    pair(1, 2, None)
    pair(2, 4, None)
    pair(3, 2, 0)

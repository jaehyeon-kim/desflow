"""DS-Y5: Lists and mappings in expressions.

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    seen = {}
    q = []
    s = simpy.Store(env)
    s.put({"v": 1})
    s.put({"v": 2})

    def cnt(k0, d):

        def p(k):
            kk = k0
            seen[kk] = seen.get(kk, 0) + 1
            L(
                k,
                "cnt_log",
                {
                    "event_type": "cnt",
                    "k": kk,
                    "seen": dict(seen),
                    "has_k": kk in seen,
                    "has_z": "z" in seen,
                },
            )
            yield env.timeout(0)

        return p

    def lst(i):

        def p(k):
            q.append(i * 10)
            L(
                k,
                "lst_log",
                {
                    "event_type": "lst",
                    "q": list(q),
                    "first": q[0],
                    "last": q[-1],
                    "n": len(q),
                    "total": sum(q),
                    "ordered": sorted(q, reverse=False),
                    "plus_one": [x + 1 for x in q if x > 0],
                    "rest": q[1:],
                },
            )
            yield env.timeout(0)

        return p

    def peek_store(k):
        L(
            k,
            "peek_store",
            {
                "event_type": "peek_store",
                "first_v": s.items[0]["v"],
                "n": len(s.items),
                "values": [x["v"] for x in s.items],
            },
        )
        yield env.timeout(0)

    for name, k0, d in [("c1", "a", 1), ("c2", "b", 1), ("c3", "a", 2)]:
        arrival(env, name, cnt(k0, d), gap=1, max_events=1, first=d)
    for i in range(3):
        arrival(env, "l%d" % i, lst(i), gap=1, max_events=1, first=i)
    arrival(env, "s", peek_store, gap=1, max_events=1, first=0)

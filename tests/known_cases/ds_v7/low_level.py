"""DS-V7: Names for a cause, holders, an item priority and nested values.

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

    m = simpy.PreemptiveResource(env, 1)
    m2 = simpy.Resource(env, 2)
    st = simpy.PriorityStore(env)
    names = {}

    def low(k):
        names[env.active_process] = k
        with m.request(priority=5) as r:
            yield r
            began = env.now
            try:
                yield env.timeout(10)
            except simpy.Interrupt as i:
                c = i.cause
                L(
                    k,
                    "low",
                    S(
                        "preempted",
                        resource="m",
                        preempted_by=names[c.by],
                        usage_since=c.usage_since,
                    ),
                )
                L(
                    k,
                    "low_cause",
                    {
                        "event_type": "cause",
                        "by": names[c.by],
                        "usage_since": c.usage_since,
                        "left": 10 - (env.now - began),
                        "state": "interrupted",
                        "now": env.now,
                    },
                )

    def high(k):
        names[env.active_process] = k
        yield env.timeout(0)
        with m.request(priority=1) as r:
            yield r
            L(
                k,
                "high_seize",
                {"event_type": "high_granted", "holders": len(m.users), "at": env.now},
            )

    def hold2(k):
        names[env.active_process] = k
        with m2.request() as r:
            yield r
            L(
                k,
                "hold2",
                {"event_type": "users", "who": [names[u.proc] for u in m2.users]},
            )
            yield env.timeout(5)

    def put1(k):
        yield st.put(simpy.PriorityItem(2, {"req": {"id": 7}}))
        L(k, "put1", {"req": {"id": 7}})
        yield st.put(simpy.PriorityItem(1, {"req": {"id": 8}}))
        L(k, "put2", {"req": {"id": 8}})
        for t in ("get1", "get2"):
            it = yield st.get()
            L(
                k,
                t,
                {"event_type": "got", "prio": it.priority, "id": it.item["req"]["id"]},
            )

    def end(k):
        L(k, "end", {"event_type": "holders_at_the_end", "holders": len(m.users)})
        yield env.timeout(0)

    for n, f, at in (
        ("low", low, 0),
        ("high", high, 0),
        ("a", hold2, 0),
        ("b", hold2, 0),
        ("pr", put1, 0),
        ("fin", end, 20),
    ):
        arrival(env, n, f, gap=1, max_events=1, first=at)

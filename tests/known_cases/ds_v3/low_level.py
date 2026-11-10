"""DS-V3: Computed values in a write, a cause and a timeout value.

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

    state = {"n": 1, "reason": "r1", "total": 10}
    built = simpy.Store(env)

    def writes(k):
        state["n"] += 1
        state["total"] -= 3
        built.items.extend((state["n"] * i for i in range(3)))
        L(
            k,
            "writes_report",
            {
                "event_type": "writes",
                "n": state["n"],
                "total": state["total"],
                "items": list(built.items),
            },
        )
        yield env.timeout(0)

    def worker(k):
        try:
            yield env.timeout(9)
        except simpy.Interrupt as i:
            L(k, "worker", S("interrupted", cause=i.cause))
            L(k, "worker_cause", {"event_type": "cause", "cause": i.cause})

    def cutter(k):
        yield env.timeout(1)
        cause = state["reason"]
        state["reason"] = "changed in the same step"
        PROCS["wk-1"].interrupt(cause)
        L(k, "cutter", {"event_type": "cutter_goes_on_first"})

    def timed(k):
        made = env.now
        v = yield env.timeout(2, value=env.now * 2)
        L(
            k,
            "timed1",
            {"event_type": "value_fixed", "v": v, "made": made, "now": env.now},
        )
        made = env.now
        v = yield env.timeout(2, value=env.now * 2)
        L(
            k,
            "timed2",
            {"event_type": "value_fixed", "v": v, "made": made, "now": env.now},
        )

    def child(k, wrap):
        yield env.timeout(1)
        L(k, "child", {"v": 10})
        return {"v": 10} if wrap else 10

    def parent(k):
        plain = yield env.process(child(k, False))
        field = yield env.process(child(k, True))
        L(
            k,
            "parent2",
            {
                "event_type": "returned",
                "plain": plain,
                "field": field["v"],
                "same": plain == field["v"],
            },
        )

    for n, f in (
        ("wr", writes),
        ("wk", worker),
        ("ct", cutter),
        ("tm", timed),
        ("rt", parent),
    ):
        arrival(env, n, f, gap=1, max_events=1, first=0)

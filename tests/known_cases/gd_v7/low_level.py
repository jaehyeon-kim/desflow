"""GD-V7: Or, and, nested conditions and timeout values.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

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

    def test_condition(k):
        t1, t2 = (env.timeout(1, value="spam"), env.timeout(2, value="eggs"))
        ret = yield (t1 | t2)
        L(
            k,
            "any_stage",
            {
                "event_type": "any_done",
                "first": ret[t1],
                "second_missing": t2 not in ret,
            },
        )
        t1, t2 = (env.timeout(1, value="spam"), env.timeout(2, value="eggs"))
        ret = yield (t1 & t2)
        L(k, "all_stage", {"event_type": "all_done", "values": [ret[t1], ret[t2]]})
        e1, e2, e3 = [env.timeout(i) for i in range(3)]
        yield ((e1 | e2) & e3)
        L(
            k,
            "nest_stage",
            {
                "event_type": "nest_done",
                "all_processed": all((e.processed for e in [e1, e2, e3])),
            },
        )

    arrival(env, "a", test_condition, initial=size)

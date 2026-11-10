"""ST-U04: A subscriber interrupted while waiting for another child.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

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

    import simpy.util

    def child(env, i, k, task):
        yield env.timeout(i)
        L(k, task, {"event_type": "c%d" % i})

    def parent(k):
        child_proc1 = env.process(child(env, 1, k, "child1"))
        child_proc2 = env.process(child(env, 2, k, "child2"))
        try:
            simpy.util.subscribe_at(child_proc1)
            yield child_proc2
            L(k, "parent", {"event_type": "joined"})
        except simpy.Interrupt:
            L(
                k,
                "parent",
                S(
                    "interrupted",
                    cause={"event": "child1", "value": {"event_type": "c1"}},
                ),
            )
            L(k, "told", {"event_type": "interrupted", "at": env.now})

    arrival(env, "p", parent, initial=size)

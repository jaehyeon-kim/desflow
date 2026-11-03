"""ST-U02: A subscriber interrupted when another process ends.

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

    def child(env, k):
        yield env.timeout(3)
        L(k, "other", {"event_type": "other_done", "value": "ohai"})
        return "ohai"

    def parent(k):
        child_proc = env.process(child(env, k))
        simpy.util.subscribe_at(child_proc)
        try:
            yield env.event()
        except simpy.Interrupt as interrupt:
            L(
                k,
                "subscriber",
                S(
                    "interrupted",
                    cause={
                        "event": "other",
                        "value": {
                            "event_type": "other_done",
                            "value": interrupt.cause[1],
                        },
                    },
                ),
            )
            L(
                k,
                "told",
                {
                    "event_type": "interrupted",
                    "at": env.now,
                    "value": interrupt.cause[1],
                },
            )

    arrival(env, "s", parent, initial=size)

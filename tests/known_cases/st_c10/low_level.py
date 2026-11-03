"""ST-C10: Build flat conditions with in-place and.

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

    def stage(k, task, first, then):
        t1 = env.timeout(1, value=1)
        t2 = env.timeout(2, value=2)
        cond = t1 & t2 if first == "&" else t1 | t2
        t0 = env.timeout(0, value=0)
        if then == "&":
            cond &= t0
        else:
            cond |= t0
        results = yield cond
        L(
            k,
            task,
            {
                "event_type": "done",
                "at": env.now,
                "values": list(results.values()),
                "in_result": [t in results for t in (t1, t2, t0)],
            },
        )

    def pem(k):
        yield from stage(k, "iand_and", "&", "&")
        yield from stage(k, "iand_or", "|", "&")
        yield from stage(k, "ior_or", "|", "|")
        yield from stage(k, "ior_and", "&", "|")

    arrival(env, "a", pem, initial=size)

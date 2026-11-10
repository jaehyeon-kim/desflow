"""ST-C06: A failing event inside a nested condition reaches the waiter.

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

    def explode(k, stage, delay, text):
        yield env.timeout(delay)
        L(k, f"explode_{stage}", S("failed", reason=text))
        raise ValueError(text)

    def parent(k):
        for stage, (delay, text, how) in enumerate(
            [
                (1, "Onoes!", "all"),
                (0, "Onoes, failed after 0!", "any"),
                (0, "Onoes, failed after 0!", "nested"),
            ],
            1,
        ):
            child = env.process(explode(k, stage, delay, text))
            if how == "all":
                condition = child & env.timeout(1)
            elif how == "any":
                condition = child | env.timeout(1)
            else:
                condition = child & env.timeout(1) | env.timeout(1)
            try:
                yield condition
                L(k, f"stage_{stage}", {"event_type": "unreachable"})
            except ValueError as error:
                L(k, f"stage_{stage}", S("failed", reason=str(error)))
                L(k, f"caught_{stage}", {"event_type": "caught", "reason": str(error)})

    arrival(env, "a", parent, initial=size)

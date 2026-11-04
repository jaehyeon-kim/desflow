"""DS-A7: A spawn count and copied attributes.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment

PROCS: dict = {}  # entity key -> its process
UNTIL = 50


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


def _ckey(k, n):
    return f"clone-{n}"


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    attrs = {"n": 3, "tag": "x"}
    cnt = [0]

    def child(k, a):
        L(k, "clone", {"event_type": "clone", "tag": a["tag"]})
        yield env.timeout(0)

    def parent(k):
        for i in range(attrs["n"]):
            cnt[0] += 1
            env.process(child(_ckey(k, cnt[0]), dict(attrs)))
        L(k, "parent", {"event_type": "parent"})
        yield env.timeout(1)

    arrival(env, "a", parent, gap=3, max_events=2, first=0)

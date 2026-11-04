"""CV-117: A nested generator called recursively.

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

    def f(key, k):
        L(key, "f", {"event_type": "call", "k": k})
        if k == 0:
            L(key, "base", {"event_type": "base", "ret": 0})
            return 0
        yield env.timeout(1)
        L(key, "step", {"event_type": "tick", "k": k})
        v = yield env.process(f(key, k - 1))
        L(key, "down", {"event_type": "level", "k": k, "v": v, "ret": v + k})
        return v + k

    def g(key):
        total = yield env.process(f(key, 2))
        L(key, "g", {"event_type": "total", "total": total})

    arrival(env, "a", g, initial=size)

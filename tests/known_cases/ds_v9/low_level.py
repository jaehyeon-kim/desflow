"""DS-V9: Arguments for a child flow.

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def child(k, name, x):
        L(k, "child", {"event_type": "starts", "x": x})
        yield env.timeout(1)
        L(k, "child_end", {"event_type": "ended", "v": x + 1})
        return x + 1

    def parent(k):
        kk = 2
        a = env.process(child(k, "a", kk * 3))
        kk = 100
        b = env.process(child(k, "b", kk * 3))
        L(k, "start_b", {"event_type": "parent_goes_on_first"})
        va = yield a
        vb = yield b
        L(k, "wait_b", {"event_type": "results", "va": va, "vb": vb})

    arrival(env, "a", parent, gap=4, max_events=1, first=0)

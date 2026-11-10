"""P57: Clone with its own attributes.

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

    cnt = {"c1": 0, "c2": 0}

    def child(name, d, x):

        def c(k):
            yield env.timeout(d)
            L(k, name, {})
            L(k, name + "b", {"x": x})

        return c

    def spawn(name, d, x):
        cnt[name] += 1
        return env.process(child(name, d, x)(f"{name}-{cnt[name]}"))

    def split(k):
        L(k, "split", {})
        p1 = spawn("c1", 1, 1)
        L(k, "split2", {})
        p2 = spawn("c2", 2, 2)
        yield env.all_of([p1, p2])
        L(k, "join", {})

    arrival(env, "a", split, initial=size)

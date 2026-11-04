"""CV-138: Random integers, choices, samples and shuffles.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler

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

    DRAWS_R = {
        p: Sampler(seed=42).generator(p)
        for p in [
            "tasks.draw.payload.n",
            "tasks.draw.payload.c",
            "tasks.draw.payload.s",
        ]
    }

    def draw(k):
        n = int(DRAWS_R["tasks.draw.payload.n"].integers(1, 7))
        c = ["a", "b", "c"][int(DRAWS_R["tasks.draw.payload.c"].integers(3))]
        s = [
            ["a", "b", "c", "d"][i]
            for i in DRAWS_R["tasks.draw.payload.s"].choice(4, size=2, replace=False)
        ]
        L(k, "draw", {"event_type": "draw", "n": n, "c": c, "s": s})
        yield env.timeout(0)

    arrival(env, "a", draw, initial=size)

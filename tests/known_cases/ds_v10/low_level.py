"""DS-V10: Routing weights from an expression.

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    DRAWS_R = {p: Sampler(seed=42).generator(p) for p in ["route.pick"]}
    import random

    class R(random.Random):
        def random(self):
            return float(DRAWS_R["route.pick"].random())

    r = R()

    def pick(weights):

        def f(k):
            L(k, "pick", {"event_type": "pick"})
            n = r.choices(["fast", "slow"], weights=weights)[0]
            L(k, n, {"event_type": n})
            yield env.timeout(0)

        return f

    arrival(env, "a", pick([1, 0]), gap=1, max_events=4, first=0)
    arrival(env, "b", pick([0.5, 0.5]), gap=1, max_events=4, first=4)

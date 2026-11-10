"""P69: Leave with a probability.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Router, Sampler, Upstream

PROCS: dict = {}  # entity key -> its process
UNTIL = 5


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
    router = Router(
        {"weights": {"gone": 1, "stay": 1}},
        rng=Sampler(seed=42).generator("route.t"),
    )

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def t(k):
        L(k, "t", {"e": 1})
        n = router.choose(Upstream(task_id=0, key=k, attributes={}))
        L(k, n, {"left": 1} if n == "gone" else {"stay": 1})
        yield env.timeout(0)

    arrival(env, "orders", t, gap=1)

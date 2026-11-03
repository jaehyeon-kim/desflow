"""P22: A branch by fixed weights.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Router, Sampler, Upstream

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
    router = Router(
        {"weights": {"a": 3, "b": 1}}, rng=Sampler(seed=42).generator("route.route")
    )

    def route(k):
        env.publish_event(k, {"event_type": "route"}, task="route")
        chosen = router.choose(Upstream(task_id=0, key=k, attributes={}))
        env.publish_event(k, {"event_type": chosen}, task=chosen)
        yield env.timeout(0)

    arrival(env, "a", route, gap=1, max_events=8)

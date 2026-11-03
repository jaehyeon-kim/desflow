"""P20: Branch by an attribute with fixed weights.

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
    sampler = Sampler(seed=42)
    vips = sampler.generator("arrivals.a.attributes.vip")
    weights = Router(
        {"weights": {"a": 3, "b": 1}}, rng=sampler.generator("route.weighted")
    )

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def route(k):
        vip = bool(vips.random() < 0.3)
        L(k, "route", {"event_type": "route"})
        if vip:
            L(k, "fast", {"event_type": "fast"})
        else:
            L(k, "weighted", {"event_type": "weighted"})
            chosen = weights.choose(Upstream(task_id=0, key=k, attributes={}))
            L(k, chosen, {"event_type": chosen})
        yield env.timeout(0)

    arrival(env, "a", route, gap=1, max_events=6)

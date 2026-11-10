"""CV-128: A resource picked by a computed name.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler
import simpy
import simpy.rt

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

    DRAWS_R = {p: Sampler(seed=42).generator(p) for p in ["arrivals.a.attributes.side"]}
    res = {"left": simpy.Resource(env, 1), "right": simpy.Resource(env, 1)}

    def a(k):
        side = ["left", "right"][int(DRAWS_R["arrivals.a.attributes.side"].integers(2))]
        L(k, "pick", {"event_type": "pick"})
        with res[side].request() as r:
            yield r
            yield env.timeout(2)
            L(k, "at_" + side, {"event_type": "served_" + side})

    arrival(env, "a", a, gap=1, max_events=4)

"""DS-V4: A random value drawn for each entity and used as a priority.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler
import simpy

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

    m = simpy.PriorityResource(env, 1)
    draws = {
        name: Sampler(seed=42).generator(f"arrivals.{name}.attributes.x")
        for name in ("j0", "j1", "j2", "j3")
    }

    def job(k):
        x = float(draws[k.split("-")[0]].exponential(1 / 2))
        with m.request(priority=x) as r:
            yield r
            yield env.timeout(1)
            L(k, "job", {"event_type": "served", "x": x})

    for n in ("j0", "j1", "j2", "j3"):
        arrival(env, n, job, gap=1, max_events=1, first=0)

"""P02: An attribute drawn for each entity.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DistributionConfig, DynamicRealtimeEnvironment, Sampler

PROCS: dict = {}  # entity key -> its process
UNTIL = 20


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

    def t(k):
        x = float(x_stream.exponential(1 / 2))  # drawn when the entity starts
        L(k, "t", {"event_type": "seen", "x": x})
        yield env.timeout(0)

    sampler = Sampler(seed=42)
    gap = DistributionConfig(dist="exponential", rate=1)
    gap.stream = "arrival.a"  # the random sequence the blueprint's draw uses
    x_stream = sampler.generator("arrivals.a.attributes.x")
    arrival(env, "a", t, gap=lambda: sampler.sample(gap))

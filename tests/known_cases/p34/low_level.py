"""P34: A capacity computed from the state.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from typing import Any

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

PROCS: dict = {}  # entity key -> its process
UNTIL = None
PARAMS = SimParameter(
    sim_id="P", resources={"m": CapacityConfig(current_cap=1, max_cap=3)}
)


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


def build(env: DynamicRealtimeEnvironment) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    m = DynamicResource(env, "P", "m")

    def job(k):
        with m.request() as r:
            yield r
            yield env.timeout(5)
            L(k, "job", {"event_type": "done"})

    def grow(k):
        L(k, "grow", {"event_type": "grow"})
        env.registry.update("P.resources.m.current_cap", len(m.queue) + 1)
        yield env.timeout(0)

    arrival(env, "a", job, gap=1, max_events=2, first=0)
    arrival(env, "boss", grow, gap=2, max_events=1)

    def drive():
        env.run()

    return drive

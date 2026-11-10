"""ST-E05: A periodic event that five consumers count.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from typing import Any

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


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    class System:
        def __init__(self, env):
            self.env = env
            self.counter = 0
            self.periodic_event = env.event()

        def periodic(self, k):
            for _ in range(3):
                yield self.env.timeout(1)
                L(k, "tick", {"event_type": "tick"})
                event, self.periodic_event = (self.periodic_event, self.env.event())
                event.succeed(self.counter)

        def consumer(self, k):
            while True:
                yield self.periodic_event
                self.counter += 1
                L(k, "consume", {"event_type": "seen"})

    system = System(env)
    arrival(env, "period", system.periodic, initial=size)
    arrival(env, "consumers", system.consumer, initial=5)

    def drive():
        env.run(until=system.periodic_event)
        env.run(until=system.periodic_event)
        env.run(until=system.periodic_event)
        env.run()

    return drive

"""GD-P1: A car and a battery controller that pause and wake each other.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler

PROCS: dict = {}  # entity key -> its process
UNTIL = 150


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

    sampler = Sampler(seed=42)
    drives = sampler.generator("tasks.drive.service_time")
    parks = sampler.generator("tasks.park.service_time")
    charges = sampler.generator("tasks.charge.service_time")
    cur = [env.event()]

    def drive(k):
        while True:
            yield env.timeout(int(drives.integers(20, 41)))
            L(k, "drive", {"event_type": "start_parking"})
            e = cur[0]
            cur[0] = env.event()
            e.succeed()
            yield env.timeout(int(parks.integers(60, 361)))
            L(k, "park", {"event_type": "stop_parking"})

    def ctrl(k):
        while True:
            yield cur[0]
            L(k, "passivate", {"event_type": "reactivated"})
            yield env.timeout(int(charges.integers(30, 91)))
            L(k, "charge", {"event_type": "charged"})

    arrival(env, "ev", drive, initial=size)
    arrival(env, "ctrl", ctrl, initial=1)

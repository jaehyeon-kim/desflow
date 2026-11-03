"""GD-P3: Interrupt the charging branch of a wait.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = 100


def S(status, **fields):
    """A record value that carries a status."""
    return {"status": status, **fields}


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
    sampler = Sampler(seed=42)
    drives = sampler.generator("tasks.drive.service_time")
    charges = sampler.generator("tasks.bat_ctrl.service_time")

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def drive(k):
        while True:
            yield env.timeout(int(drives.integers(20, 41)))
            L(k, "drive", {"event_type": "start_parking"})
            charging = env.process(bat_ctrl(k))
            parking = env.timeout(60)
            parking.callbacks.append(lambda e: L(k, "park", {"event_type": "parked"}))
            yield (charging | parking)
            if not charging.triggered:
                charging.interrupt("Need to go!")
            L(k, "run_ev", {"event_type": "stop_parking"})

    def bat_ctrl(k):
        try:
            yield env.timeout(int(charges.integers(60, 91)))
            L(k, "bat_ctrl", {"event_type": "bat_done"})
        except simpy.Interrupt as i:
            L(k, "bat_ctrl", S("interrupted", cause=i.cause))

    arrival(env, "ev", drive, initial=size)

"""GD-I3: A driver interrupts a car that catches the interrupt.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 15


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


def _hit(prefix, cause):
    for k, p in list(PROCS.items()):
        if k.startswith(prefix) and p.is_alive:
            p.interrupt(cause)


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def car(k):
        while True:
            try:
                yield env.process(charge(k))
                L(k, "park_and_charge", {"event_type": "charged_and_parked"})
            except simpy.Interrupt as i:
                L(k, "park_and_charge", S("interrupted", cause=i.cause))
            yield env.timeout(2)
            L(k, "drive", {"event_type": "drove"})

    def charge(k):
        yield env.timeout(5)
        L(k, "charge_task", {"event_type": "charge_done"})

    def drv(k):
        L(k, "interrupt_car", {"event_type": "interrupted"})
        _hit("car-", None)
        yield env.timeout(0)

    arrival(env, "car", car, initial=size)
    arrival(env, "driver", drv, gap=3, max_events=1)

"""CV-131: A callback that interrupts or spawns.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = None


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

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    bell = env.event()
    procs = []

    def sleeper(k):
        procs.append(env.active_process)
        try:
            yield env.timeout(10)
            L(k, "sleeper", {"event_type": "slept"})
        except simpy.Interrupt as i:
            L(k, "sleeper", S("interrupted", cause=i.cause))

    def helper(k):
        L(k, "helper", {"event_type": "helper"})
        yield env.timeout(0)

    def on_bell(ev):
        for p in procs:
            if p.is_alive:
                p.interrupt("bell")
        env.process(helper("helper-1"))

    bell.callbacks.append(on_bell)

    def ring(k):
        L(k, "ring", {"event_type": "ring"})
        bell.succeed()
        yield env.timeout(0)

    arrival(env, "s", sleeper, initial=size)
    arrival(env, "r", ring, gap=2, max_events=1)

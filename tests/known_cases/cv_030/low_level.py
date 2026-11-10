"""CV-030: Schedule an event as urgent.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
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


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    sig = env.event()
    schedule = env.schedule

    def urgent_after_2(event, priority=simpy.events.NORMAL, delay=0):
        if event is sig:
            priority, delay = (simpy.events.URGENT, 2)
        schedule(event, priority, delay)

    env.schedule = urgent_after_2

    def m(k):
        L(k, "make", {"event_type": "made"})
        sig.succeed()
        yield env.timeout(0)

    def w(k):
        yield sig
        L(k, "wait_x", {"event_type": "got", "at": env.now})

    arrival(env, "m", m, initial=size)
    arrival(env, "w", w, initial=1)

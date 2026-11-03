"""ST-I06: Two interrupts reach a process that ends after the first.

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

    def child(k):
        try:
            yield env.timeout(1)
            L(k, "child", {"event_type": "finished"})
        except simpy.Interrupt as i:
            L(k, "child", S("interrupted", cause=i.cause))
            L(k, "return_cause", {"cause": i.cause})
            return i.cause

    def parent(k):
        c = env.process(child(k))
        yield env.timeout(0)
        L(k, "parent", {"event_type": "go"})
        c.interrupt(1)
        L(k, "second", {"event_type": "second"})
        c.interrupt(2)
        result = yield c
        L(k, "join", {"event_type": "joined", "result": result})

    arrival(env, "p", parent, initial=size)

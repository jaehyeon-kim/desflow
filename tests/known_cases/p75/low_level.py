"""P75: Read the remaining duration of a component.

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


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    P = {}

    def work(k):
        t0 = env.now
        try:
            yield env.timeout(10)
            L(k, "work", {"event_type": "done"})
        except simpy.Interrupt as i:
            L(k, "work", S("interrupted", cause=i.cause))
            L(k, "left", {"event_type": "left", "remaining": 10 - (env.now - t0)})

    def hit(k):
        L(k, "hit", {"event_type": "hit"})
        P["w"].interrupt("x")
        yield env.timeout(0)

    P["w"] = env.process(work("a-1"))
    arrival(env, "i", hit, gap=4, max_events=1)

"""GD-P4: Wait again for the same request after an interrupt.

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

    m = simpy.Resource(env, 1)

    def block(k):
        with m.request() as r:
            yield r
            yield env.timeout(3)
        L(k, "block", {"event_type": "blocked_done"})

    def victim(k):
        r = m.request()
        while True:
            try:
                yield r
                break
            except simpy.Interrupt as i:
                L(k, "victim", S("interrupted", cause=i.cause))
        left = 1
        while left > 0:
            t0 = env.now
            try:
                yield env.timeout(left)
                left = 0
            except simpy.Interrupt as i:
                left -= env.now - t0
                L(k, "victim", S("interrupted", cause=i.cause))
        m.release(r)
        L(k, "victim", {"event_type": "victim_done"})

    def poker(k):
        L(k, "poker", {"event_type": "poke"})
        for q, p in list(PROCS.items()):
            if q.startswith("victim-") and p.is_alive:
                p.interrupt("x")
        yield env.timeout(0)

    arrival(env, "blocker", block, initial=size)
    arrival(env, "victim", victim, initial=1)
    arrival(env, "poke", poker, gap=1, max_events=1)

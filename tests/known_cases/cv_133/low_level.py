"""CV-133: An interrupt during a queued request.

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

    def hold(k):
        with m.request() as r:
            yield r
            yield env.timeout(3)
        L(k, "hold", {"event_type": "hold"})

    def left(k):
        r = m.request()
        try:
            yield r
            yield env.timeout(1)
            m.release(r)
            L(k, "left", {"event_type": "left_served"})
        except simpy.Interrupt as i:
            r.cancel() if not r.triggered else m.release(r)
            L(k, "left", S("interrupted", cause=i.cause))

    def kept(k):
        r = m.request()
        while True:
            try:
                yield r
                break
            except simpy.Interrupt as i:
                L(k, "kept", S("interrupted", cause=i.cause))
        yield env.timeout(1)
        m.release(r)
        L(k, "kept", {"event_type": "kept_served"})

    def again(k):
        r = m.request()
        while True:
            try:
                yield r
                break
            except simpy.Interrupt as i:
                L(k, "again", S("interrupted", cause=i.cause))
                r.cancel()
                r = m.request()
        yield env.timeout(1)
        m.release(r)
        L(k, "again", {"event_type": "again_served"})

    def poke(k):
        L(k, "poke", {"event_type": "poke"})
        PROCS["a-1"].interrupt("x")
        L(k, "poke_kept", {"event_type": "poke_kept"})
        PROCS["b-1"].interrupt("x")
        L(k, "poke_again", {"event_type": "poke_again"})
        PROCS["c-1"].interrupt("x")
        yield env.timeout(0)

    arrival(env, "h", hold, initial=size)
    for nm, fn in (("a", left), ("b", kept), ("c", again)):
        arrival(env, nm, fn, gap=0.5, max_events=1)
    arrival(env, "i", poke, gap=1, max_events=1)

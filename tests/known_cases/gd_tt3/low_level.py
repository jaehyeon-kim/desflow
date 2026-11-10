"""GD-TT3: Interrupted while queued, keeping the same request.

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

    def pem(k):
        with res.request() as req:
            yield req
            yield env.timeout(1)
        L(k, "hold", {"event_type": "hold_done"})

    def victim(k):
        evt = res.request()
        try:
            yield evt
            raise AssertionError("Should not have gotten the resource.")
        except simpy.Interrupt as i:
            L(k, "victim", S("interrupted", cause=i.cause))
            yield evt
            res.release(evt)
            L(k, "victim", {"event_type": "victim_done"})

    def interruptor(k):
        L(k, "poke", {"event_type": "poke"})
        for q, p in list(PROCS.items()):
            if q.startswith("b-") and p.is_alive:
                p.interrupt()
        return 0
        yield

    res = simpy.Resource(env, 1)
    arrival(env, "a", pem, initial=size)
    arrival(env, "b", victim, initial=1)
    arrival(env, "i", interruptor, initial=1)

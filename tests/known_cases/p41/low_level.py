"""P41: Renege on a drawn patience.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler
import simpy
import simpy.rt

PROCS: dict = {}  # entity key -> its process
UNTIL = 20


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


def build(env: DynamicRealtimeEnvironment, size: int = 3) -> None:
    PROCS.clear()
    patiences = Sampler(seed=42).generator("tasks.job.patience")

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    m = simpy.Resource(env, 1)

    def jobk(k):
        patience = float(patiences.uniform(1, 5))
        req = m.request()
        got = yield (req | env.timeout(patience))
        if req not in got:
            if req.triggered:
                m.release(req)
            else:
                req.cancel()
            L(k, "job", S("reneged"))
            return
        yield env.timeout(10)
        L(k, "job", {"event_type": "served"})
        m.release(req)

    arrival(env, "a", jobk, initial=size)

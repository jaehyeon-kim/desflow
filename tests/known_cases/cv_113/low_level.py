"""CV-113: A cleanup that always runs.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler
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

    DRAWS_R = {p: Sampler(seed=42).generator(p) for p in ["arrivals.a.attributes.bad"]}
    procs = []

    def a(k):
        bad = bool(DRAWS_R["arrivals.a.attributes.bad"].random() < 0.5)
        procs.append(env.active_process)
        try:
            yield env.timeout(2)
            if bad:
                L(k, "body", S("failed", reason="body failed"))
                L(k, "cleanup", {"event_type": "cleanup"})
                return
            L(k, "body", {"event_type": "body"})
            L(k, "cleanup", {"event_type": "cleanup"})
        except simpy.Interrupt as i:
            L(k, "body", S("interrupted", cause=i.cause))
            L(k, "cleanup", {"event_type": "cleanup"})

    def hit(k):
        L(k, "hit", {"event_type": "hit"})
        for p in list(procs):
            if p.is_alive and p is not env.active_process:
                p.interrupt("x")
        yield env.timeout(0)

    arrival(env, "a", a, gap=1, max_events=3)
    arrival(env, "i", hit, gap=2.5, max_events=1)

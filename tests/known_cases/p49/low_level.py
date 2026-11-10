"""P49: A breakdown on a calendar clock.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 40


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

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    m = simpy.PreemptiveResource(env, 1)

    def failer():
        while True:
            yield env.timeout(3)
            with m.request(priority=-1, preempt=True) as r:
                L("m", None, {"status": "down", "reason": "failure"})
                yield r
                yield env.timeout(2)
                L("m", None, {"status": "repaired", "reason": "failure"})

    def job(k):
        left = 1.5
        while left > 1e-09:
            with m.request(priority=0, preempt=False) as r:
                st = None
                try:
                    yield r
                    st = env.now
                    yield env.timeout(left)
                    left = 0
                except simpy.Interrupt:
                    L(k, "job", S("interrupted", cause="breakdown"))
                    if st is not None:
                        left -= env.now - st
        L(k, "job", {"event_type": "done"})

    env.process(failer())
    arrival(env, "a", job, initial=size)

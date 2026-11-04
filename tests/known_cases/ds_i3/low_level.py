"""DS-I3: A flow after an interrupt, then the time left.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

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

    repair_time, work_time = (5, 10)

    def repair(k):
        yield env.timeout(repair_time)
        L(k, "repair", {"event_type": "repaired"})

    def work(k):
        left = work_time
        while left > 0:
            began = env.now
            try:
                yield env.timeout(left)
                left = 0
            except simpy.Interrupt as interrupt:
                left = left - (env.now - began)
                L(k, "work", S("interrupted", cause=interrupt.cause))
                yield env.process(repair(k))
        L(k, "work", {"event_type": "done"})

    def cut(k):
        L(k, "hit", {"event_type": "hit"})
        PROCS["jobs-1"].interrupt("x")
        yield env.timeout(0)

    arrival(env, "jobs", work, gap=1, max_events=1, first=0)
    arrival(env, "breaker", cut, gap=2, max_events=1)

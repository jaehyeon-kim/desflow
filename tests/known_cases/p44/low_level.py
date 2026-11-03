"""P44: One deadline across queues.

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


def build(env: DynamicRealtimeEnvironment, size: int = 3) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    a = simpy.Resource(env, 1)
    b = simpy.Resource(env, 1)

    def c(k):
        deadline = env.now + 5
        with a.request() as r:
            res = yield (r | env.timeout(deadline - env.now))
            if r not in res:
                L(k, "first", S("reneged"))
                return
            yield env.timeout(2)
            L(k, "first", {"event_type": "first_done"})
        with b.request() as r:
            res = yield (r | env.timeout(max(0, deadline - env.now)))
            if r not in res:
                L(k, "second", S("reneged"))
                return
            yield env.timeout(4)
            L(k, "second", {"event_type": "done"})

    arrival(env, "c", c, initial=size)

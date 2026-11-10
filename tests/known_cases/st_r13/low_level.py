"""ST-R13: A preempted process waits for an event that never comes.

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

    res = simpy.PreemptiveResource(env, 1)
    names = {}

    def proc_a(k, resource, prio):
        names[env.active_process] = k
        with resource.request(priority=prio) as req:
            try:
                yield req
                yield env.timeout(1)
                L(k, "proc_a", {"event_type": "should_not_finish"})
            except simpy.Interrupt as i:
                c = i.cause
                L(
                    k,
                    "proc_a",
                    S(
                        "preempted",
                        resource="res",
                        preempted_by=names[c.by],
                        usage_since=c.usage_since,
                    ),
                )
        yield env.event()

    def proc_b(k, resource, prio):
        names[env.active_process] = k
        with resource.request(priority=prio) as req:
            yield req
            L(k, "proc_b", {"event_type": "got"})

    arrival(env, "a", lambda k: proc_a(k, res, 1), initial=size)
    arrival(env, "b", lambda k: proc_b(k, res, 0), initial=1)

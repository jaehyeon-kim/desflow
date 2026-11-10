"""GD-RS5: Preemptive resource that reports who preempted.

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

    def resource_user(k, env, resource, wait, prio):
        yield env.timeout(wait)
        with resource.request(priority=prio) as req:
            L(k, "user", {"event_type": "requesting", "priority": prio})
            yield req
            L(k, "get", {"event_type": "got"})
            try:
                yield env.timeout(3)
                L(k, "hold", {"event_type": "held"})
            except simpy.Interrupt as interrupt:
                by = interrupt.cause.by
                usage = env.now - interrupt.cause.usage_since
                who = {p: n for n, p in PROCS.items()}[by]
                L(
                    k,
                    "hold",
                    S(
                        "preempted",
                        resource="res",
                        preempted_by=who,
                        usage_since=interrupt.cause.usage_since,
                    ),
                )
                L(
                    k,
                    "preempted",
                    {"event_type": "preempted", "by": who, "usage": usage},
                )

    res = simpy.PreemptiveResource(env, capacity=1)
    arrival(
        env, "u1", lambda k: resource_user(k, env, res, wait=0, prio=0), initial=size
    )
    arrival(env, "u2", lambda k: resource_user(k, env, res, wait=1, prio=0), initial=1)
    arrival(env, "u3", lambda k: resource_user(k, env, res, wait=2, prio=-1), initial=1)

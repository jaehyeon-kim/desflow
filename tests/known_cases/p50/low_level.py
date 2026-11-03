"""P50: A failure clock that runs on busy time only.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 16


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

    mach = simpy.PreemptiveResource(env, 1)
    rep = simpy.Resource(env, 1)

    def job(k):
        left = 10.0
        while left > 0:
            with mach.request(priority=1) as r:
                yield r
                t = env.now
                try:
                    yield env.timeout(left)
                    left = 0
                except simpy.Interrupt as i:
                    L(
                        k,
                        "job",
                        S(
                            "preempted",
                            resource="mach",
                            preempted_by=next(
                                (kk for kk, p in PROCS.items() if p is i.cause.by)
                            ),
                            usage_since=i.cause.usage_since,
                        ),
                    )
                    left -= env.now - t
        L(k, "job", {"event_type": "job_done"})

    def other(k):
        with rep.request() as r:
            yield r
            yield env.timeout(6)
            L(k, "other", {"event_type": "other_done"})

    def failer(k):
        while True:
            yield env.timeout(5)
            L(k, "tick", {"event_type": "tick"})
            with mach.request(priority=-1) as m:
                yield m
                with rep.request() as rr:
                    yield rr
                    yield env.timeout(2)
            L(k, "repair", {"event_type": "repaired"})

    arrival(env, "jobs", job, initial=size)
    arrival(env, "others", other, initial=1)
    arrival(env, "clock", failer, initial=1)

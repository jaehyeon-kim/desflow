"""P79: Change the priority rules with preempt and restart flags.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
import itertools
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

    draws = {
        "arrival.a.attributes.p": itertools.cycle([3, 2, 1, 0]),
        "arrival.a.attributes.can": itertools.cycle([True, False]),
    }
    m = simpy.PreemptiveResource(env, 1)

    def job(k):
        p = next(draws["arrival.a.attributes.p"])
        can = next(draws["arrival.a.attributes.can"])
        while True:
            with m.request(priority=p, preempt=can) as r:
                yield r
                try:
                    yield env.timeout(3)
                    L(k, "job", {"event_type": "done"})
                    return
                except simpy.Interrupt as i:
                    who = {p: n for n, p in PROCS.items()}[i.cause.by]
                    L(
                        k,
                        "job",
                        S(
                            "preempted",
                            resource="m",
                            preempted_by=who,
                            usage_since=i.cause.usage_since,
                        ),
                    )

    arrival(env, "a", job, gap=1, max_events=5)

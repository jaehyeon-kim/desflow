"""GD-RS11: A preemptive resource with its own queue or preemption rule.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from typing import Any
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


class MReq(simpy.resources.resource.PriorityRequest):
    def __init__(self, resource, rank, preempt=True):
        resource.n += 1
        t = resource._env.now
        self.qkey = (
            (rank, -t, not preempt, -resource.n)
            if resource.ties == "lifo"
            else (rank, t, not preempt, resource.n)
        )
        super().__init__(resource, rank, preempt)


class RankedResource(simpy.PreemptiveResource):
    """A PreemptiveResource whose queue order and preemption follow design M5."""

    def __init__(self, env, when, victim, ties="fifo"):
        super().__init__(env, 1)
        self.when = when
        self.victim = victim
        self.ties = ties
        self.n = 0

    class PutQueue(list):
        def __init__(self, maxlen=None):
            super().__init__()

        def append(self, item):
            super().append(item)
            super().sort(key=lambda e: e.qkey)

    request = simpy.resources.base.BoundClass(MReq)

    def _do_put(self, event):
        if len(self.users) >= self.capacity and event.preempt:
            victim = max(self.users, key=lambda e: self.victim(e))
            if self.when(event, victim):
                self.users.remove(victim)
                victim.proc.interrupt(
                    simpy.resources.resource.Preempted(
                        by=event.proc, usage_since=victim.usage_since, resource=self
                    )
                )
        return simpy.Resource._do_put(self, event)


def build(env: DynamicRealtimeEnvironment) -> Any:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    draws = {"arrival.a.attributes.deadline": itertools.cycle([20, 17, 14, 11])}

    def when(rq, h):
        return rq.priority < h.priority

    def victim(h):
        return h.priority

    ties = "fifo"
    max_events = 4
    service = 3
    res = RankedResource(env, when, victim, ties)

    def job(k):
        deadline = next(draws["arrival.a.attributes.deadline"])
        left = service
        while left > 0:
            r = res.request(deadline - float(env.now))
            yield r
            start = env.now
            try:
                yield env.timeout(left)
                left = 0
            except simpy.Interrupt as i:
                c = i.cause
                who = {p: n for n, p in PROCS.items()}[c.by]
                L(
                    k,
                    "job",
                    S(
                        "preempted",
                        resource="res",
                        preempted_by=who,
                        usage_since=c.usage_since,
                    ),
                )
                left -= env.now - start
                continue
            res.release(r)
        L(k, "job", {"event_type": "done"})

    arrival(env, "a", job, gap=1, max_events=max_events, first=0)

    def drive():
        env.run()

    return drive

"""DS-M5: A custom queue order and preemption rule.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment
from simpy.core import BoundClass as _M5Bound
from simpy.resources.resource import Preempted as _M5Preempted
from simpy.resources.resource import PriorityRequest as _M5PReq
from simpy.resources.resource import Request as _M5Req
import simpy

PROCS: dict = {}  # entity key -> its process
UNTIL = 30


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


class _M5RankQueue(list):
    """research/yaml-only/programs/M5.py RankQueue: sorts on `order`."""

    def __init__(self, maxlen=None):
        super().__init__()
        self.maxlen = maxlen

    def append(self, item):
        super().append(item)
        self.sort(key=lambda e: e.order)


class _M5RankRequest(_M5PReq):
    def __init__(self, resource, rank=0, preempt=True):
        self.priority, self.preempt, self.time = rank, preempt, resource._env.now
        self.key = (rank, self.time, not preempt)
        self.order = (
            (rank, -self.time, -resource.next_seq(), not preempt)
            if resource.ties == "lifo"
            else self.key
        )
        _M5Req.__init__(self, resource)


class _M5Custom(simpy.PreemptiveResource):
    request = _M5Bound(_M5RankRequest)
    PutQueue = _M5RankQueue

    def __init__(self, env, capacity=1, ties="fifo", when=None, victim=None):
        self.ties, self.when, self.victim, self.seq = ties, when, victim, 0
        super().__init__(env, capacity)

    def next_seq(self):
        self.seq += 1
        return self.seq

    def _do_put(self, event):
        if len(self.users) >= self.capacity and event.preempt:
            value = self.victim or (lambda h: h.key)
            chosen = max(self.users, key=value)
            when = self.when or (lambda r, h: r.key < h.key)
            if when(event, chosen):
                self.users.remove(chosen)
                chosen.proc.interrupt(
                    _M5Preempted(
                        by=event.proc, usage_since=chosen.usage_since, resource=self
                    )
                )
        return simpy.PriorityResource._do_put(self, event)


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    names = {}
    m1 = _M5Custom(env, 1, ties="lifo")
    m2 = _M5Custom(
        env,
        1,
        when=lambda r, h: (
            r.priority < h.priority and r.key < h.key and r.time - h.usage_since > 1
        ),
    )
    m3 = _M5Custom(
        env,
        1,
        when=lambda r, h: (
            r.priority < h.priority and r.key < h.key and r.time - h.usage_since > 1
        ),
    )
    m4 = _M5Custom(env, 2, victim=lambda h: h.priority)

    def job(m, tag, rank, pre, span):

        def f(k):
            names[env.active_process] = k
            with m.request(rank, pre) as r:
                try:
                    yield r
                    yield env.timeout(span)
                    L(k, tag, {"event_type": "done"})
                except simpy.Interrupt as i:
                    c = i.cause
                    L(
                        k,
                        tag,
                        S(
                            "preempted",
                            resource=tag.replace("j", "m"),
                            preempted_by=names[c.by],
                            usage_since=c.usage_since,
                        ),
                    )

        return f

    for n, tag, m, at, rank, pre, span in (
        ("a1", "j1", m1, 0, 1, False, 2),
        ("b1", "j1", m1, 1, 1, False, 1),
        ("c1", "j1", m1, 1, 1, False, 1),
        ("d1", "j1", m1, 1.5, 1, False, 1),
        ("a2", "j2", m2, 0, 5, True, 5),
        ("b2", "j2", m2, 1, 1, True, 1),
        ("a3", "j3", m3, 0, 5, True, 5),
        ("b3", "j3", m3, 2, 1, True, 1),
        ("old", "j4", m4, 0, 5, True, 9),
        ("new", "j4", m4, 1, 5, True, 9),
        ("hi", "j4", m4, 2, 1, True, 9),
    ):
        arrival(env, n, job(m, tag, rank, pre, span), gap=1, max_events=1, first=at)

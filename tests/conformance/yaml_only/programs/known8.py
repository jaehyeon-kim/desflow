"""The SimPy programs of the Batch 8 known cases whose documents run now, one for each
document in `tests/conformance/yaml_only/known8/`. Each is the program of the
prototype's comparison (written from the original SimPy test, example or design) and
returns (log, error): `log` holds (time, key, task, payload) as the blueprint's
records do. A program that draws a random number takes a fixed value from `fixed`, the
value its document writes in place of the draw. `new`, `arrival` and `go` are the
prototype's small harness: `arrival` starts the entities of an arrival in file order
at time 0 (`initial`) and then one every `gap`, `first` after the start."""

import itertools as _it
from typing import Any, Dict, List

import simpy
from simpy.core import BoundClass as _M5Bound
from simpy.resources.container import ContainerGet as _CGet
from simpy.resources.resource import Preempted as _M5Preempted
from simpy.resources.resource import PriorityRequest as _M5PReq
from simpy.resources.resource import Request as _M5Req
from simpy.resources.resource import SortedQueue as _SortedQueue
from simpy.resources.resource import SortedQueue as _SQ
from simpy.resources.store import StoreGet as _StoreGet
from simpy.resources.store import StoreGet as _SG
from simpy.resources.store import StorePut as _StorePut

PROCS: dict = {}  # entity key -> its process, filled by `arrival`
MODE = {"on": False, "n": None, "rev": False}  # a tie run is a different scenario


class fixed:
    """The values the documents write in place of a draw: a constant, or a list that
    the document's `sequence` generator repeats."""

    calls: Dict[str, int] = {}
    values: Dict[str, List[Any]] = {
        "wash.payload.dirt_removed_pct": [70],
        "arrival.cars.gap": [7],
        "arrival.g.attributes.rank": [3, 2, 1, 0],
        "arrival.a.attributes.p": [3, 2, 1, 0],
        "arrival.a.attributes.deadline": [20, 17, 14, 11],
        "arrival.a.attributes.can": [True, False],
    }

    @classmethod
    def _next(cls, purpose):
        listed = cls.values[purpose]
        index = cls.calls.get(purpose, 0)
        cls.calls[purpose] = index + 1
        return listed[index % len(listed)]

    @classmethod
    def integer(cls, purpose, low, high):
        return cls._next(purpose)

    @classmethod
    def boolean(cls, purpose):
        return cls._next(purpose)


def new(t0=0, rt=None):
    PROCS.clear()
    fixed.calls.clear()
    PENDING.clear()
    env = simpy.Environment(t0)
    log = []

    def L(key, task, payload):
        log.append((round(env.now, 6), key, task, payload))

    return env, log, L


PENDING: List[Any] = []


def flush():
    """A sized run starts the arrivals when the run starts, in the order they were
    written, and the first with `initial` takes the size."""
    if not MODE["on"]:
        return
    items = list(PENDING)
    PENDING.clear()
    sized = False
    for item in items:
        item = list(item)
        if MODE["n"] is not None and not sized and item[3]:
            item[3] = MODE["n"]
            sized = True
        _arrival(*item)


def go(env, log, until=None, expect_error=False):
    flush()
    err = None
    try:
        env.run(until=until)
    except BaseException as exc:  # the program's own error is a result
        err = f"{type(exc).__name__}: {str(exc)[:300]}"
    return log, err


def arrival(
    env,
    name,
    make,
    initial=0,
    gap=None,
    max_events=None,
    first=None,
    keyfn=None,
    gate=None,
):
    if MODE["on"]:
        PENDING.append((env, name, make, initial, gap, max_events, first, keyfn, gate))
        return None
    return _arrival(env, name, make, initial, gap, max_events, first, keyfn, gate)


def _arrival(
    env,
    name,
    make,
    initial=0,
    gap=None,
    max_events=None,
    first=None,
    keyfn=None,
    gate=None,
):
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


def EX_2():
    env, log, L = new()
    m = simpy.Resource(env, 2)
    seq = [0]

    def keyfn():
        k = f"Car {seq[0]}"
        seq[0] += 1
        return k

    def wash(k):
        yield env.timeout(5)
        L(
            k,
            "wash",
            {
                "event_type": "washed",
                "dirt_removed_pct": fixed.integer(
                    "wash.payload.dirt_removed_pct", 50, 99
                ),
            },
        )

    def car(k):
        with m.request() as request:
            yield request
            L(k, "car", {"event_type": "enters"})
            yield env.process(wash(k))
            L(k, "washing", {"event_type": "leaves"})

    arrival(
        env,
        "cars",
        car,
        initial=4,
        gap=lambda: fixed.integer("arrival.cars.gap", 5, 9),
        keyfn=keyfn,
    )
    return go(env, log, 20)


def GD_RS1():
    # resources.rst, the first resource_user example: request, yield, timeout(1), release
    env, log, L = new()

    def resource_user(k, resource):
        request = resource.request()  # Generate a request event
        yield request  # Wait for access
        L(k, "user", {"event_type": "got"})
        yield env.timeout(1)  # Do something
        L(k, "use", {"event_type": "used"})
        resource.release(request)  # Release the resource

    res = simpy.Resource(env, capacity=1)
    arrival(env, "a", lambda k: resource_user(k, res), initial=1)
    return go(env, log)


def S(s: Any, **k: Any) -> Dict[str, Any]:
    return dict(status=s, **k)


def GD_RS6(n=None, rev=False):
    # resources.rst, the head start example: A runs until 1, then B (no preempt) and C (preempt) are created and the run goes on
    env, log, L = new()

    def user(k, env, res, prio, preempt):
        with res.request(priority=prio, preempt=preempt) as req:
            try:
                L(k, "user", {"event_type": "requesting"})
                yield req
                L(k, "get", {"event_type": "got"})
                yield env.timeout(3)
                L(k, "hold", {"event_type": "held"})
            except simpy.Interrupt as i:
                c = i.cause
                who = {p: n for n, p in PROCS.items()}[c.by]
                L(
                    k,
                    "hold",
                    S(
                        "preempted",
                        resource="res",
                        preempted_by=who,
                        usage_since=c.usage_since,
                    ),
                )
                L(k, "preempted", {"event_type": "preempted"})

    res = simpy.PreemptiveResource(env, capacity=1)
    for i in range(n or 1):
        k = f"a-{i + 1}"
        PROCS[k] = env.process(user(k, env, res, prio=0, preempt=True))
    go(env, log, 1)  # Give A a head start
    late = [
        ("late-1", -2, False),
        ("late-2", -1, True),
    ]  # one replay arrival with two rows at time 1: keys late-1 and late-2
    if rev:
        late = [
            ("late-1", -1, True),
            ("late-2", -2, False),
        ]  # reversed rows: the file lists C first, so late-1 is C and late-2 is B
    for name, prio, preempt in late:
        k = name
        PROCS[k] = env.process(user(k, env, res, prio=prio, preempt=preempt))
    return go(env, log)


setattr(GD_RS6, "ties", True)


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


def _ranked(when, victim, ties, max_events, service):
    env, log, L = new()
    res = RankedResource(env, when, victim, ties)

    def job(k):
        deadline = fixed.integer("arrival.a.attributes.deadline", 5, 20)
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
    return go(env, log)


def GD_RS11():
    return _ranked(
        lambda rq, h: rq.priority < h.priority, lambda h: h.priority, "fifo", 4, 3
    )


def ST_P08():  # tests/test_process.py test_error_and_interrupted_join: child_a interrupts the parent, child_b raises; the parent no longer waits for b, so the error ends the run
    env, log, L = new()

    def child_a(k, process):
        L(k, "child_a", {"event_type": "a"})
        process.interrupt()
        return
        yield  # Dummy yield

    def child_b(k):
        L(k, "child_b", S("failed", reason="spam"))
        raise AttributeError("spam")
        yield  # Dummy yield

    def parent(k):
        L(k, "parent", {"event_type": "parent"})
        # each child is named from its parent's index, as the document's spawns are
        env.process(child_a(f"child_a-{k.rsplit('-', 1)[1]}", env.active_process))
        L(k, "start_b", {"event_type": "start_b"})
        b = env.process(child_b(k))
        try:
            yield b
        # This interrupt unregisters me from b so I won't receive its AttributeError
        except simpy.Interrupt as i:
            L(k, "join", S("interrupted", cause=i.cause))
        yield env.timeout(0)
        L(k, "after", {"event_type": "after"})

    arrival(env, "p", parent, initial=1)
    return go(env, log)


def ST_R01():
    # tests/test_resources.py test_resource
    env, log, L = new()
    olog = []

    def pem(k, resource):
        name = chr(
            97 + len(olog) + len(resource.users) + len(resource.queue)
        )  # a, b: the process's name in the test
        req = resource.request()
        yield req
        assert resource.count == 1
        L(k, "pem", {"event_type": "got", "count": resource.count})
        yield env.timeout(1)
        resource.release(req)
        L(k, "use", {"event_type": "done"})
        olog.append((name, env.now))

    resource = simpy.Resource(env, capacity=1)
    assert resource.capacity == 1
    assert resource.count == 0
    arrival(env, "users", lambda k: pem(k, resource), initial=2)
    r = go(env, log)
    assert olog == [(chr(97 + i), i + 1) for i in range(len(olog))]
    return r


def ST_R14():
    # test_mixed_preemption
    env, log, L = new()
    res = simpy.PreemptiveResource(env, 1)
    names = {}
    olog = []

    def mk(id, delay, prio, preempt):
        def p(k):
            names[env.active_process] = k
            yield env.timeout(delay)
            L(k, "p", {"event_type": "start"})
            with res.request(priority=prio, preempt=preempt) as req:
                try:
                    yield req
                    yield env.timeout(2)
                    olog.append((env.now, id))
                    L(k, "use", {"event_type": "done"})
                except simpy.Interrupt as ir:
                    olog.append((env.now, id, (ir.cause.by, ir.cause.usage_since)))
                    c = ir.cause
                    L(
                        k,
                        "use",
                        S(
                            "preempted",
                            resource="res",
                            preempted_by=names[c.by],
                            usage_since=c.usage_since,
                        ),
                    )
                    L(
                        k,
                        "lost",
                        {
                            "event_type": "preempted",
                            "by": names[c.by],
                            "since": c.usage_since,
                        },
                    )

        return p

    for n, (d, pr, c) in enumerate(
        [(0, 2, True), (0, 2, True), (1, 1, False), (3, 0, True), (4, 3, True)]
    ):
        arrival(env, f"p{n}", mk(n, d, pr, c), initial=1)
    r = go(env, log)
    p3 = [p for p, k in names.items() if k == "p3-1"][0]
    if not MODE["rev"] and MODE["n"] is None:
        assert olog == [(2, 0), (3, 2, (p3, 2)), (5, 3), (7, 1), (9, 4)]
    return r


def ST_R15():
    env, log, L = new()
    res0 = simpy.PreemptiveResource(env, 1)
    res1 = simpy.PreemptiveResource(env, 1)
    names = {}
    olog = []  # test_nested_preemption

    def lost(key, ir):
        c = ir.cause
        assert isinstance(c, simpy.resources.resource.Preempted)
        rn = "res1" if c.resource is res1 else "res0"
        olog.append((env.now, key, (c.by, c.usage_since, c.resource)))
        L(
            key,
            "two_second",
            S(
                "preempted",
                resource=rn,
                preempted_by=names[c.by],
                usage_since=c.usage_since,
            ),
        )
        L(
            key,
            "lost",
            {
                "event_type": "preempted",
                "by": names[c.by],
                "since": c.usage_since,
                "resource": rn,
            },
        )
        L(key, "lost_free_res0", {"event_type": "freed_after_preemption"})

    def process2(key, delay, prio, preempt):
        names[env.active_process] = key
        yield env.timeout(delay)
        with res0.request(priority=prio, preempt=preempt) as req0:
            try:
                yield req0
                L(key, "two_first", {"event_type": "got_res0"})
                done = False
                with res1.request(priority=prio, preempt=preempt) as req1:
                    try:
                        yield req1
                        yield env.timeout(5)
                        olog.append((env.now, key))
                        L(key, "two_second", {"event_type": "done"})
                        done = True
                    except simpy.Interrupt as ir:
                        lost(key, ir)
                if done:
                    L(key, "two_done", {"event_type": "freed_res0"})
            except simpy.Interrupt as ir:
                lost(key, ir)

    def process(key, res, delay, prio, preempt, task):
        names[env.active_process] = key
        yield env.timeout(delay)
        with res.request(priority=prio, preempt=preempt) as req:
            try:
                yield req
                yield env.timeout(5)
                olog.append((env.now, key))
                L(key, task, {"event_type": "done"})
            except simpy.Interrupt as ir:
                olog.append((env.now, key, (ir.cause.by, ir.cause.usage_since)))

    # every process is created at time 0 and waits its own delay, as in the test; only p0 takes the tie size, so the others start from an arrival with first=0
    arrival(env, "p0", lambda k: process2(k, 0, -1, True), initial=1)
    arrival(
        env,
        "p1",
        lambda k: process(k, res1, 1, -2, True, "one"),
        gap=1,
        max_events=1,
        first=0,
    )
    arrival(
        env, "p2", lambda k: process2(k, 20, -1, True), gap=1, max_events=1, first=0
    )
    arrival(
        env,
        "p3",
        lambda k: process(k, res0, 21, -2, True, "zero"),
        gap=1,
        max_events=1,
        first=0,
    )
    arrival(
        env, "p4", lambda k: process2(k, 21, -1, True), gap=1, max_events=1, first=0
    )
    r = go(env, log)
    P = {k: p for p, k in names.items()}
    ids = {"p0-1": 0, "p1-1": 1, "p2-1": 2, "p3-1": 3, "p4-1": 4}
    if len(names) == 5:
        assert [(e[0], ids[e[1]]) + e[2:] for e in olog] == [
            (1, 0, (P["p1-1"], 0, res1)),
            (6, 1),
            (21, 2, (P["p3-1"], 20, res0)),
            (26, 3),
            (31, 4),
        ]
    return r


def ST_R26():
    # test_filter_store_get_after_mismatch (issue 49)
    env, log, L = new()
    s = simpy.FilterStore(env, 2)

    def getter(k):
        eggs = s.get(lambda i: i == "eggs")
        spam = s.get(lambda i: i == "spam")
        ret = yield spam | eggs
        assert spam in ret
        assert eggs not in ret
        assert env.now == 0
        L(k, "getter", {"event_type": "first", "spam_first": spam.triggered})
        yield eggs
        assert env.now == 1
        L(k, "second", {"event_type": "eggs_at_one"})

    def putter(k):
        yield s.put("spam")
        L(k, "putter", {"event_type": "put_spam"})
        yield env.timeout(1)
        L(k, "later", {"event_type": "pause"})
        yield s.put("eggs")
        L(k, "put_eggs", {"event_type": "put_eggs"})

    arrival(env, "g", getter, initial=1)
    arrival(env, "p", putter, initial=1)
    return go(env, log)


def ST_R29():
    # test_immediate_put_request: two requests and no run; the first is granted at once, the second queues
    env, log, L = new()
    resource = simpy.Resource(env, capacity=1)
    asked = []

    def ask(k):
        request = resource.request()
        asked.append(request)
        if len(asked) == 1:
            assert (
                request.triggered
                and len(resource.users) == 1
                and len(resource.queue) == 0
            )
        elif len(asked) == 2:
            assert (
                not request.triggered
                and len(resource.users) == 1
                and len(resource.queue) == 1
            )
        L(k, "ask", {"event_type": "asked"})
        return
        yield

    def look(k):
        L(
            k,
            "look",
            {
                "event_type": "look",
                "users": len(resource.users),
                "queued": len(resource.queue),
            },
        )
        return
        yield

    arrival(env, "a", ask, initial=2)
    arrival(env, "look", look, initial=1)
    return go(env, log)


def P30():
    # register review-r1/patterns/r5_res.py "P30 simpy": a cap-1 Resource, jobs at 0, 0.5 and 1, hold across two steps (1 then 2); it records the grant ("got") and the release
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def job(k):
        with m.request() as r:
            L(
                k, "t1", S("queued")
            )  # the blueprint publishes a queued record for every request
            yield r
            L(k, "t1", S("started"))
            yield env.timeout(1)
            L(k, "t1", {"event_type": "t1"})
            L(k, "t2", S("started"))
            yield env.timeout(2)
            L(k, "t2", {"event_type": "t2"})

    arrival(env, "a", job, gap=0.5, max_events=3, first=0)
    return go(env, log)


setattr(P30, "grants", True)


def P50b():
    # register review-r1/patterns/writer-checks/p7_tie.py simpy_case: the job is written as machine_shop.py writes it (while left > 0, subtract the time worked on an interrupt); both timeouts fall at 3; until 10. The creation order is the arrival order (run in both orders)
    env, log, L = new()
    mach = simpy.PreemptiveResource(env, 1)

    def job(k):
        left = 3.0
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
                                kk for kk, p in PROCS.items() if p is i.cause.by
                            ),
                            usage_since=i.cause.usage_since,
                        ),
                    )
                    left -= env.now - t
        L(k, "job", {"event_type": "job_done"})

    def failer(k):
        yield env.timeout(3)
        L(k, "tick", {"event_type": "tick"})
        with mach.request(priority=-1) as m:
            yield m
            yield env.timeout(2)
        L(k, "repair", {"event_type": "repaired"})

    arrival(env, "jobs", job, initial=1)
    arrival(env, "clock", failer, initial=1)
    return go(env, log, 10)


class _GStore(simpy.Store):
    GetQueue = _SQ


_SEQ = _it.count()


class _RG(_SG):
    def __init__(self, s, rank, lifo=False):
        self.key = (rank, -next(_SEQ) if lifo else next(_SEQ))
        super().__init__(s)


def P76():
    env, log, L = new()
    q = _GStore(env)

    def want(k):
        rank = fixed.integer("arrival.g.attributes.rank", 0, 3)
        yield _RG(q, rank, True)
        L(k, "want", {"event_type": "got"})

    def supply(k):
        yield q.put(None)
        L(k, "supply", {"event_type": "supplied"})

    arrival(env, "g", want, gap=0.5, max_events=4, keyfn=None)
    arrival(env, "p", supply, gap=2, max_events=4, first=1)
    return go(env, log)


def P77(n=None, rev=False):
    n = n or 1
    env, log, L = new()
    m = simpy.Resource(env, 3)

    def p(i):
        k = f"a-{i}"
        reqs = []
        for name in ("one", "two"):
            r = m.request()
            yield r
            reqs.append(r)
            L(k, "take_one" if name == "one" else "take_two", {"event_type": name})
        yield env.timeout(1)
        L(k, "work", {"event_type": "worked"})
        while True:
            L(k, "release_all", {"event_type": "releasing"})
            m.release(reqs.pop(0))
            if not reqs:
                break

    for i in range(1, n + 1):
        env.process(p(i))
    return go(env, log)


setattr(P77, "ties", True)


def P79():
    env, log, L = new()
    m = simpy.PreemptiveResource(env, 1)

    def job(k):
        p = fixed.integer("arrival.a.attributes.p", 0, 3)
        can = fixed.boolean("arrival.a.attributes.can")
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
    return go(env, log)


def DS_H1():
    # research/yaml-only/programs/H1.py run(2, 3): a job (priority 5) holds the unit across two timed steps; a waiter (priority 9, no preempt) takes it after
    env, log, L = new()
    m = simpy.PreemptiveResource(env, 1)
    x, d = 2, 3

    def job(k):
        with m.request(priority=5) as r:
            yield r
            L(k, "take", {"event_type": "got"})
            yield env.timeout(x)
            L(k, "a", {"event_type": "a"})
            yield env.timeout(d)
            L(k, "b", {"event_type": "b"})

    def other(k):
        with m.request(priority=9, preempt=False) as r:
            yield r
            L(k, "waiter", {"event_type": "waiter_granted"})

    arrival(env, "job", job, initial=1)
    arrival(env, "other", other, initial=1)
    return go(env, log)


def DS_H5():
    # research/yaml-only/programs/H5.py run(1, 0): x takes r1 then r2, y takes r2 then r1, both at 0, one unit each: deadlock, no error
    env, log, L = new()
    r1 = simpy.Resource(env, 1)
    r2 = simpy.Resource(env, 1)

    def job(a, b, names):
        def make(k):
            with a.request() as q:
                yield q
                yield env.timeout(1)
                L(k, names[0], {"event_type": names[3]})
                with b.request() as s:
                    yield s
                    yield env.timeout(1)
                    L(k, names[1], {"event_type": "got_both"})
                L(k, names[2], {"event_type": names[4]})

        return make

    arrival(
        env,
        "first",
        job(r1, r2, ("one_two", "one_two_b", "one_two_c", "got_r1", "free_r1")),
        initial=1,
    )
    arrival(
        env,
        "second",
        job(r2, r1, ("two_one", "two_one_b", "two_one_c", "got_r2", "free_r2")),
        initial=1,
    )
    log, err = go(env, log)
    assert (
        err is None
        and not [r for r in log if r[3]["event_type"] == "got_both"]
        and len(r1.queue) == 1
        and len(r2.queue) == 1
    )  # stated (H5 run(1, 0)): deadlock with no error, nobody gets both units
    return log, err


def DS_H6():
    # research/yaml-only/programs/H6.py run(): holder (5), waiter (10) and a third that releases the holder's request at 1; the first process of the holder arrival (process.instance 0) is the one released
    env, log, L = new()
    m = simpy.Resource(env, 1)
    held = {}

    def holder(k):
        with m.request() as r:
            held[k] = r
            yield r
            yield env.timeout(5)
            assert (
                env.now == 5 and len(m.users) == 1
            )  # stated (H6): the holder goes on at 5 and is not told; the waiter holds the unit
            L(k, "hold", {"event_type": "held"})
        assert (
            env.now == 5 and len(m.users) == 1 and len(m.queue) == 0
        )  # stated (H6): the holder's own release has no effect

    def waiter(k):
        with m.request() as q:
            yield q
            yield env.timeout(10)
            L(k, "want", {"event_type": "served"})

    def third(k):
        yield env.timeout(1)
        L(k, "free_it", {"event_type": "freed"})
        m.release(held["holder-1"])

    arrival(env, "holder", holder, initial=1)
    arrival(env, "waiter", waiter, initial=1)
    arrival(env, "third", third, initial=1)
    log, err = go(env, log)
    assert (
        [(r[0], r[2]) for r in log] == [(1, "free_it"), (5, "hold"), (11, "want")]
        and len(m.users) == 0
    )  # stated (H6): released by the third at 1, the waiter is granted at 1 and ends at 11
    return log, err


def DS_H8():
    # research/yaml-only/programs/H8.py run([(0, 1), (4, 2)], [0, 1, 2, 5, 6]): the queue limit is 1, then 2 from time 4; service 10
    env, log, L = new()
    m = simpy.Resource(env, 1)
    limit = [1]
    service = 10

    def ctl():
        yield env.timeout(4)
        limit[0] = 2

    def job(k):
        if limit[0] is not None and len(m.queue) >= limit[0]:
            L(k, "job", {"status": "balked", "resource": "m"})
            return
        with m.request() as r:
            yield r
            yield env.timeout(service)
            L(k, "job", {"event_type": "done"})

    env.process(ctl())
    arrival(env, "early", job, gap=1, max_events=3, first=0)
    arrival(env, "late", job, gap=1, max_events=2, first=5)
    return go(env, log)


def DS_H9():
    # research/simpy-gap/review-r3/python-checks/cancel_and_name/sp.py h(): a request, the grant, cancel() after the grant, hold 3, release; b asks at 1 and holds 1
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def first(k):
        req = m.request()
        yield req
        L(k, "first_job", {"event_type": "opened"})
        req.cancel()
        L(k, "cancel_it", {"event_type": "cancelled_after_grant"})
        yield env.timeout(3)
        L(k, "hold_on", {"event_type": "still_held"})
        m.release(req)

    def second(k):
        req = m.request()
        yield req
        yield env.timeout(1)
        L(k, "second_job", {"event_type": "got"})
        m.release(req)

    arrival(env, "a", first, initial=1)
    arrival(env, "b", second, gap=1, max_events=1)
    return go(env, log)


def _hit(prefix, cause):
    for k, p in list(PROCS.items()):
        if k.startswith(prefix) and p.is_alive:
            p.interrupt(cause)


def DS_I10():
    env, log, L = new()

    def waiter(k):
        ev = env.timeout(5)
        while True:
            try:
                yield ev
                break
            except simpy.Interrupt as i:
                L(k, "waiter", S("interrupted", cause=i.cause))
        L(k, "waiter", {"event_type": "ended"})

    def poke(k):
        L(k, "poke", {"event_type": "poke"})
        _hit("a-", "x")
        yield env.timeout(0)

    arrival(env, "a", waiter, initial=1)
    arrival(env, "b", poke, gap=2, max_events=1)
    log, err = go(env, log)
    assert [
        (r[0], r[3].get("cause"), r[3].get("event_type"))
        for r in log
        if r[2] == "waiter"
    ] == [
        (2, "x", None),
        (5, None, "ended"),
    ]  # stated: interrupted at 2, then the same timeout ends at 5, not 7
    return log, err


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


def DS_M5():
    # research/yaml-only/programs/M5.py, the Custom class with the cases "ties lifo", "when with a time rule, not yet" and "met", and "victim value holder.rank" (capacity 2); four independent resources in one environment, run until 30. A record comes at the end of a hold or at a preemption.
    env, log, L = new()
    names = {}
    m1 = _M5Custom(env, 1, ties="lifo")
    m2 = _M5Custom(
        env,
        1,
        when=lambda r, h: r.priority < h.priority and env.now - h.usage_since > 1,
    )
    m3 = _M5Custom(
        env,
        1,
        when=lambda r, h: r.priority < h.priority and env.now - h.usage_since > 1,
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
    return go(env, log, 30)


def DS_M7():
    # research/yaml-only/README.md row M7 and step-1-findings.md M7: a flow of two tasks (3, then 3) runs as one process; ANOTHER entity yields that process and goes on at 6
    env, log, L = new()

    def relay(k):
        yield env.timeout(3)
        L(k, "relay", {"event_type": "relay"})
        yield env.timeout(3)
        L(k, "more", {"event_type": "more"})

    def watch(k):
        yield PROCS["a-1"]
        L(k, "watch", {"event_type": "flow_done", "at": float(env.now)})

    arrival(env, "a", relay, initial=1)
    arrival(env, "w", watch, initial=1)
    log, err = go(env, log)
    assert [r[3]["at"] for r in log if r[2] == "watch"] == [
        6.0
    ]  # stated: yielding the process goes on at 6 (its end), not at 3
    return log, err


def DS_Y4():
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def w(k):
        try:
            yield env.timeout(9)
            L(k, "w", {"event_type": "w"})
        except simpy.Interrupt as i:
            L(k, "w", S("interrupted", cause=i.cause))

    def flow(k):
        slot = m.request()
        limit = env.timeout(5, "late")
        helper = env.process(w(k))
        L(k, "begin", {"event_type": "begin"})
        yield env.timeout(1)
        L(k, "work", {"event_type": "work"})
        yield slot | limit
        L(k, "join", {"event_type": "join"})
        if helper.is_alive:
            L(k, "stop", {"event_type": "stop"})
            helper.interrupt("stop")
        slot.cancel()
        L(k, "end", {"event_type": "end"})
        if slot in m.users:
            m.release(slot)

    def src():
        for i in (1, 2):
            env.process(flow(f"a-{i}"))
            yield env.timeout(1)

    env.process(src())
    return go(env, log)


def DS_Y8():
    # research/yaml-only/programs/Y8.py computed(1), computed(3) and branches(): a low holder (priority 5) for 5 from 0 and a high request at 1 with priority p and preempt p < 2 (two independent units), and two held units with a joint request p (m rank 0, n rank 5) at 0.5 and an n request q (rank 1) at 1. The source logs the grant time; a record comes at the end of the 1 s hold with the grant time.
    env, log, L = new()
    names = {}

    def jobs(rname, tag):
        r = simpy.PreemptiveResource(env, 1)

        def job(pr):
            def f(k):
                names[env.active_process] = k
                with r.request(priority=pr, preempt=pr < 2) as q:
                    yield q
                    try:
                        yield env.timeout(5)
                        L(k, tag, {"event_type": "done"})
                    except simpy.Interrupt as i:
                        L(
                            k,
                            tag,
                            S(
                                "preempted",
                                resource=rname,
                                preempted_by=names[i.cause.by],
                                usage_since=i.cause.usage_since,
                            ),
                        )

            return f

        return job

    j1 = jobs("r1", "job1")
    j2 = jobs("r2", "job2")
    m = simpy.PriorityResource(env)
    n = simpy.PriorityResource(env)

    def hold(r):
        def f(k):
            with r.request() as q:
                yield q
                yield env.timeout(2)

        return f

    def ask(k):
        x = m.request(priority=0)
        y = n.request(priority=5)
        yield x & y
        got = env.now
        yield env.timeout(1)
        m.release(x)
        n.release(y)
        L(k, "ask", {"event_type": "got", "at": got})

    def only_n(k):
        with n.request(priority=1) as q:
            yield q
            got = env.now
            yield env.timeout(1)
            L(k, "only_n", {"event_type": "got", "at": got})

    for nm, f, at in (
        ("low1", j1(5), 0),
        ("high1", j1(1), 1),
        ("low2", j2(5), 0),
        ("high2", j2(3), 1),
        ("hm", hold(m), 0),
        ("hn", hold(n), 0),
        ("p", ask, 0.5),
        ("q", only_n, 1),
    ):
        arrival(env, nm, f, gap=1, max_events=1, first=at)
    return go(env, log)


class _GetRanked(simpy.Store):
    GetQueue = _SortedQueue


class _PutRanked(simpy.Store):
    PutQueue = _SortedQueue


class _RankedContainer(simpy.Container):
    GetQueue = _SortedQueue


_YSEQ = _it.count()


class _RankedGet(_StoreGet):  # research/yaml-only/programs/Y12.py RankedGet
    def __init__(self, store, rank):
        self.key = (rank, store._env.now, next(_YSEQ))
        super().__init__(store)


class _RankedGetC(_CGet):
    def __init__(self, c, amount, rank):
        self.key = (rank, c._env.now, next(_YSEQ))
        super().__init__(c, amount)


class _RankedPut(_StorePut):
    def __init__(self, store, item, rank):
        self.key = (rank, store._env.now, next(_YSEQ))
        super().__init__(store, item)


def DS_Y12():
    # research/yaml-only/programs/Y12.py gets([5, 1, 3]), gets([1, 1, 0]), puts() and blocked_head(), with the source's ranked request classes; four independent groups in one environment.
    env, log, L = new()
    sA = _GetRanked(env)
    sB = _GetRanked(env)
    shelf = _PutRanked(env, capacity=1)
    c = _RankedContainer(env, capacity=10, init=0)

    def getter(s, tag, i, r):
        def f(k):
            v = yield _RankedGet(s, r)
            L(k, "get" + tag, {"event_type": "got", "i": i, "item": v})

        return f

    def putter(s, tag):
        def f(k):
            yield env.timeout(1)
            for number, x in enumerate("abc", 1):
                yield s.put(x)
                L(k, f"put{tag}{number}", {"event_type": "put"})

        return f

    def shelve(name, r):
        def f(k):
            yield _RankedPut(shelf, name, r)
            L(k, "shelve", {"event_type": "in", "name": name})

        return f

    def take(k):
        v = yield shelf.get()
        L(k, "take", {"event_type": "out", "name": v})

    def cget(name, a, r):
        def f(k):
            yield _RankedGetC(c, a, r)
            L(k, "cget", {"event_type": "got", "name": name, "level": c.level})

        return f

    def cput(k):
        yield env.timeout(1)
        yield c.put(2)
        yield env.timeout(1)
        yield c.put(3)

    for n, f in (
        ("ga0", getter(sA, "A", 0, 5)),
        ("ga1", getter(sA, "A", 1, 1)),
        ("ga2", getter(sA, "A", 2, 3)),
        ("pa", putter(sA, "A")),
        ("gb0", getter(sB, "B", 0, 1)),
        ("gb1", getter(sB, "B", 1, 1)),
        ("gb2", getter(sB, "B", 2, 0)),
        ("pb", putter(sB, "B")),
    ):
        arrival(env, n, f, gap=1, max_events=1, first=0)
    arrival(env, "wf", shelve("first", 0), initial=1)
    arrival(env, "wl", shelve("low", 5), initial=1)
    arrival(env, "wh", shelve("high", 1), initial=1)
    arrival(env, "tk", take, gap=1, max_events=3, first=1)
    arrival(env, "cbig", cget("big, rank 0", 5, 0), gap=1, max_events=1, first=0)
    arrival(env, "csmall", cget("small, rank 1", 1, 1), gap=1, max_events=1, first=0)
    arrival(env, "cp", cput, gap=1, max_events=1, first=0)
    return go(env, log)


def CV_069():
    # research/yaml-only/programs/X2.py run("yield", n, holder_first): capacity 2, holders (request, wait 1, yield release) and others (wait 1, wait 0)
    env, log, L = new()
    res = simpy.Resource(env, 2)

    def take(k):
        q = res.request()
        yield q
        yield env.timeout(1)
        L(k, "take", {"event_type": "held"})
        yield res.release(q)
        L(k, "zero", {"event_type": "after_release"})

    def watch(k):
        yield env.timeout(1)
        L(k, "watch", {"event_type": "o"})
        yield env.timeout(0)
        L(k, "again", {"event_type": "o_again"})

    arrival(env, "h", take, initial=2)
    arrival(env, "o", watch, initial=2)
    return go(env, log)

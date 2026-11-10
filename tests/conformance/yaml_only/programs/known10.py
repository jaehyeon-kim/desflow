"""The SimPy programs of the Batch 10 known cases (breakdown clock, batches and
monitoring), one for each document in `tests/conformance/yaml_only/known10/`. Each is the
program of the prototype's comparison and returns (log, error): `log` holds
(time, key, task, payload) as the blueprint's records do. The monitoring programs also
fill `TELE`, the (time, count, queue length) each patched call saw, and `TRACE`, the
(time, priority, class name) of each event the run processed. The harness (`new`,
`arrival`, `go`) is the one of `known8`."""

import simpy

from tests.conformance.yaml_only.programs.known8 import (  # noqa: F401
    MODE,
    PENDING,
    PROCS,
    _arrival,
    arrival,
    go,
    new,
)

TELE: list = []
TRACE: list = []


def S(status, **extra):
    return dict(status=status, **extra)


def DS_B1():
    # B1.py run(jobs), corrected in two places: a job keeps the unit after a repair
    # until it is done, and the clock takes a busy signal that arrives while it
    # repairs. A failure clock on busy time, time to failure 5, repair 2, jobs of 3 at
    # 0, 4, 8, 12 and 16, until 40.
    env, log, L = new()
    ttf, repair = 5, 2
    m = simpy.PreemptiveResource(env, 1)
    left, holding = [ttf], [0]
    start, stop = [env.event()], [env.event()]

    def on():
        holding[0] = 1
        e = start[0]
        start[0] = env.event()
        e.succeed()

    def off():
        holding[0] = 0
        e = stop[0]
        stop[0] = env.event()
        e.succeed()

    def job(name, at, service):
        yield env.timeout(at)
        r = m.request(priority=1)
        yield r
        on()
        rem = service
        while rem > 0:
            began = env.now
            try:
                yield env.timeout(rem)
                rem = 0
            except simpy.Interrupt:
                rem -= env.now - began
                L(name, "job", {"status": "interrupted", "cause": "breakdown"})
                r = m.request(priority=0)
                yield r
                on()
        L(name, "job", {"event_type": "done"})
        off()
        m.release(r)

    def clock():
        while True:
            if not holding[0]:
                yield start[0]
            began = env.now
            to = env.timeout(left[0])
            stp = stop[0]
            yield to | stp
            left[0] -= env.now - began
            if left[0] <= 1e-12:
                holding[0] = 0
                with m.request(priority=-1) as q:
                    L("m", None, S("down", reason="failure"))
                    yield q
                    yield env.timeout(repair)
                    L("m", None, S("repaired", reason="failure"))
                left[0] = ttf

    env.process(clock())
    for i in range(5):
        env.process(job(f"a-{i + 1}", 4 * i, 3))
    return go(env, log, 40)


def P50():
    # review-r1/patterns/r9_break.py "P50 simpy": mach is a PreemptiveResource, rep a
    # Resource; the job needs 10 on mach (priority 1, resumes), other holds rep for 6,
    # the failure clock waits 5, takes mach at priority -1 and then rep, holds 2
    env, log, L = new()
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
                                kk for kk, p in PROCS.items() if p is i.cause.by
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

    arrival(env, "jobs", job, initial=1)
    arrival(env, "others", other, initial=1)
    arrival(env, "clock", failer, initial=1)
    return go(env, log, 16)


def P49():
    # audit2/patterns/mine/g_e.py P49: a PreemptiveResource; a failer process waits 3,
    # takes the machine at priority -1 with preemption, holds 2, and repeats; jobs ask
    # at priority 0 without preemption, service 1.5, resume with the time left
    env, log, L = new()
    m = simpy.PreemptiveResource(env, 1)

    def failer():
        while True:
            yield env.timeout(3)
            with m.request(priority=-1, preempt=True) as r:
                L("m", None, S("down", reason="failure"))
                yield r
                yield env.timeout(2)
                L("m", None, S("repaired", reason="failure"))

    def job(k):
        left = 1.5
        while left > 1e-9:
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
    arrival(env, "a", job, initial=3)
    return go(env, log, 40)


def _batcher(env, L, size, timeout, rule, task):
    """The batcher of B3.py: members put themselves and an event in a store; a batcher
    takes the first, then more until `size`, the rule or the timeout, and then lets them
    go in the order they came. Returns the member process."""
    st = simpy.Store(env)

    def member(k):
        ev = env.event()
        yield st.put((k, ev))
        yield ev
        L(k, task, {"event_type": "packed"})

    def batcher():
        while True:
            first = yield st.get()
            batch = [first]
            deadline = env.timeout(timeout)
            while len(batch) < size and not (rule and rule(batch)):
                g = st.get()
                yield g | deadline
                if g.triggered:
                    batch.append(g.value)
                if deadline.processed:
                    if not g.triggered:
                        g.cancel()
                    break
            for _, ev in batch:
                ev.succeed()

    env.process(batcher())
    return member


def _members(env, member, names, times):
    for name, at in zip(names.split(), times):
        if at == 0:
            arrival(env, name, member, initial=1)
        else:
            arrival(env, name, member, gap=1, max_events=1, first=at)


def DS_B3():
    # B3.py run(A) (size 3, timeout 2, no rule), and the rule line (members at 0, 1
    # and 1.5, a rule of two members)
    env, log, L = new()
    plain = _batcher(env, L, 3, 2, None, "pack")
    ruled = _batcher(env, L, 3, 2, lambda batch: len(batch) >= 2, "pack_rule")
    _members(env, plain, "m0 m1 m2 m3 m4", (0, 1, 5, 5.5, 5.8))
    _members(env, ruled, "ra rb rc", (0, 1, 1.5))
    return go(env, log, 30)


def P52():
    # B3.py with size 3 and timeout 2, members at 0, 1, 5, 5.5 and 5.8
    env, log, L = new()
    plain = _batcher(env, L, 3, 2, None, "pack")
    _members(env, plain, "m0 m1 m2 m3 m4", (0, 1, 5, 5.5, 5.8))
    return go(env, log, 30)


def P53():
    # as P52, and the batcher also closes when it holds two members
    env, log, L = new()
    ruled = _batcher(env, L, 3, 2, lambda batch: len(batch) >= 2, "pack")
    _members(env, ruled, "m0 m1 m2 m3 m4", (0, 1, 5, 5.5, 5.8))
    return go(env, log, 30)


def _source(env, name, make, initial):
    """The process a blueprint's arrival is: it starts its entities and ends."""
    if MODE["on"] and MODE["n"] is not None:
        initial = MODE["n"]  # a sized run is a different number of entities
    _arrival(env, name, make, initial=initial)
    return
    yield


def _watch_resource(env, res):
    def post(resource):
        TELE.append((float(env.now), resource.count, len(resource.queue)))

    for name in ("request", "release"):
        original = getattr(res, name)

        def wrapped(*args, _f=original, **kwargs):
            ret = _f(*args, **kwargs)
            post(res)
            return ret

        setattr(res, name, wrapped)


def _trace_steps(env):
    original = env.step

    def tracing_step():
        if len(env._queue):
            t, prio, eid, event = env._queue[0]
            TRACE.append((float(t), prio, type(event).__name__))
        return original()

    env.step = tracing_step


def DS_M1():
    # M1.py: two jobs at time 0 on one unit, readings after each request and release
    # call, and the event trace; a job holds for timeout(0). The jobs start from a
    # source process, as the arrival of a blueprint starts them.
    TELE.clear()
    TRACE.clear()
    env, log, L = new()
    res = simpy.Resource(env, 1)
    _watch_resource(env, res)
    _trace_steps(env)

    def job(k):
        with res.request() as r:
            yield r
            yield env.timeout(0)
            L(k, "use", {"event_type": "used"})

    env.process(_source(env, "a", job, 2))
    return go(env, log)


def GD_M2():
    # monitoring.rst:125-183: request, release, put and get patched to record (now,
    # count, queue length) after each call
    TELE.clear()
    env, log, L = new()
    res = simpy.Resource(env, capacity=1)
    _watch_resource(env, res)

    def test_process(k):
        with res.request() as req:
            yield req
            yield env.timeout(1)
        L(k, "use", {"event_type": "used"})

    arrival(env, "a", test_process, initial=1)
    return go(env, log)


def GD_M3():
    # monitoring.rst:196-222: a Resource subclass that records the queue before each
    # request and release; the series the blueprint publishes is the reading after
    # each call
    TELE.clear()
    env, log, L = new()
    res = simpy.Resource(env, capacity=1)
    _watch_resource(env, res)

    def test_process(k):
        with res.request() as req:
            yield req
            yield env.timeout(1)
        L(k, "use", {"event_type": "used"})

    arrival(env, "a", test_process, initial=2)
    return go(env, log)


def GD_M4():
    # monitoring.rst:255-296: env.step patched to trace every event
    TRACE.clear()
    env, log, L = new()
    _trace_steps(env)

    def test_process(k):
        yield env.timeout(1)
        L(k, "ticking", {"event_type": "tick"})

    env.process(_source(env, "a", test_process, 1))
    return go(env, log)


# The programs whose first arrival the sized runs resize, though the call is not written
# with `initial=` in the program's own source
SIZED = {"DS_M1", "GD_M4", "P52", "P53"}

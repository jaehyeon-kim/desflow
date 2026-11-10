"""The SimPy programs of the Batch 9 known cases (interrupts and failures), one for each
document in `tests/conformance/yaml_only/known9/`. Each is the program of the
prototype's comparison (written from the original SimPy test, example or design) and
returns (log, error): `log` holds (time, key, task, payload) as the blueprint's records
do. The harness (`new`, `arrival`, `go`) is the one of `known8`."""

import simpy

from tests.conformance.yaml_only.programs.known8 import (  # noqa: F401
    MODE,
    PENDING,
    PROCS,
    arrival,
    go,
    new,
)


def S(status, **extra):
    return dict(status=status, **extra)


def DS_I3():
    # I3.py run([2]): work 10 is interrupted at 2 with cause "x", a repair flow of 5
    # runs, then the time left (resume) is worked
    env, log, L = new()
    repair_time, work_time = 5, 10

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
    return go(env, log)


def DS_I4():
    # I4.py run([1, 2, 3], limit=1, where=lambda i: i == 2, resume_at=2): three park at
    # 0, the second one is resumed with value 9
    env, log, L = new()
    parked = []
    limit, value, count = 1, 9, [0]

    def where(i):
        return i == 2

    def park(k):
        count[0] += 1
        index = count[0]
        event = env.event()
        parked.append((index, event))
        got = yield event
        L(k, "park", {"event_type": "woke", "got": got})

    def wake(k):
        chosen = [p for p in parked if where(p[0])][:limit]
        for index, event in chosen:
            event.succeed(value)
            parked.remove((index, event))
        L(k, "wake", {"event_type": "wake"})
        yield env.timeout(0)

    arrival(env, "parked", park, initial=3)
    arrival(env, "waker", wake, gap=2, max_events=1)
    log, error = go(env, log)
    if not MODE["on"]:
        woken = [(r[0], r[1], r[3]["got"]) for r in log if r[2] == "park"]
        assert woken == [(2, "parked-2", 9)]
        assert [i for i, _ in parked] == [1, 3]
    return log, error


def P65():
    # I4.py run([1, 2, 3]): three parked at 0, the resumer at 2 resumes all with 9
    env, log, L = new()
    events = []

    def park(k):
        event = env.event()
        events.append(event)
        yield event
        L(k, "park", {"event_type": "woke"})

    def wake(k):
        L(k, "wake", {"event_type": "wake"})
        for event in events:
            event.succeed(9)
        yield env.timeout(0)

    arrival(env, "parked", park, initial=3)
    arrival(env, "waker", wake, gap=2, max_events=1)
    return go(env, log)


def P74():
    # I6.py run(4): the holder keeps the unit 5 s from 0; a late entity at 1 has
    # patience 4, which ends at the release instant 5; elsewhere follows a renege
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def holder(k):
        with m.request() as r:
            yield r
            yield env.timeout(5)
            L(k, "hold", {"event_type": "held"})

    def late(k):
        with m.request() as req:
            res = yield req | env.timeout(4)
            if req in res:
                L(k, "job", {"event_type": "served"})
            else:
                L(k, "job", S("reneged"))
                L(k, "elsewhere", {"event_type": "elsewhere"})

    arrival(env, "h", holder, initial=1)
    arrival(env, "late", late, gap=1, max_events=1)
    return go(env, log)


def DS_I6():
    # I6.py run(2), run(4) and run(2, limit=0): three independent holder and late pairs
    env, log, L = new()

    def pair(n, patience, limit):
        m = simpy.Resource(env, 1)

        def hold(k):
            with m.request() as r:
                yield r
                yield env.timeout(5)

        def elsewhere(k, why):
            L(k, "elsewhere", {"event_type": "elsewhere", "why": why})
            yield env.timeout(0)

        def late(k):
            if limit is not None and len(m.queue) >= limit:
                L(k, "late%d" % n, S("balked"))
                yield env.process(elsewhere(k, "balked"))
                return
            with m.request() as req:
                res = yield req | env.timeout(patience)
                if req in res:
                    L(k, "late%d" % n, {"event_type": "served"})
                else:
                    L(k, "late%d" % n, S("reneged"))
                    yield env.process(elsewhere(k, "reneged"))

        arrival(env, "h%d" % n, hold, gap=1, max_events=1, first=0)
        arrival(env, "l%d" % n, late, gap=1, max_events=1, first=1)

    pair(1, 2, None)
    pair(2, 4, None)
    pair(3, 2, 0)
    return go(env, log)


def P39():
    # balk when 1 or more wait, service 2, entities at one instant; the balked entity
    # goes to "other"
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def job(k):
        if len(m.queue) >= 1:
            L(k, "job", S("balked"))
            L(k, "other", {"event_type": "other"})
            return
        with m.request() as r:
            yield r
            yield env.timeout(2)
            L(k, "job", {"event_type": "served"})

    arrival(env, "a", job, initial=3)
    return go(env, log)


def P42():
    # capacity 1, service 3, arrivals at 0, 1 and 2, patience 2.5; a customer who
    # reneges spends 1 second at "other"
    env, log, L = new()
    m = simpy.Resource(env, 1)

    def job(k):
        with m.request() as r:
            x = yield r | env.timeout(2.5)
            if r in x:
                yield env.timeout(3)
                L(k, "job", {"event_type": "served"})
                return
        L(k, "job", S("reneged"))
        yield env.timeout(1)
        L(k, "elsewhere", {"event_type": "elsewhere"})

    arrival(env, "a", job, gap=1, max_events=3, first=0)
    return go(env, log)


def ST_R10():
    # test_sorted_queue_maxlen: one process makes three requests, the third raises
    # RuntimeError and the test catches it. In the blueprint the third open request
    # balks at its own step, with the queue full as its balk reason.
    env, log, L = new()
    resource = simpy.PriorityResource(env, capacity=1)
    resource.put_queue.maxlen = 1

    def process(k):
        # each request that finds the queue full raises; the flow ends at the step
        # that made it
        try:
            resource.request(priority=1)
            resource.request(priority=1)
        except RuntimeError as error:
            assert "Cannot append event. Queue is full." in str(error)
            L(k, "pem", S("balked", resource="res"))
            return
        L(k, "pem", {"event_type": "opened"})
        try:
            resource.request(priority=1)
        except RuntimeError as error:
            assert "Cannot append event. Queue is full." in str(error)
            L(k, "third", S("balked", resource="res"))
        yield env.timeout(0)

    arrival(env, "users", process, initial=1)
    return go(env, log)


def ST_X01():
    # test_error_forwarding: a failing child is caught by the parent that waits for it
    env, log, L = new()

    def child(k):
        L(k, "child", S("failed", reason="Onoes!"))
        raise ValueError("Onoes!")
        yield env.timeout(1)  # pragma: no cover

    def parent(k):
        try:
            yield env.process(child(k))
            L(k, "parent", {"event_type": "no_error"})
        except ValueError as error:
            assert str(error) == "Onoes!"
            L(k, "parent", S("failed", reason=str(error)))
            L(k, "caught", {"event_type": "caught", "reason": str(error)})

    arrival(env, "p", parent, initial=1)
    return go(env, log)


def GD_TT2():
    # tests/test_interrupts.py:29: three interrupts at one time, each with its own cause
    env, log, L = new()
    flog = []

    def fox(k):
        while True:
            try:
                yield env.timeout(10)
                L(k, "fox", {"event_type": "ran"})
            except simpy.Interrupt as interrupt:
                flog.append((env.now, interrupt.cause))
                L(k, "fox", S("interrupted", cause=interrupt.cause))
                L(
                    k,
                    "note_cause",
                    {"event_type": "interrupted", "cause": interrupt.cause},
                )

    def farmer(name):
        def f(k):
            L(k, "farmer", {"event_type": "farmer"})
            for q, p in list(PROCS.items()):
                if q.startswith("fox-") and p.is_alive:
                    p.interrupt(name)
            yield env.timeout(1)

        return f

    arrival(env, "fox", fox, initial=1)
    for name in ("boggis", "bunce", "beans"):
        arrival(env, name, farmer(name), initial=1)
    log, error = go(env, log, 20)
    if not MODE["on"]:
        assert flog == [(0, "boggis"), (0, "bunce"), (0, "beans")]
    return log, error


def EX_3():
    # machine_shop.py: Machine.working, Machine.break_machine and other_jobs in the
    # original order. The random draws are two repeating sequences, the same two the
    # document writes.
    env, log, L = new()
    repairman = simpy.PreemptiveResource(env, capacity=1)
    draws = {"part": 0, "gap": 0}

    def part_time():
        draws["part"] += 1
        return 8 + (draws["part"] - 1) % 5

    def break_gap():
        draws["gap"] += 1
        return 250 + (draws["gap"] - 1) % 7 * 10

    class Machine:
        def __init__(self, name):
            self.name = name
            self.broken = False

        def working(self):
            self.process = env.active_process
            env.process(self.break_machine())
            L(self.name, "start_machine", {"event_type": "machine_started"})
            while True:
                done_in = part_time()
                while done_in:
                    start = env.now
                    try:
                        yield env.timeout(done_in)
                        done_in = 0
                    except simpy.Interrupt:
                        L(self.name, "part", S("interrupted", cause=None))
                        L(self.name, "begin_repair", {"event_type": "broken"})
                        self.broken = True
                        done_in -= env.now - start
                        with repairman.request(priority=1) as req:
                            yield req
                            yield env.timeout(30)
                        L(self.name, "repair", {"event_type": "repaired"})
                        self.broken = False
                L(self.name, "part", {"event_type": "part_made"})

        def break_machine(self):
            idx = int(self.name.split()[1])
            while True:
                yield env.timeout(break_gap())
                L(f"break_loop-{idx + 1}", "break_loop", {"event_type": "tick"})
                if not self.broken:
                    self.process.interrupt()

    def machine(k):
        return Machine(k).working()

    def other_jobs(k):
        while True:
            done_in = 30
            while done_in:
                with repairman.request(priority=2) as req:
                    yield req
                    start = env.now
                    try:
                        yield env.timeout(done_in)
                        done_in = 0
                    except simpy.Interrupt as i:
                        c = i.cause
                        by = {p: n for n, p in PROCS.items()}[c.by]
                        L(
                            k,
                            "other_job",
                            S(
                                "preempted",
                                resource="repairman",
                                preempted_by=by,
                                usage_since=c.usage_since,
                            ),
                        )
                        done_in -= env.now - start
            L(k, "other_job", {"event_type": "other_done"})

    count = [0]

    def keyfn():
        k = f"Machine {count[0]}"
        count[0] += 1
        return k

    arrival(env, "machines", machine, initial=10, keyfn=keyfn)
    arrival(env, "others", other_jobs, initial=1)
    return go(env, log, 4 * 7 * 24 * 60)  # four weeks, SimPy's run length

"""The SimPy programs of the Batch 11 design tests (computed sets and events: Y1, Y2
and Y3). Each returns the marks the blueprint publishes, in publication order, as
(time, key, label, extra), the same form as `batch8`. `n` entities start at one
instant, with the keys `a-1` to `a-n` and the attribute `i` from 0 to n - 1."""

from typing import Any, Callable, List, Tuple

import simpy

Marks = List[Tuple[Any, ...]]


def round_robin(n: int, members: int = 3) -> Marks:
    """Y1: `members` resources of one unit, made from a number when the run starts; the
    entity `i` asks for the one at `i % members` and serves 2 s, and reads the users
    and the queue of the first at the end of its service."""
    env = simpy.Environment()
    marks: Marks = []
    rs = [simpy.Resource(env, capacity=1) for _ in range(members)]

    def job(i):
        with rs[i % members].request() as request:
            yield request
            yield env.timeout(2)
            marks.append(
                (env.now, f"a-{i + 1}", "done", [len(rs[0].users), len(rs[0].queue)])
            )

    for i in range(n):
        env.process(job(i))
    env.run()
    return marks


def stores(n: int) -> Marks:
    """Y1: a store for each key `i % 2`, made at the first put, at time 3. The entity
    `i` waits 3 s, puts `i` into its store, waits 10 s and gets from the same store, so
    the stores give their items in the order they were put."""
    env = simpy.Environment()
    marks: Marks = []
    stores_: dict = {}

    def job(i):
        yield env.timeout(3)
        stores_.setdefault(i % 2, simpy.Store(env)).put(i)
        yield env.timeout(10)
        item = yield stores_.setdefault(i % 2, simpy.Store(env)).get()
        marks.append((env.now, f"a-{i + 1}", "got", [item, sorted(stores_)]))

    for i in range(n):
        env.process(job(i))
    env.run()
    return marks


def containers(n: int) -> Marks:
    """Y1: a container for each key `i % 2` that starts with a level of `10 * (i + 1)`
    when it is made; the entity takes 4 from its container and reads the level."""
    env = simpy.Environment()
    marks: Marks = []
    found: dict = {}

    def job(i):
        c = found.setdefault(
            i % 2, simpy.Container(env, capacity=1000, init=10 * (i + 1))
        )
        yield c.get(4)
        marks.append((env.now, f"a-{i + 1}", "took", [c.level, sorted(found)]))

    for i in range(n):
        env.process(job(i))
    env.run()
    return marks


def signals(n: int) -> Marks:
    """Y1: a signal for each key. The entity `i` waits for the signal of its key from
    the start; a second set of flows fires the signal of each key at time 1 with
    `{"v": i}`, and a flow that waits after the firing resumes at once."""
    env = simpy.Environment()
    marks: Marks = []
    evs: dict = {}

    def waiter(i):
        value = yield evs.setdefault(i, env.event())
        marks.append((env.now, f"a-{i + 1}", "heard", value["v"]))

    def late(i):
        yield env.timeout(2)
        value = yield evs.setdefault(i, env.event())
        marks.append((env.now, f"a-{i + 1}", "late", value["v"]))

    def firer(i):
        yield env.timeout(1)
        evs.setdefault(i, env.event()).succeed({"v": i})

    for i in range(n):
        env.process(waiter(i))
    for i in range(n):
        env.process(firer(i))
    for i in range(n):
        env.process(late(i))
    env.run()
    return marks


def lowest(n: int) -> Marks:
    """Y2: two resources of one unit. The entity `i` asks for the one with the fewest
    users and waiting requests, the first when equal, and serves 2 s."""
    env = simpy.Environment()
    marks: Marks = []
    rs = [simpy.Resource(env, capacity=1) for _ in range(2)]

    def job(i):
        k = min(range(2), key=lambda k: len(rs[k].queue) + rs[k].count)
        with rs[k].request() as request:
            yield request
            yield env.timeout(2)
            marks.append((env.now, f"a-{i + 1}", "done", k))

    for i in range(n):
        env.process(job(i))
    env.run()
    return marks


def by_name(n: int) -> Marks:
    """Y2: the entity `i` asks for `left` or `right` by the text of its `side`."""
    env = simpy.Environment()
    marks: Marks = []
    rs = {
        "left": simpy.Resource(env, capacity=1),
        "right": simpy.Resource(env, capacity=1),
    }

    def job(i):
        side = "left" if i % 2 == 0 else "right"
        with rs[side].request() as request:
            yield request
            yield env.timeout(2)
            marks.append((env.now, f"a-{i + 1}", "done", side))

    for i in range(n):
        env.process(job(i))
    env.run()
    return marks


def _at_least(count: int) -> Callable[[Any, List[Any]], Any]:
    return lambda env, evs: simpy.events.Condition(env, lambda e, c: c >= count, evs)


GROUPS = {
    "all": lambda env, evs: env.all_of(evs),
    "any": lambda env, evs: env.any_of(evs),
    "at_least_2": _at_least(2),
}


def each_timeouts(n: int, delays: List[int], group: str) -> Marks:
    """Y3: each entity waits for a timeout for each delay, under `group`; the value of
    each is the delay times 10. It reads the values in element order, with None for an
    event not done, and the flags."""
    env = simpy.Environment()
    marks: Marks = []

    def job(i):
        evs = [env.timeout(d, value=d * 10) for d in delays]
        res = yield GROUPS[group](env, evs)
        marks.append(
            (
                env.now,
                f"a-{i + 1}",
                "done",
                [[res[e] if e in res else None for e in evs], [e in res for e in evs]],
            )
        )

    for i in range(n):
        env.process(job(i))
    env.run()
    return marks


def each_children(n: int, count: int) -> Marks:
    """Y3: each entity starts `count` child flows, the flow `j` taking `count - j` s and
    returning `j * 2`, and waits for all of them."""
    env = simpy.Environment()
    marks: Marks = []

    def child(j):
        yield env.timeout(count - j)
        return j * 2

    def job(i):
        ps = [env.process(child(j)) for j in range(count)]
        res = yield env.all_of(ps)
        marks.append((env.now, f"a-{i + 1}", "done", [res[p] for p in ps]))

    for i in range(n):
        env.process(job(i))
    env.run()
    return marks


def each_requests(n: int, count: int) -> Marks:
    """Y3: `count` resources, the unit of the resource `j` held until `3 - j` s from
    the start; each entity asks all of them at 0.5, waits for the first grant, keeps
    that unit and gives the others up."""
    env = simpy.Environment()
    marks: Marks = []
    rs = [simpy.Resource(env, capacity=1) for _ in range(count)]

    def holder(j):
        with rs[j].request() as request:
            yield request
            yield env.timeout(3 - j)

    def job(i):
        yield env.timeout(0.5)
        qs = [r.request() for r in rs]
        res = yield env.any_of(qs)
        got = [q in res for q in qs]
        for q in qs:
            if q not in res:
                q.cancel()
        marks.append((env.now, f"a-{i + 1}", "done", [got, [len(r.queue) for r in rs]]))
        for j, q in enumerate(qs):
            if q in res:
                rs[j].release(q)

    for j in range(count):
        env.process(holder(j))
    for i in range(n):
        env.process(job(i))
    env.run()
    return marks


def y9_waiters_fail(n: int) -> Marks:
    """Y9: `n` waiters of a one-shot event; a driver fails it at 1 with the reason
    `boom`, and each waiter catches the failure (the keys are `a-1` to `a-n`)."""
    env = simpy.Environment()
    marks: Marks = []
    ev = env.event()

    def waiter(i):
        try:
            yield ev
            marks.append((env.now, f"a-{i + 1}", "fine", None))
        except ValueError as error:
            marks.append((env.now, f"a-{i + 1}", "caught", str(error)))

    def driver():
        yield env.timeout(1)
        ev.fail(ValueError("boom"))

    for i in range(n):
        env.process(waiter(i))
    env.process(driver())
    env.run()
    return marks


def y9_computed_reason(n: int) -> Marks:
    """Y9: each of `n` processes raises an error whose text is read when it fails, at 1,
    and the process that waits for it catches it."""
    env = simpy.Environment()
    marks: Marks = []

    def failing(i):
        yield env.timeout(1)
        raise ValueError(f"bad-{i}")

    def parent(i):
        try:
            yield env.process(failing(i))
        except ValueError as error:
            marks.append((env.now, f"a-{i + 1}", "caught", str(error)))

    for i in range(n):
        env.process(parent(i))
    env.run()
    return marks


def y9_unhandled(defused: bool) -> Tuple[Marks, Any]:
    """Y9: a one-shot event fails at 1 and no process waits for it. `run()` raises the
    error unless the event is defused. Returns the marks (none) and the error."""
    env = simpy.Environment()
    ev = env.event()

    def driver():
        yield env.timeout(1)
        if defused:
            ev.defused = True
        ev.fail(ValueError("boom"))

    env.process(driver())
    try:
        env.run()
    except ValueError as error:
        return [], error
    return [], None


def y10_priorities(n: int) -> Marks:
    """Y10: three events scheduled at 1 with the priorities 2 (`a`), 1 (`b`) and 0
    (`c`), in that order; the waiters of `c` run first, then `b`, then `a`. `n` waiters
    for each, with the keys `wa-1` to `wa-n`, `wb-1` to `wb-n` and `wc-1` to `wc-n`."""
    env = simpy.Environment()
    marks: Marks = []
    events = {name: env.event() for name in "abc"}

    def waiter(name, i):
        yield events[name]
        marks.append((env.now, f"w{name}-{i + 1}", "heard", None))

    def driver():
        yield env.timeout(1)
        for name, priority in (("a", 2), ("b", 1), ("c", 0)):
            event = events[name]
            event._ok, event._value = True, None
            env.schedule(event, priority)

    for name in "abc":
        for i in range(n):
            env.process(waiter(name, i))
    env.process(driver())
    env.run()
    return marks


def y11_listen(n: int) -> Marks:
    """Y11: each of `n` processes adds a callback to one event, then waits for it, at
    0. A callback counts when its process has an even index. The event succeeds at 2;
    each process, in order, reads the count when it resumes, and reads it again at 3."""
    env = simpy.Environment()
    marks: Marks = []
    go = env.event()
    count = [0]

    def job(i):
        def callback(_event):
            if i % 2 == 0:
                count[0] += 1

        go.callbacks.append(callback)
        yield go
        marks.append((env.now, f"a-{i + 1}", "woke", count[0]))
        yield env.timeout(1)
        marks.append((env.now, f"a-{i + 1}", "report", count[0]))

    def driver():
        yield env.timeout(2)
        go.succeed()

    for i in range(n):
        env.process(job(i))
    env.process(driver())
    env.run()
    return marks

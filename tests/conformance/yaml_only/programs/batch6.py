"""The SimPy programs of the Batch 6 known cases (names, lists, local values and
sources), each written from the blueprint of the same case in `test_yaml_only.py`
(independence class: written from the blueprint). Each returns the marks the blueprint
publishes, in publication order, as (time, key, label, extra)."""

from typing import Any, Dict, List, Optional, Tuple

import simpy

Marks = List[Tuple[Any, ...]]


def v7_cause(n: int) -> Marks:
    """V7: n low jobs (priority 5, 10 s) share one preemptive unit; a high job
    (priority 1, 2 s) arrives at 3 and preempts the holder, which reads who preempted
    it, since when it held the unit, the time left and who holds the unit then."""
    env = simpy.Environment()
    unit = simpy.PreemptiveResource(env, 1)
    marks: Marks = []
    owner: Dict[Any, str] = {}

    def low(key: str):
        with unit.request(priority=5) as request:
            owner[request] = key
            yield request
            began = env.now
            try:
                yield env.timeout(10)
                marks.append((env.now, key, "low_done", None))
            except simpy.Interrupt as interrupt:
                cause = interrupt.cause
                marks.append(
                    (
                        env.now,
                        key,
                        "cause",
                        [
                            owner[cause.resource.users[0]],
                            cause.usage_since,
                            10 - (env.now - began),
                            [owner[r] for r in unit.users],
                        ],
                    )
                )

    def high(key: str):
        yield env.timeout(3)
        with unit.request(priority=1) as request:
            owner[request] = key
            yield request
            yield env.timeout(2)
            marks.append((env.now, key, "high_done", None))

    for index in range(1, n + 1):
        env.process(low(f"low-{index}"))
    env.process(high("high-1"))
    env.run(until=100)
    return marks


def v9_args(n: int) -> Marks:
    """V9: each parent starts two children with 3 * k and 3 * k + 1 and waits for both;
    a child ends after 1 with its argument plus one."""
    env = simpy.Environment()
    marks: Marks = []

    def child(key: str, x: int):
        yield env.timeout(1)
        marks.append((env.now, key, "ended", x + 1))

    def parent(index: int):
        key = f"a-{index}"
        first = env.process(child(key, index * 3))
        second = env.process(child(key, index * 3 + 1))
        yield first & second
        marks.append((env.now, key, "parent", None))

    for index in range(1, n + 1):
        env.process(parent(index))
    env.run(until=50)
    return marks


def v11_when(n: int, done: int, before: float) -> Marks:
    """V11: a condition over a timeout of 1 and one of 3 that holds when `done`
    events have been processed and the time is below `before`."""
    env = simpy.Environment()
    marks: Marks = []

    def gate(key: str):
        events = [env.timeout(1), env.timeout(3)]
        yield simpy.events.Condition(
            env, lambda evts, count: count == done and env.now < before, events
        )
        marks.append((env.now, key, "opened", env.now))

    for index in range(1, n + 1):
        env.process(gate(f"a-{index}"))
    env.run(until=10)
    return marks


def v12_filter_put(n: int) -> Marks:
    """V12: entity i takes the item n - i from a filter store of 0 to n - 1, puts back
    the item plus 100; at 1 the sorted items of the store are read."""
    env = simpy.Environment()
    store = simpy.FilterStore(env)
    store.items.extend(range(n))
    marks: Marks = []

    def entity(index: int):
        key = f"p-{index}"
        want = n - index
        item = yield store.get(lambda value: value == want)
        marks.append((env.now, key, "took", item))
        yield store.put(item + 100)
        marks.append((env.now, key, "back", item + 100))

    def look():
        yield env.timeout(1)
        marks.append((env.now, "r-1", "items", sorted(store.items)))

    for index in range(1, n + 1):
        env.process(entity(index))
    env.process(look())
    env.run(until=5)
    return marks


def y6_local(n: int) -> Marks:
    """Y6: each parent starts two counting children, with a gap of 1 and of 1.5, each
    with its own count from 0 that ends at 2, and waits for both."""
    env = simpy.Environment()
    marks: Marks = []

    def counting(key: str, gap: float):
        i = 0
        marks.append((env.now, key, "begin", gap))
        while True:
            yield env.timeout(gap)
            marks.append((env.now, key, "step", i + 1))
            i += 1
            if not i < 2:
                return

    def parent(index: int):
        key = f"a-{index}"
        first = env.process(counting(key, 1))
        second = env.process(counting(key, 1.5))
        yield first & second
        marks.append((env.now, key, "parent", None))

    for index in range(1, n + 1):
        env.process(parent(index))
    env.run(until=50)
    return marks


def y7_state(n: int) -> Marks:
    """Y7: n workers use one unit for 5 each from 0; at 1 a report reads how many
    workers run, who waits, how many hold the unit, how many running workers are of
    class 1, and the time of the next event."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    marks: Marks = []
    keys: Dict[Any, str] = {}
    running: List[int] = []

    def worker(index: int):
        running.append(index)
        with unit.request() as request:
            keys[request] = f"w-{index}"
            yield request
            yield env.timeout(5)
        running.remove(index)

    def report():
        yield env.timeout(1)
        marks.append(
            (
                env.now,
                "r-1",
                "state",
                [
                    len(running),
                    [keys[r] for r in unit.queue],
                    len(unit.users),
                    len([i for i in running if i % 2 == 1]),
                    env.peek(),
                ],
            )
        )

    for index in range(1, n + 1):
        env.process(worker(index))
    env.process(report())
    env.run(until=50)
    return marks


def a3_group(n: int) -> Marks:
    """A3: every 5 a source draws the group size n * (floor(t / 5) - 1) and starts
    that many members; a size of 0 starts nobody and uses no key number."""
    env = simpy.Environment()
    marks: Marks = []
    number = [0]

    def member(key: str):
        marks.append((env.now, key, "member", None))
        yield env.timeout(0)

    def source():
        while True:
            yield env.timeout(5)
            for _ in range(max(0, n * (int(env.now / 5) - 1))):
                number[0] += 1
                env.process(member(f"g-{number[0]}"))

    env.process(source())
    env.run(until=22)
    return marks


def a5_drop(n: int) -> Marks:
    """A5 drop: n sources with a gap of 1 are switched off at 2.5 and on at 5.5; an
    arrival that falls while a source is off starts nobody."""
    env = simpy.Environment()
    marks: Marks = []
    state = {"on": True}

    def source(name: str):
        number = 0
        while True:
            yield env.timeout(1)
            if state["on"]:
                number += 1
                marks.append((env.now, f"{name}-{number}", "t", None))

    def switch(at: float, on: bool):
        yield env.timeout(at)
        state["on"] = on

    for index in range(1, n + 1):
        env.process(source(f"s{index}"))
    env.process(switch(2.5, False))
    env.process(switch(5.5, True))
    env.run(until=7.5)
    return marks


def a5_restart(n: int) -> Marks:
    """A5 restart: n sources with a gap of 2 are switched off at 3 and on at 7; going
    off drops the gap that was running and going on starts a whole new gap."""
    env = simpy.Environment()
    marks: Marks = []
    wake = {"event": env.event()}

    def source(name: str):
        number = 0
        while True:
            try:
                yield env.timeout(2)
            except simpy.Interrupt:
                yield wake["event"]
                continue
            number += 1
            marks.append((env.now, f"{name}-{number}", "t", None))

    processes = [env.process(source(f"s{index}")) for index in range(1, n + 1)]

    def switch():
        yield env.timeout(3)
        for process in processes:
            process.interrupt()
        yield env.timeout(4)
        wake["event"].succeed()

    env.process(switch())
    env.run(until=14)
    return marks


def a7_spawn(n: int) -> Marks:
    """A7: a source starts n parents at 1, in one step. Each parent publishes and then
    starts two children that copy its attributes and read the tag and the parent. The
    parents take their first steps in the order they were started, and the children
    after them, in creation order."""
    env = simpy.Environment()
    marks: Marks = []
    counter = [0]

    def child(key: str, attributes: Dict[str, Any]):
        marks.append((env.now, key, "clone", [attributes["tag"], attributes["parent"]]))
        yield env.timeout(0)

    def parent(index: int):
        key = f"p-{index}"
        attributes: Dict[str, Any] = {"count": 2, "tag": f"t{index}"}
        marks.append((env.now, key, "parent", None))
        for _ in range(attributes["count"]):
            counter[0] += 1
            copy = dict(attributes, parent=key)
            env.process(child(f"clone-{counter[0]}", copy))
        yield env.timeout(0)

    def source():
        yield env.timeout(1)
        for index in range(1, n + 1):
            env.process(parent(index))

    env.process(source())
    env.run(until=5)
    return marks


def m4_initial(n: int) -> Marks:
    """M4: n cars exist before the run starts and a source starts one more each 1;
    a control process that is started by a source at 0 takes its step after them."""
    env = simpy.Environment()
    marks: Marks = []
    number = [0]

    def car(key: str):
        marks.append((env.now, key, "car", None))
        yield env.timeout(0)

    def car_source():
        while True:
            yield env.timeout(1)
            number[0] += 1
            env.process(car(f"cars-{number[0]}"))

    def control_source():
        env.process(car("ctl-1"))
        yield env.timeout(100)

    for _ in range(n):
        number[0] += 1
        env.process(car(f"cars-{number[0]}"))
    env.process(car_source())
    env.process(control_source())
    env.run(until=2.5)
    return marks


def m6_flow_name(n: int) -> Marks:
    """M6: a process that runs a second step in a function of its own reads the name
    of the first function it ran, which is the process name of SimPy."""
    env = simpy.Environment()
    marks: Marks = []

    def second(key: str):
        marks.append((env.now, key, "second", env.active_process.name))
        yield env.timeout(1)

    def first(key: str):
        yield env.timeout(1)
        yield from second(key)

    for index in range(1, n + 1):
        env.process(first(f"w-{index}"))
    env.run()
    return marks


def users_and_queue(until: Tuple[int, ...]) -> List[Tuple[List[str], List[str]]]:
    """ST-R11 (SimPy's test_get_users): three processes use one unit for 1 each; the
    users and the queue are read after run(until=1) and after run(until=2)."""
    env = simpy.Environment()
    res = simpy.Resource(env, 1)
    owner: Dict[Any, str] = {}

    def pem(key: str):
        with res.request() as request:
            owner[request] = key
            yield request
            yield env.timeout(1)

    for index in range(1, 4):
        env.process(pem(f"users-{index}"))
    readings = []
    for stop in until:
        env.run(until=stop)
        readings.append(([owner[r] for r in res.users], [owner[r] for r in res.queue]))
    return readings


def container_queues(until: Tuple[int, ...]) -> List[Tuple[List[str], List[str]]]:
    """ST-R17 (SimPy's test_container_get_queued): a container of capacity 1; one get
    waits and three puts, each after 1, wait; the put queue and the get queue are read
    after run(until=1) and after run(until=2)."""
    env = simpy.Environment()
    box = simpy.Container(env, 1, init=0)
    owner: Dict[Any, str] = {}

    def getter(key: str):
        event = box.get(1)
        owner[event] = key
        yield event

    def putter(key: str):
        yield env.timeout(1)
        event = box.put(1)
        owner[event] = key
        yield event

    env.process(getter("g-1"))
    for index in range(1, 4):
        env.process(putter(f"p-{index}"))
    readings = []
    for stop in until:
        env.run(until=stop)
        readings.append(
            ([owner[e] for e in box.put_queue], [owner[e] for e in box.get_queue])
        )
    return readings


def gd_e3_steps(until: float) -> Tuple[List[Dict[str, Any]], Optional[Marks]]:
    """GD-E3: a process that ticks every 1, driven by a loop of `env.peek()` and
    `env.step()` up to `until`; one reading `{step, now, peek}` after each step."""
    env = simpy.Environment()

    def ticking():
        while True:
            yield env.timeout(1)

    env.process(ticking())
    readings: List[Dict[str, Any]] = []
    while env.peek() < until:
        env.step()
        readings.append({"step": len(readings) + 1, "now": env.now, "peek": env.peek()})
    return readings, None


def v9_spawn_args(n: int) -> Marks:
    """V9 on a spawn: each parent starts two children at 1, and each child is given the
    next number of one counter as its argument; a child ends after 1 with it plus one."""
    env = simpy.Environment()
    marks: Marks = []
    given = [0]
    kids = [0]

    def child(key: str, x: int):
        yield env.timeout(1)
        marks.append((env.now, key, "kid", x + 1))

    def parent(index: int):
        yield env.timeout(1)
        for _ in range(2):
            given[0] += 1
            kids[0] += 1
            env.process(child(f"kid-{kids[0]}", given[0]))

    for index in range(1, n + 1):
        env.process(parent(index))
    env.run(until=50)
    return marks


def v7_tie(n: int) -> Marks:
    """V7, Tie: n low jobs and a high job all arrive at 1 on one preemptive unit. The
    first low job is granted the unit and preempted at once, before it starts to work,
    so the time it has left is its whole time."""
    env = simpy.Environment()
    unit = simpy.PreemptiveResource(env, 1)
    marks: Marks = []
    owner: Dict[Any, str] = {}
    label: Dict[Any, str] = {}

    def low(key: str):
        yield env.timeout(1)
        with unit.request(priority=5) as request:
            owner[request] = key
            began = None
            try:
                yield request
                began = env.now
                yield env.timeout(10)
            except simpy.Interrupt as interrupt:
                left = 10 if began is None else 10 - (env.now - began)
                marks.append((env.now, key, "cause", [label[interrupt.cause.by], left]))

    def high(key: str):
        yield env.timeout(1)
        with unit.request(priority=1) as request:
            owner[request] = key
            yield request
            yield env.timeout(2)

    for index in range(1, n + 1):
        env.process(low(f"low-{index}"))
    label[env.process(high("high-1"))] = "high-1"
    env.run(until=100)
    return marks


def v7_wait_left(n: int) -> Marks:
    """V7, a single `wait: {timeout}`: n processes wait 10 and are interrupted at 1;
    each reads the time left of its wait."""
    env = simpy.Environment()
    marks: Marks = []

    def waiter(key: str):
        began = env.now
        try:
            yield env.timeout(10)
        except simpy.Interrupt:
            marks.append((env.now, key, "cause", 10 - (env.now - began)))

    processes = [env.process(waiter(f"w-{index}")) for index in range(1, n + 1)]

    def poke():
        yield env.timeout(1)
        for process in processes:
            process.interrupt("hi")

    env.process(poke())
    env.run(until=30)
    return marks


def y5_forms(n: int) -> Marks:
    """Y5: the list, mapping and function forms, on n entities with k = 1 to n."""
    env = simpy.Environment()
    marks: Marks = []

    def entity(k: int):
        yield env.timeout(0)
        values = [
            sum([a * 2 for a in [1, 2, 3, k] if a > 1]),
            sorted([k, 5, 1])[::-1],
            len(list(range(k))),
            {**{"k": k}, "z": [k, 2]}["z"][1],  # type: ignore[index]
            {"a": 1}.get("b", k),
            max([k, 2]),
            2 in [k, 2],
            k is not None,
            [v for v in [k, k + 1]][-1],
        ]
        marks.append((env.now, f"a-{k}", "forms", values))

    for k in range(1, n + 1):
        env.process(entity(k))
    env.run(until=5)
    return marks


def p37_preemption(mode: str) -> List[Tuple[float, int]]:
    """P37: ten jobs in pairs at 0, 2, 4, 6 and 8 (the first pair before the run, each later pair started by one source in one step), priority 1 when i % 3 == 0 and 5
    otherwise, each served 1.5 on one preemptive unit; `resume` serves the time left
    after a preemption and `restart` serves the whole time again. Returns the (time, i)
    of each job's end."""
    env = simpy.Environment()
    unit = simpy.PreemptiveResource(env, 1)
    ends: List[Tuple[float, int]] = []

    def job(i: int):
        left = 1.5
        while left > 0:
            with unit.request(priority=1 if i % 3 == 0 else 5) as request:
                yield request
                began = env.now
                try:
                    yield env.timeout(left)
                    left = 0
                except simpy.Interrupt:
                    left = left - (env.now - began) if mode == "resume" else 1.5
        ends.append((env.now, i))

    # The first pair exists before the run, as `initial` makes it; a source starts each
    # later pair in one step, as the rows of a replay at one time start
    env.process(job(0))
    env.process(job(1))

    def source():
        for start in (2, 4, 6, 8):
            yield env.timeout(start - env.now)
            env.process(job(start))
            env.process(job(start + 1))

    env.process(source())
    env.run(until=100)
    return ends

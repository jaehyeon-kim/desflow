"""The SimPy programs of the Batch 8 known cases (holding resources), each written
from the blueprint of the same case in `test_yaml_only.py` (independence class:
written from the blueprint). Each returns the marks the blueprint publishes, in
publication order, as (time, key, label, extra). `n` entities start at one instant,
in file order, and the programs start their processes in that order."""

import itertools
from typing import Any, Dict, List, Optional, Tuple

import simpy
from simpy.core import BoundClass
from simpy.resources.container import ContainerGet
from simpy.resources.resource import (
    Preempted,
    PriorityRequest,
    Request,
    SortedQueue,
)
from simpy.resources.store import StoreGet, StorePut

Marks = List[Tuple[Any, ...]]


def h1_held_across_steps(n: int, cut_at: Optional[float] = None) -> Marks:
    """H1: n jobs each take the unit with priority 5, keep it through two timed steps
    (2 s, then 3 s) and give it back; a waiter of priority 9 that does not preempt
    asks at the same instant. A preempter (priority 1, 1 s) arrives at `cut_at`: the
    job that held the unit asks again under the same request name at priority 5 and
    goes on with the time it had left."""
    env = simpy.Environment()
    unit = simpy.PreemptiveResource(env, 1)
    marks: Marks = []

    def job(key: str):
        request = unit.request(priority=5)
        yield request
        marks.append((env.now, key, "got", None))
        for label, span in (("a_done", 2), ("b_done", 3)):
            left = span
            while True:
                began = env.now
                try:
                    yield env.timeout(left)
                    break
                except simpy.Interrupt:
                    left -= env.now - began
                    request = unit.request(priority=5)
                    yield request
            marks.append((env.now, key, label, None))
        unit.release(request)

    def waiter(key: str):
        with unit.request(priority=9, preempt=False) as request:
            yield request
            marks.append((env.now, key, "waiter_got", None))

    def cutter(key: str):
        yield env.timeout(cut_at)
        with unit.request(priority=1) as request:
            yield request
            yield env.timeout(1)
            marks.append((env.now, key, "cutter_done", None))

    for index in range(1, n + 1):
        env.process(job(f"job-{index}"))
    env.process(waiter("other-1"))
    if cut_at is not None:
        env.process(cutter("cut-1"))
    env.run(until=200)
    return marks


def h5_opposite_orders(n: int, cap: int, at: float) -> Marks:
    """H5: n jobs take r1 then r2 and n jobs take r2 then r1, each holding its first
    unit for 1 s before it asks for the second. The first group starts at 0 and the
    second at `at`. With one unit each and a tie at 0, the two sides wait for each
    other for ever and the run ends with no error."""
    env = simpy.Environment()
    r1, r2 = simpy.Resource(env, cap), simpy.Resource(env, cap)
    marks: Marks = []

    def job(key: str, first: simpy.Resource, second: simpy.Resource, start: float):
        yield env.timeout(start)
        a = first.request()
        yield a
        yield env.timeout(1)
        marks.append((env.now, key, "got_first", None))
        b = second.request()
        yield b
        yield env.timeout(1)
        marks.append((env.now, key, "both", None))
        second.release(b)
        marks.append((env.now, key, "done", None))
        first.release(a)

    for index in range(1, n + 1):
        env.process(job(f"first-{index}", r1, r2, 0))
    for index in range(1, n + 1):
        env.process(job(f"second-{index}", r2, r1, at))
    env.run(until=200)
    return marks


def h6_release_of_another_holder(n: int) -> Marks:
    """H6: n holders take the one unit for 5 s each, and n waiters ask for it for
    10 s. A third entity frees the unit of the first holder at 1; the holder is not
    told, and its own release at 5 changes nothing."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    marks: Marks = []
    held: Dict[str, Any] = {}

    def holder(key: str):
        with unit.request() as request:
            held[key] = request
            yield request
            yield env.timeout(5)
            marks.append((env.now, key, "held", None))

    def waiter(key: str):
        with unit.request() as request:
            yield request
            yield env.timeout(10)
            marks.append((env.now, key, "served", None))

    def third(key: str):
        yield env.timeout(1)
        marks.append((env.now, key, "freed", None))
        unit.release(held["holder-1"])

    for index in range(1, n + 1):
        env.process(holder(f"holder-{index}"))
    for index in range(1, n + 1):
        env.process(waiter(f"waiter-{index}"))
    env.process(third("third-1"))
    env.run(until=500)
    return marks


def h9_cancel_after_the_grant(n: int) -> Marks:
    """H9: n jobs open a request, wait for it and cancel it after the grant, which
    does nothing: the unit stays held for 3 s until the release. A later job asks for
    the unit at 1."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    marks: Marks = []

    def first(key: str):
        slot = unit.request()
        yield slot
        marks.append((env.now, key, "opened", None))
        slot.cancel()
        marks.append((env.now, key, "cancelled_after_grant", None))
        yield env.timeout(3)
        marks.append((env.now, key, "still_held", None))
        unit.release(slot)

    def second(key: str):
        yield env.timeout(1)
        with unit.request() as request:
            yield request
            yield env.timeout(1)
            marks.append((env.now, key, "got", None))

    for index in range(1, n + 1):
        env.process(first(f"a-{index}"))
    env.process(second("b-1"))
    env.run(until=500)
    return marks


def i10_wait_again_after_an_interrupt(n: int) -> Marks:
    """I10: n flows wait for a timeout of 5 s made before the wait; an interrupt at 2
    reaches each, and each waits for the same timeout again, so it ends at 5."""
    env = simpy.Environment()
    marks: Marks = []
    processes: List[Any] = []

    def waiter(key: str):
        limit = env.timeout(5)
        while True:
            try:
                yield limit
                break
            except simpy.Interrupt:
                marks.append((env.now, key, "interrupted", None))
        marks.append((env.now, key, "ended", None))

    def poke(key: str):
        yield env.timeout(2)
        marks.append((env.now, key, "poke", None))
        for process in processes:
            if process.is_alive:
                process.interrupt("x")

    for index in range(1, n + 1):
        processes.append(env.process(waiter(f"a-{index}")))
    env.process(poke("b-1"))
    env.run(until=100)
    return marks


def m7_whole_flow(n: int, only_first: bool) -> Marks:
    """M7: n flows of two tasks of 3 s each, one after the other. A watcher waits for
    the whole flow of the first instance, or for every flow, and reads the time."""
    env = simpy.Environment()
    marks: Marks = []

    def relay(key: str):
        yield env.timeout(3)
        marks.append((env.now, key, "relay", None))
        yield env.timeout(3)
        marks.append((env.now, key, "more", None))

    flows = [env.process(relay(f"a-{index}")) for index in range(1, n + 1)]

    def watch(key: str):
        yield env.all_of(flows[:1] if only_first else flows)
        marks.append((env.now, key, "flow_done", env.now))

    env.process(watch("w-1"))
    env.run(until=100)
    return marks


def t10_wait_again(n: int) -> Marks:
    """T10: n flows make a timeout of 1 s and a timeout of 5 s that carries a value,
    wait for either, and then wait for the second again."""
    env = simpy.Environment()
    marks: Marks = []

    def flow(key: str):
        first = env.timeout(1)
        later = env.timeout(5, value="late")
        yield first | later
        marks.append((env.now, key, "first", later.triggered))
        value = yield later
        marks.append((env.now, key, "again", value))

    for index in range(1, n + 1):
        env.process(flow(f"a-{index}"))
    env.run(until=100)
    return marks


def _holder(
    env: simpy.Environment,
    unit: simpy.Resource,
    marks: Marks,
    key: str,
    label: str,
    span: float,
):
    """A flow with a plain `resource` that holds the unit for `span` s and publishes."""
    with unit.request() as request:
        yield request
        yield env.timeout(span)
        marks.append((env.now, key, label, None))


def unit_grant(n: int) -> Marks:
    """Unit states, Grant: a holder has each unit until 10. n flows seize one and keep
    going when it is granted; n flows open a request on the other, spend 12 s on a
    timed step, and read whether the request was granted, whatever they were doing."""
    env = simpy.Environment()
    m, k = simpy.Resource(env, 1), simpy.Resource(env, 1)
    marks: Marks = []

    def seizer(key: str):
        request = m.request()
        yield request
        marks.append((env.now, key, "got", None))
        yield env.timeout(2)
        marks.append((env.now, key, "s_done", None))
        m.release(request)

    def opener(key: str):
        request = k.request()
        yield env.timeout(12)
        marks.append((env.now, key, "after", request.triggered))
        k.release(request)

    env.process(_holder(env, m, marks, "hm-1", "held_m", 10))
    env.process(_holder(env, k, marks, "hk-1", "held_k", 10))
    for index in range(1, n + 1):
        env.process(seizer(f"s-{index}"))
    for index in range(1, n + 1):
        env.process(opener(f"h-{index}"))
    env.run(until=500)
    return marks


def unit_patience(n: int, part: str) -> Marks:
    """Unit states, Patience end. `seize`: n flows seize a unit that a holder has until
    10 with a patience of 2, give up at 2, and a flow that asks at 0.5 gets the unit
    at 10. `handle`: n flows open a request, wait for it with a patience of 2 and give
    up, but the request stays and is granted later. `held`: a flow that holds a unit
    gives up waiting for another at 2 and keeps the first."""
    env = simpy.Environment()
    m, other = simpy.Resource(env, 1), simpy.Resource(env, 1)
    marks: Marks = []

    def reading(key: str, resource: simpy.Resource):
        yield env.timeout(30)
        marks.append((env.now, key, "end", [len(resource.users), len(resource.queue)]))

    def seizer(key: str):
        request = m.request()
        got = yield request | env.timeout(2)
        if request not in got:
            request.cancel()
            marks.append((env.now, key, "reneged", None))

    def behind(key: str):
        yield env.timeout(0.5)
        with m.request() as request:
            yield request
            yield env.timeout(1)
            marks.append((env.now, key, "behind", None))

    def opener(key: str):
        request = m.request()
        got = yield request | env.timeout(2)
        if request not in got:
            marks.append((env.now, key, "reneged", None))

    def holder_of_two(key: str):
        first = m.request()
        yield first
        marks.append((env.now, key, "seized", None))
        second = other.request()
        got = yield second | env.timeout(2)
        if second not in got:
            second.cancel()
            marks.append((env.now, key, "reneged", None))

    if part == "held":
        env.process(_holder(env, other, marks, "blocker-1", "blocked", 9))
        for index in range(1, n + 1):
            env.process(holder_of_two(f"s-{index}"))
    else:
        env.process(_holder(env, m, marks, "holder-1", "held", 10))
        for index in range(1, n + 1):
            env.process((seizer if part == "seize" else opener)(f"s-{index}"))
        if part == "seize":
            env.process(behind("behind-1"))
    env.process(reading("probe-1", m))
    env.run(until=500)
    return marks


def unit_preemption(n: int, policy: str) -> Marks:
    """Unit states, Preemption: n flows seize the one preemptive unit with priority 1
    and spend 5 s on a timed step. A flow with priority 2 seizes at 0.5, a flow
    with an opened request (priority 2) looks at it at 2, and a priority 0 request
    takes the unit at 1 for 2 s. The flow that lost the unit asks again at priority 1,
    under the same name, and goes on with its time left (`resume`) or starts the step
    again (`restart`); under `abandon` its task ends, and the release in the task
    that follows frees nothing."""
    env = simpy.Environment()
    unit = simpy.PreemptiveResource(env, 1)
    marks: Marks = []

    def victim(key: str):
        request = unit.request(priority=1)
        yield request
        marks.append((env.now, key, "taken", None))
        left = 5
        while True:
            began = env.now
            try:
                yield env.timeout(left)
                break
            except simpy.Interrupt:
                if policy == "abandon":
                    marks.append((env.now, key, "cleanup", None))
                    unit.release(request)
                    return
                left = 5 if policy == "restart" else left - (env.now - began)
                request = unit.request(priority=1)
                yield request
        marks.append((env.now, key, "worked", None))
        unit.release(request)

    def queued(key: str):
        yield env.timeout(0.5)
        request = unit.request(priority=2)
        yield request
        marks.append((env.now, key, "queued_got", None))
        unit.release(request)

    def looker(key: str):
        yield env.timeout(0.5)
        request = unit.request(priority=2)
        yield env.timeout(1.5)
        marks.append((env.now, key, "looked", request.triggered))
        request.cancel()

    def high(key: str):
        yield env.timeout(1)
        with unit.request(priority=0) as request:
            yield request
            yield env.timeout(2)
            marks.append((env.now, key, "high_done", None))

    def reading(key: str):
        yield env.timeout(50)
        marks.append((env.now, key, "end", [len(unit.users), len(unit.queue)]))

    for index in range(1, n + 1):
        env.process(victim(f"v-{index}"))
    env.process(queued("q-1"))
    env.process(looker("l-1"))
    env.process(high("h-1"))
    env.process(reading("probe-1"))
    env.run(until=500)
    return marks


def unit_loss_in_any(n: int) -> Marks:
    """Unit states, Loss in `any`: n flows open a request for the one unit and wait for
    it or for a timeout of 1 s. The request that loses stays in the line and the one
    that wins stays held until its flow releases it; a release of a request still in
    the line does nothing, and the request is granted later."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    marks: Marks = []

    def flow(key: str):
        request = unit.request()
        done = yield request | env.timeout(1)
        marks.append(
            (
                env.now,
                key,
                "any_done",
                [request in done, len(unit.users), len(unit.queue)],
            )
        )
        yield env.timeout(2)
        marks.append(
            (
                env.now,
                key,
                "after",
                [request.triggered, len(unit.users), len(unit.queue)],
            )
        )
        unit.release(request)

    def reading(key: str):
        yield env.timeout(50)
        marks.append((env.now, key, "end", [len(unit.users), len(unit.queue)]))

    for index in range(1, n + 1):
        env.process(flow(f"a-{index}"))
    env.process(reading("probe-1"))
    env.run(until=500)
    return marks


def unit_cancel(n: int) -> Marks:
    """Unit states, `cancel`: a holder has the unit until 5. n flows open a request,
    spend 1 s, cancel it (it leaves the line), release it (nothing to release) and
    read the unit's state each time."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    marks: Marks = []

    def state() -> List[int]:
        return [len(unit.users), len(unit.queue)]

    def flow(key: str):
        request = unit.request()
        yield env.timeout(1)
        marks.append((env.now, key, "before_cancel", state()))
        request.cancel()
        marks.append((env.now, key, "cancelled", state()))
        unit.release(request)
        marks.append((env.now, key, "released", state()))

    env.process(_holder(env, unit, marks, "holder-1", "held", 5))
    for index in range(1, n + 1):
        env.process(flow(f"a-{index}"))
    env.run(until=500)
    return marks


def unit_cancel_twice(n: int) -> Marks:
    """Unit states, `cancel` of a request that left the line: SimPy raises
    `ValueError`; n flows each cancel their queued request twice."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    marks: Marks = []

    def flow(key: str):
        request = unit.request()
        yield env.timeout(1)
        request.cancel()
        marks.append((env.now, key, "cancelled", len(unit.queue)))
        try:
            request.cancel()
        except ValueError:
            marks.append((env.now, key, "again", "ValueError"))

    env.process(_holder(env, unit, marks, "holder-1", "held", 5))
    for index in range(1, n + 1):
        env.process(flow(f"a-{index}"))
    env.run(until=500)
    return marks


def unit_release(n: int) -> Marks:
    """Unit states, `release`: n flows open two requests for a resource of two units,
    wait for both, give the first back after 1 s and the second after 1 s more, and
    release the first again, which does nothing."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 2)
    marks: Marks = []

    def flow(key: str):
        a, b = unit.request(), unit.request()
        yield a & b
        marks.append((env.now, key, "both", unit.count))
        yield env.timeout(1)
        marks.append((env.now, key, "first", None))
        unit.release(a)
        marks.append((env.now, key, "after_a", [unit.count, b in unit.users]))
        yield env.timeout(1)
        marks.append((env.now, key, "second", None))
        unit.release(b)
        marks.append((env.now, key, "after_b", unit.count))
        marks.append((env.now, key, "third", None))
        unit.release(a)
        marks.append((env.now, key, "again", unit.count))

    for index in range(1, n + 1):
        env.process(flow(f"a-{index}"))
    env.run(until=500)
    return marks


def unit_task_and_flow_end(n: int, part: str) -> Marks:
    """Unit states, Task end and Flow end. `task`: n flows open a request for a unit a
    holder has until 10, finish their task, read in the next task that the request
    is still in the line, and wait for it there. `flow`: n flows open a request for
    that unit and end at once; the first request is granted at 10 and held for ever.
    `held`: n flows seize a unit and end; the unit stays held."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    marks: Marks = []

    def task_end(key: str):
        request = unit.request()
        yield env.timeout(2)
        marks.append((env.now, key, "te_first", None))
        marks.append((env.now, key, "te_next", request in unit.queue))
        yield request
        marks.append((env.now, key, "te_got", None))
        unit.release(request)

    def flow_end(key: str):
        unit.request()
        marks.append((env.now, key, "flow_ended", None))
        return
        yield

    def held_end(key: str):
        request = unit.request()
        yield request
        marks.append((env.now, key, "seized", None))

    def reading(key: str):
        yield env.timeout(50)
        marks.append((env.now, key, "end", [len(unit.users), len(unit.queue)]))

    if part != "held":
        env.process(_holder(env, unit, marks, "holder-1", "held", 10))
    body = {"task": task_end, "flow": flow_end, "held": held_end}[part]
    for index in range(1, n + 1):
        env.process(body(f"a-{index}"))
    env.process(reading("probe-1"))
    env.run(until=500)
    return marks


def h1_preempting_an_ended_flow() -> List[Any]:
    """H1: a process ends while it holds a unit of a preemptive resource; a request of
    a higher priority at 1 raises `RuntimeError` in the process that made it."""
    env = simpy.Environment()
    unit = simpy.PreemptiveResource(env, 1)
    caught: List[Any] = []

    def holder():
        yield unit.request(priority=5)

    def high():
        yield env.timeout(1)
        try:
            unit.request(priority=0)
        except RuntimeError as error:
            caught.append((env.now, type(error).__name__))

    env.process(holder())
    env.process(high())
    env.run()
    return caught


def y4_child_reference(n: int, interrupt: bool) -> Marks:
    """Y4: n flows start a child process, read that it is alive, spend 1 s, optionally
    interrupt it, and wait for its end and read its value, whether it is alive and
    whether it succeeded. The child returns after the interrupt."""
    env = simpy.Environment()
    marks: Marks = []

    def child(key: str):
        try:
            yield env.timeout(5)
            marks.append((env.now, key, "done", None))
            return "done"
        except simpy.Interrupt:
            marks.append((env.now, key, "interrupted", None))
            marks.append((env.now, key, "stopped_task", None))
            return "stopped"

    def parent(key: str):
        process = env.process(child(key))
        marks.append((env.now, key, "alive", process.is_alive))
        yield env.timeout(1)
        marks.append((env.now, key, "waited", None))
        if interrupt:
            process.interrupt("stop")
        value = yield process
        marks.append((env.now, key, "got", [value, process.is_alive, process.ok]))

    for index in range(1, n + 1):
        env.process(parent(f"a-{index}"))
    env.run(until=100)
    return marks


def y4_get_cancel(n: int) -> Marks:
    """Y4: n flows open a get on an empty store, spend 1 s, cancel it, spend 1 s more,
    read the number of getters, and put an item, which stays in the store; one flow
    opens a get on a store that has an item, so it is granted, cancels it after the
    grant (no effect) and reads the store, `ok` and the value; one flow opens a put
    on a full store, reads the putters, cancels it and reads them again."""
    env = simpy.Environment()
    empty = simpy.Store(env)
    full = simpy.Store(env, capacity=1)
    stocked = simpy.Store(env)
    full.put(1)
    stocked.put("x")
    marks: Marks = []

    def getter(key: str):
        request = empty.get()
        yield env.timeout(1)
        marks.append((env.now, key, "begun", None))
        request.cancel()
        yield env.timeout(1)
        marks.append((env.now, key, "cancelled", len(empty.get_queue)))
        yield empty.put("x")
        marks.append((env.now, key, "put_done", None))
        marks.append((env.now, key, "items", len(empty.items)))

    def granted(key: str):
        request = stocked.get()
        yield env.timeout(0)
        marks.append((env.now, key, "granted", None))
        request.cancel()
        marks.append(
            (env.now, key, "kept", [len(stocked.items), request.ok, request.value])
        )

    def putter(key: str):
        request = full.put(2)
        marks.append((env.now, key, "put_opened", len(full.put_queue)))
        request.cancel()
        marks.append((env.now, key, "put_cancelled", len(full.put_queue)))
        return
        yield

    for index in range(1, n + 1):
        env.process(getter(f"g-{index}"))
    env.process(granted("k-1"))
    env.process(putter("p-1"))
    env.run(until=100)
    return marks


def y4_scope_and_group() -> Marks:
    """Y4: a child reads an event its parent made (`handle`), a group of timeouts made
    now is waited for later and gives each member's value."""
    env = simpy.Environment()
    marks: Marks = []

    def child(key: str, event: simpy.Event):
        value = yield event
        marks.append((env.now, key, "child", value))

    def parent(key: str):
        event = env.timeout(2, value="v")
        yield env.process(child(key, event))

    def grouped(key: str):
        events = [env.timeout(1, value=1), env.timeout(3, value=3)]
        yield env.timeout(2)
        marks.append((env.now, key, "made", None))
        result = yield env.all_of(events)
        marks.append((env.now, key, "got", dict(zip("ab", result.values()))))

    env.process(parent("a-1"))
    env.process(grouped("g-1"))
    env.run(until=100)
    return marks


def y4_key_order(n: int) -> Marks:
    """Y4 key order: n flows open a request and then seize the same one-unit resource;
    a blocker holds it until 1. The opened request is ahead of the seize, so the
    seize waits until a third entity frees the opened unit at 2."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    marks: Marks = []
    opened: List[Any] = []

    def blocker(key: str):
        with unit.request() as request:
            yield request
            yield env.timeout(1)

    def flow(key: str):
        first = unit.request()
        opened.append(first)
        second = unit.request()
        marks.append(
            (
                env.now,
                key,
                "queued",
                [len(unit.queue), unit.queue.index(first) < unit.queue.index(second)],
            )
        )
        yield second
        marks.append((env.now, key, "seized_got", None))

    def releaser(key: str):
        yield env.timeout(2)
        for request in list(opened):
            if request in unit.users:
                unit.release(request)
        marks.append((env.now, key, "released", None))

    env.process(blocker("blocker-1"))
    for index in range(1, n + 1):
        env.process(flow(f"a-{index}"))
    env.process(releaser("r-1"))
    env.run(until=100)
    return marks


def granted_then_preempted(n: int) -> Marks:
    """Engine fix 9: a holder keeps the one preemptive unit until 5, n flows have
    asked for it with priority 5 and wait for it, and at 5, right after the holder
    gave it back and before the first waiting flow looked at its grant, a request of
    priority 0 preempts it. The flow asks again under the same name at the back of
    the line, and goes on when it is granted."""
    env = simpy.Environment()
    unit = simpy.PreemptiveResource(env, 1)
    marks: Marks = []

    def holder(key: str):
        with unit.request(priority=1) as request:
            yield request
            yield env.timeout(5)
            marks.append((env.now, key, "held", None))

    def victim(key: str):
        request = unit.request(priority=5)
        while True:
            try:
                yield request
                break
            except simpy.Interrupt:
                request = unit.request(priority=5)
        marks.append((env.now, key, "taken", None))
        yield env.timeout(3)
        marks.append((env.now, key, "worked", None))
        unit.release(request)

    def high(key: str):
        # The arrival starts the flow after the holder's grant is processed, so the
        # holder's 5 s timeout is scheduled first and the holder gives the unit back
        # before this request is made
        yield env.timeout(0)
        yield env.timeout(5)
        with unit.request(priority=0) as request:
            yield request
            yield env.timeout(2)
            marks.append((env.now, key, "high_done", None))

    def reading(key: str):
        yield env.timeout(100)
        marks.append((env.now, key, "end", [len(unit.users), len(unit.queue)]))

    env.process(holder("h-1"))
    for index in range(1, n + 1):
        env.process(victim(f"v-{index}"))
    env.process(high("p-1"))
    env.process(reading("probe-1"))
    env.run(until=500)
    return marks


# ----------------------------------------------------------------- M5, Y8, Y12, H8
# Jobs are (name, at, rank, preempt, span); `copies` repeats each job, the copies
# next to each other, as separate arrivals. A copy of a job `a` is the process `a1`,
# `a2`, and so on, and its key is `a1-1`.


def _copies(jobs, copies):
    return [
        (f"{name}{index}", at, rank, preempt, span)
        for name, at, rank, preempt, span in jobs
        for index in range(1, copies + 1)
    ]


class _RankRequest(PriorityRequest):
    """M5: a request that sorts on `order`; `key` stays SimPy's tuple."""

    def __init__(self, resource, rank=0, preempt=True):
        self.priority, self.preempt, self.time = rank, preempt, resource._env.now
        self.key = (rank, self.time, not preempt)
        resource.seq += 1
        self.order = (
            (rank, -self.time, -resource.seq, not preempt)
            if resource.ties == "lifo"
            else self.key
        )
        Request.__init__(self, resource)


class _RankQueue(list):
    def __init__(self, maxlen=None):
        super().__init__()
        self.maxlen = maxlen

    def append(self, item):
        super().append(item)
        self.sort(key=lambda e: e.order)


class _CustomResource(simpy.PreemptiveResource):
    """M5: a PreemptiveResource whose waiting order is `ties` and whose victim and
    test are the functions `victim(holder)` and `when(request, holder)`."""

    request = BoundClass(_RankRequest)
    PutQueue = _RankQueue

    def __init__(self, env, capacity, ties, when, victim):
        self.ties, self.when, self.victim, self.seq = ties, when, victim, 0
        super().__init__(env, capacity)

    def _do_put(self, event):
        if len(self.users) >= self.capacity and event.preempt:
            value = self.victim or (lambda holder: holder.key)
            chosen = max(self.users, key=value)
            when = self.when or (lambda request, holder: request.key < holder.key)
            if when(event, chosen):
                self.users.remove(chosen)
                chosen.proc.interrupt(
                    Preempted(
                        by=event.proc,
                        usage_since=chosen.usage_since,
                        resource=self,
                    )
                )
        return simpy.PriorityResource._do_put(self, event)


def m5_rules(jobs, copies, capacity=1, ties="fifo", when=None, victim=None, until=30):
    """M5: jobs with a rank and a preempt flag of their own on one resource with a
    queue order and a preempt rule."""
    env = simpy.Environment()
    unit = _CustomResource(env, capacity, ties, when, victim)
    marks: Marks = []

    def job(key, at, rank, preempt, span):
        yield env.timeout(at)
        with unit.request(rank, preempt) as request:
            try:
                yield request
                yield env.timeout(span)
                marks.append((env.now, key, "done", None))
            except simpy.Interrupt:
                marks.append((env.now, key, "preempted", None))

    for name, at, rank, preempt, span in _copies(jobs, copies):
        env.process(job(f"{name}-1", at, rank, preempt, span))
    env.run(until=until)
    return marks


def y8_computed(p: int, copies: int) -> Marks:
    """Y8: a low holder (priority 5, 5 s) from 0; `copies` requests at 1 with
    priority p and a preempt flag of p < 2, each held for 5 s."""
    env = simpy.Environment()
    unit = simpy.PreemptiveResource(env, 1)
    marks: Marks = []

    def job(key, rank, at):
        yield env.timeout(at)
        with unit.request(priority=rank, preempt=rank < 2) as request:
            try:
                yield request
                yield env.timeout(5)
                marks.append((env.now, key, "done", None))
            except simpy.Interrupt:
                marks.append((env.now, key, "preempted", None))

    env.process(job("low1-1", 5, 0))
    for index in range(1, copies + 1):
        env.process(job(f"high{index}-1", p, 1))
    env.run(until=100)
    return marks


def y8_branches(copies: int) -> Marks:
    """Y8: two units held for 2 s from 0; `copies` flows ask at 0.5 for m at rank 0
    and n at rank 5 together, and `copies` ask for n only at 1, rank 1; a flow
    that gets its units holds them for 1 s and publishes the time of the grant."""
    env = simpy.Environment()
    m = simpy.PriorityResource(env, 1)
    n = simpy.PriorityResource(env, 1)
    marks: Marks = []

    def hold(resource):
        with resource.request() as request:
            yield request
            yield env.timeout(2)

    def ask(key, at):
        yield env.timeout(at)
        x = m.request(priority=0)
        y = n.request(priority=5)
        yield x & y
        granted = env.now
        yield env.timeout(1)
        marks.append((env.now, key, "got", granted))
        m.release(x)
        n.release(y)

    def only_n(key, at):
        yield env.timeout(at)
        with n.request(priority=1) as request:
            yield request
            granted = env.now
            yield env.timeout(1)
            marks.append((env.now, key, "got", granted))

    env.process(hold(m))
    env.process(hold(n))
    for index in range(1, copies + 1):
        env.process(ask(f"p{index}-1", 0.5))
    for index in range(1, copies + 1):
        env.process(only_n(f"q{index}-1", 1))
    env.run(until=100)
    return marks


def y8_omitted(copies: int) -> Marks:
    """Y8: a holder (priority 5, 5 s) from 0; `copies` requests at 1 that name no
    priority and no preempt flag, so they are SimPy's `request()` (priority 0,
    preempt true), and `copies` requests at 2 with priority 3."""
    env = simpy.Environment()
    unit = simpy.PreemptiveResource(env, 1)
    marks: Marks = []

    def job(key, at, **kwargs):
        yield env.timeout(at)
        with unit.request(**kwargs) as request:
            try:
                yield request
                yield env.timeout(5)
                marks.append((env.now, key, "done", None))
            except simpy.Interrupt:
                marks.append((env.now, key, "preempted", None))

    env.process(job("low1-1", 0, priority=5))
    for index in range(1, copies + 1):
        env.process(job(f"viamap{index}-1", 1))
    for index in range(1, copies + 1):
        env.process(job(f"plain{index}-1", 2, priority=3))
    env.run(until=1000)
    return marks


def y8_default_rank(copies: int) -> Marks:
    """Y8: n is held for 2 s from 0. `copies` flows ask at 0.5 for m at priority 3
    and for n with `request()`, so at priority 0; `copies` ask for n alone at 1 with
    priority 1. A flow that gets both units holds them for 1 s and publishes the time
    of the grant."""
    env = simpy.Environment()
    m = simpy.PriorityResource(env, copies)
    n = simpy.PriorityResource(env, 1)
    marks: Marks = []

    def hold():
        with n.request() as request:
            yield request
            yield env.timeout(2)

    def mix(key):
        yield env.timeout(0.5)
        x = m.request(priority=3)
        y = n.request()
        yield x & y
        granted = env.now
        yield env.timeout(1)
        marks.append((env.now, key, "got", granted))
        m.release(x)
        n.release(y)

    def only_n(key):
        yield env.timeout(1)
        with n.request(priority=1) as request:
            yield request
            granted = env.now
            yield env.timeout(1)
            marks.append((env.now, key, "got", granted))

    env.process(hold())
    for index in range(1, copies + 1):
        env.process(mix(f"mix{index}-1"))
    for index in range(1, copies + 1):
        env.process(only_n(f"q{index}-1"))
    env.run(until=100)
    return marks


def y8_mapping_preempt(copies: int) -> Marks:
    """Y8: a and b are preemptive and held by a priority 5 flow for 5 s from 0.
    `copies` flows ask at 1 for a with `a.request(preempt=False)` and for b with
    `b.request()` (priority 0, preempt true), and hold both for 1 s when granted."""
    env = simpy.Environment()
    a = simpy.PreemptiveResource(env, 1)
    b = simpy.PreemptiveResource(env, 1)
    marks: Marks = []

    def hold(key, resource):
        with resource.request(priority=5) as request:
            try:
                yield request
                yield env.timeout(5)
                marks.append((env.now, key, "done", None))
            except simpy.Interrupt:
                marks.append((env.now, key, "preempted", None))

    def mix(key):
        yield env.timeout(1)
        x = a.request(priority=0, preempt=False)
        y = b.request()
        yield x & y
        granted = env.now
        yield env.timeout(1)
        marks.append((env.now, key, "got", granted))
        a.release(x)
        b.release(y)

    env.process(hold("hold_a-1", a))
    env.process(hold("hold_b-1", b))
    for index in range(1, copies + 1):
        env.process(mix(f"mix{index}-1"))
    env.run(until=100)
    return marks


class _Ranked:
    """Y12: a get or put event that carries the key SimPy's SortedQueue sorts on."""

    _sequence = itertools.count()

    def _key(self, rank, now, lifo=False):
        order = next(_Ranked._sequence)
        self.key = (rank, -order) if lifo else (rank, now, order)


class _RankedStoreGet(_Ranked, StoreGet):
    def __init__(self, store, rank, lifo=False):
        self._key(rank, store._env.now, lifo)
        super().__init__(store)


class _RankedStorePut(_Ranked, StorePut):
    def __init__(self, store, item, rank):
        self._key(rank, store._env.now)
        super().__init__(store, item)


class _RankedContainerGet(_Ranked, ContainerGet):
    def __init__(self, container, amount, rank):
        self._key(rank, container._env.now)
        super().__init__(container, amount)


class _GetRankedStore(simpy.Store):
    GetQueue = SortedQueue


class _PutRankedStore(simpy.Store):
    PutQueue = SortedQueue


class _GetRankedContainer(simpy.Container):
    GetQueue = SortedQueue


def y12_gets(ranks, copies: int, lifo: bool = False) -> Marks:
    """Y12: getters at 0, `copies` for each rank in file order, on a store whose
    waiting gets follow the rank, equal ranks in the order they asked or, with `lifo`,
    the latest first; one item (its value the time) is put at each of 1, 2, ... one
    instant apart."""
    env = simpy.Environment()
    store = _GetRankedStore(env)
    marks: Marks = []

    def getter(key, rank):
        item = yield _RankedStoreGet(store, rank, lifo)
        marks.append((env.now, key, "got", item))

    def putter(count):
        for number in range(1, count + 1):
            yield env.timeout(1)
            yield store.put(env.now)
            marks.append((env.now, f"p-{number}", "put", None))

    index = 0
    for rank in ranks:
        for _ in range(copies):
            index += 1
            env.process(getter(f"g{index}-1", rank))
    env.process(putter(index))
    env.run(until=100)
    return marks


def y12_puts(copies: int) -> Marks:
    """Y12: a store of one place; `copies` puts of rank 0 (first), then of rank 5
    (low), then of rank 1 (high) all at 0; takers at 1, 2, ... one instant apart."""
    env = simpy.Environment()
    store = _PutRankedStore(env, capacity=1)
    marks: Marks = []

    def shelve(key, name, rank):
        yield _RankedStorePut(store, name, rank)
        marks.append((env.now, key, "in", None))

    def taker(count):
        for number in range(1, count + 1):
            yield env.timeout(1)
            item = yield store.get()
            marks.append((env.now, f"tk-{number}", "out", item))

    for name, rank in (("first", 0), ("low", 5), ("high", 1)):
        for index in range(1, copies + 1):
            env.process(shelve(f"{name[0]}{index}-1", name, rank))
    env.process(taker(3 * copies))
    env.run(until=100)
    return marks


def y12_blocked_head(copies: int) -> Marks:
    """Y12: a container with a get of 5 (rank 0) and `copies` gets of 1 (rank 1)
    waiting; puts of 2 at 1, 3 at 2 and 1 at each of 3, 4, ... A get that cannot be
    met stops the scan, so a smaller get behind it waits."""
    env = simpy.Environment()
    container = _GetRankedContainer(env, capacity=100, init=0)
    marks: Marks = []

    def getter(key, amount, rank):
        yield _RankedContainerGet(container, amount, rank)
        marks.append((env.now, key, "got", container.level))

    def putter():
        yield env.timeout(1)
        yield container.put(2)
        yield env.timeout(1)
        yield container.put(3)
        for _ in range(copies):
            yield env.timeout(1)
            yield container.put(1)

    env.process(getter("big-1", 5, 0))
    for index in range(1, copies + 1):
        env.process(getter(f"small{index}-1", 1, 1))
    env.process(putter())
    env.run(until=100)
    return marks


def h8_limit(limits, arrivals, copies: int, service: float = 10) -> Marks:
    """H8: one unit and a queue limit that `limits` ([(time, value)], the first at 0)
    changes during the run; `copies` jobs arrive at each time of `arrivals`. A job
    that finds as many waiting as the limit balks."""
    env = simpy.Environment()
    unit = simpy.Resource(env, 1)
    limit = [limits[0][1]]
    marks: Marks = []

    def job(key, at):
        yield env.timeout(at)
        if limit[0] is not None and len(unit.queue) >= limit[0]:
            marks.append((env.now, key, "balked", None))
            return
        with unit.request() as request:
            yield request
            yield env.timeout(service)
            marks.append((env.now, key, "done", None))

    def change():
        for at, value in limits[1:]:
            yield env.timeout(at - env.now)
            limit[0] = value

    env.process(change())
    index = 0
    for at in arrivals:
        for _ in range(copies):
            index += 1
            env.process(job(f"j{index}-1", at))
    env.run()
    return marks

"""The SimPy programs of the Batch 9 design tests (interrupts and failures). Each
returns the marks the blueprint publishes, in publication order, as
(time, key, label, extra), the same form as `batch8`."""

from typing import Any, List, Optional, Tuple

import simpy

Marks = List[Tuple[Any, ...]]


def _subject(env, m, marks, key, policy, service, back=False):
    """One flow that asks for the unit, serves `service` s and releases it. An interrupt
    while it waits: `resume` keeps the request and its place, `restart` cancels a
    request that is not granted yet and asks again at the back (a request granted but
    not taken is kept), `abandon` ends the flow and frees the unit it has. With `back`, `resume` also asks again at the back, as a
    `resource:` task does that has not been granted its unit."""
    request = m.request()
    while True:
        try:
            yield request
            break
        except simpy.Interrupt:
            marks.append((env.now, key, "interrupted", None))
            if policy == "abandon":
                if request.triggered:
                    m.release(request)
                else:
                    request.cancel()
                marks.append((env.now, key, "cleanup", None))
                return
            if (policy == "restart" or back) and not request.triggered:
                request.cancel()
                request = m.request()
    left = service
    while left > 0:
        began = env.now
        try:
            yield env.timeout(left)
            left = 0
        except simpy.Interrupt:
            marks.append((env.now, key, "interrupted", None))
            if policy == "abandon":
                m.release(request)
                marks.append((env.now, key, "cleanup", None))
                return
            left = service if policy == "restart" else left - (env.now - began)
    marks.append((env.now, key, "s_done", None))
    m.release(request)


def unit_interrupt(n: int, policy: str, part: str, back: bool = False) -> Marks:
    """Unit states, Interrupt and Grant at the instant, for the flows `n` that ask for a
    unit (`seize`, `resource` or a wait `request`, the same).

    - `queued`: a holder has the unit until 10, the flows ask at 0, a flow that asks at
      1 is behind them, and all the flows are interrupted at 3.
    - `again`: as `queued`, with a flow that asks at 4 and a second interrupt at 5, so a
      request asked again at 3 is interrupted again.
    - `held`: the unit has room for every flow, each serves 5 s, and all are interrupted
      at 2.
    - `instant`: a holder has the unit until 10 and the interrupt is sent at 10, after
      the holder released it and before the first flow took the grant. A flow that asks
      at 1 is behind the flows.

    `back` is the `resource:` form, which asks again at the back under `resume` too."""
    env = simpy.Environment()
    marks: Marks = []
    m = simpy.Resource(env, n if part == "held" else 1)
    flows: List[simpy.Process] = []
    service = 5 if part == "held" else 2

    def holder():
        with m.request() as request:
            yield request
            yield env.timeout(10)
            marks.append((env.now, "holder-1", "held", None))

    def behind(key, at):
        yield env.timeout(at)
        with m.request() as request:
            yield request
            yield env.timeout(1)
            marks.append((env.now, key, "behind", None))

    def cutter():
        if part == "instant":
            # Two waits of 5 s, so the timeout that ends at 10 is made after the
            # holder's, and one of 0 s, so the interrupt is sent after the holder's
            # release was processed and before the grant it caused: the first flow's
            # request is granted but not yet taken
            yield env.timeout(5)
            yield env.timeout(5)
            yield env.timeout(0)
        else:
            yield env.timeout({"queued": 3, "again": 3, "held": 2}[part])
        marks.append((env.now, "cut-1", "cut", None))
        for flow in flows:
            flow.interrupt()
        if part == "again":
            yield env.timeout(2)
            marks.append((env.now, "cut2-1", "cut", None))
            for flow in flows:
                flow.interrupt()

    def probe():
        yield env.timeout(30)
        marks.append((env.now, "probe-1", "end", [len(m.users), len(m.queue)]))

    if part != "held":
        env.process(holder())
    for index in range(1, n + 1):
        flows.append(
            env.process(_subject(env, m, marks, f"s-{index}", policy, service, back))
        )
    if part in ("queued", "again", "instant"):
        env.process(behind("behind-1", 1))
    if part == "again":
        env.process(behind("behind2-1", 4))
    env.process(cutter())
    env.process(probe())
    env.run(until=500)
    return marks


def suspended_interrupt(n: int, policy: str, wake_at: Optional[float]) -> Marks:
    """Engine fix 11: `n` flows wait for a resume, `suspend`, and are interrupted at 2.
    Another flow suspends at 1 and is not interrupted. Under `resume` the flows stay
    suspended and a later resume at `wake_at` wakes them in the order they suspended;
    under `restart` they wait again, at the back of the suspended list, behind the
    other flow; under `abandon` they end and a resume wakes nobody."""
    env = simpy.Environment()
    marks: Marks = []
    parked: List[Tuple[str, simpy.Event]] = []
    flows: List[simpy.Process] = []

    def late():
        yield env.timeout(1)
        yield from park("late-1")

    def park(key):
        event = env.event()
        parked.append((key, event))
        while True:
            try:
                yield event
                break
            except simpy.Interrupt:
                marks.append((env.now, key, "interrupted", None))
                if policy == "abandon":
                    parked.remove((key, event))
                    marks.append((env.now, key, "cleanup", None))
                    return
                if policy == "restart":
                    parked.remove((key, event))
                    event = env.event()
                    parked.append((key, event))
        marks.append((env.now, key, "woke", None))

    def cutter():
        yield env.timeout(2)
        for flow in flows:
            flow.interrupt()

    def waker():
        yield env.timeout(wake_at)
        for _, event in list(parked):
            event.succeed()

    for index in range(1, n + 1):
        flows.append(env.process(park(f"s-{index}")))
    env.process(late())
    env.process(cutter())
    if wake_at is not None:
        env.process(waker())
    env.run(until=500)
    return marks


def i3_interrupt_run(n: int, policy: str) -> Marks:
    """I3: `n` flows work 10 s. All are interrupted at 2 and a repair flow of 5 s runs
    first; a second interrupt at 4 reaches the repair, which goes on with the time it
    has left, as its own `Interrupt` handler does. Then the work goes on with what is
    left (`resume`) or starts again (`restart`). The repair runs in the same process,
    so an interrupt reaches the repair and not the work."""
    env = simpy.Environment()
    marks: Marks = []
    flows: List[simpy.Process] = []

    def repair(key):
        left = 5
        while left > 0:
            began = env.now
            try:
                yield env.timeout(left)
                left = 0
            except simpy.Interrupt:
                marks.append((env.now, key, "interrupted", None))
                left -= env.now - began
        marks.append((env.now, key, "repaired", None))

    def work(key):
        left = 10
        while left > 0:
            began = env.now
            try:
                yield env.timeout(left)
                left = 0
            except simpy.Interrupt:
                marks.append((env.now, key, "interrupted", None))
                left = 10 if policy == "restart" else left - (env.now - began)
                yield from repair(key)
        marks.append((env.now, key, "done", None))

    def cutter(at):
        def run():
            yield env.timeout(at)
            for flow in flows:
                flow.interrupt()

        return run()

    for index in range(1, n + 1):
        flows.append(env.process(work(f"w-{index}")))
    env.process(cutter(2))
    env.process(cutter(4))
    env.run(until=500)
    return marks


def i4_resume(n: int) -> Marks:
    """I4: `n` flows suspend at 0. A flow at 2 resumes one with a value, a flow at 4
    resumes none (`limit: 0`, but its value is evaluated), a flow at 6 resumes the
    rest (`limit: 100`), and the flows get the values 0, 1 and 2 in turn. A flow of another task suspends at 1 and no resume names it."""
    env = simpy.Environment()
    marks: Marks = []
    parked: List[Tuple[str, simpy.Event, int]] = []
    draws = [0]

    def park(key, group):
        event = env.event()
        parked.append((key, event, group))
        value = yield event
        marks.append((env.now, key, "woke", value))

    def late():
        yield env.timeout(1)
        yield from park("t-1", 1)

    def waker(at, limit):
        yield env.timeout(at)
        value = draws[0]
        draws[0] += 1
        chosen = [p for p in parked if p[2] == 0]
        if limit is not None:
            chosen = chosen[: max(limit, 0)]
        for item in chosen:
            parked.remove(item)
            item[1].succeed(value)

    for index in range(1, n + 1):
        env.process(park(f"s-{index}", 0))
    env.process(late())
    for at, limit in ((2, 1), (4, 0), (6, 100)):
        env.process(waker(at, limit))
    env.run(until=500)
    return marks


def i6_leave(n: int) -> Marks:
    """I6: a holder has the unit until 5. `n` flows ask at 1 with a patience of 2 and
    leave the line when the queue already holds 2 (they balk) or when the patience
    ends (they renege). Each goes on to `elsewhere` and reads how it got there."""
    env = simpy.Environment()
    marks: Marks = []
    m = simpy.Resource(env, 1)

    def holder():
        with m.request() as request:
            yield request
            yield env.timeout(5)

    def late(key):
        yield env.timeout(1)
        if len(m.queue) >= 2:
            marks.append((env.now, key, "balked", None))
            marks.append((env.now, key, "elsewhere", "balked"))
            return
        with m.request() as request:
            got = yield request | env.timeout(2)
            if request in got:
                marks.append((env.now, key, "served", None))
                return
        marks.append((env.now, key, "reneged", None))
        marks.append((env.now, key, "elsewhere", "reneged"))

    env.process(holder())
    for index in range(1, n + 1):
        env.process(late(f"l-{index}"))
    env.run(until=500)
    return marks


def m3_failure(n: int, part: str) -> Marks:
    """M3: `n` flows fail and a task named by `on_fail` goes on. `step`: the flow's own
    expression divides by zero, and the handler reads the status. `wait`: the flow
    waits for a signal that fails at 1 with the reason `event`."""
    env = simpy.Environment()
    marks: Marks = []
    event = env.event()

    def step(key):
        try:
            1 / 0
        except ZeroDivisionError:
            marks.append((env.now, key, "failed", None))
            marks.append((env.now, key, "handled", "failed"))
        yield env.timeout(0)

    def wait(key):
        try:
            yield event
        except Exception as error:  # noqa: BLE001
            marks.append((env.now, key, "failed", None))
            marks.append((env.now, key, "handled", str(error)))

    def breaker():
        yield env.timeout(1)
        event.fail(ValueError("event"))

    for index in range(1, n + 1):
        env.process((step if part == "step" else wait)(f"a-{index}"))
    if part == "wait":
        env.process(breaker())
    env.run(until=500)
    return marks

"""C3 interrupts and preemption.

A plain SimPy model and the same model in the low-level API, the builder and YAML:
a waiting task interrupted (it leaves the queue, then queues again or ends), a task
interrupted in service under each policy (resume serves the rest, restart draws a
new time from the same seeded stream, abandon gives the unit back), two interrupts at
one instant, and preemption on a priority resource, with equal priorities and two
preemptions at one instant.

The builder and YAML deliver interrupts with `interrupt` (a scenario step, or
`context.interrupt`); the low-level models use SimPy's `Process.interrupt`. Times
are compared for the first instance of each task only, because a YAML arrival
repeats.
"""

import simpy

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicResource,
    Entity,
    Sampler,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder, run_low_level, run_yaml

POLICIES = ("resume", "restart", "abandon")


def _trace(records, key, queued=False):
    """(time, status) of one entity's events; a finished event is `finished`, and
    `queued` events are left out unless asked for."""
    out = []
    for r in records:
        if r["stream_type"] != "event" or r["key"] != key:
            continue
        status = r["value"].get("status", "finished")
        if status != "queued" or queued:
            out.append((r["sim_ts"], status))
    return out


def _machine(sim_id, cap=1):
    return SimParameter(
        sim_id=sim_id,
        resources={"machine": CapacityConfig(current_cap=cap, max_cap=cap)},
    )


# ------------------------------------------------------- interrupt waiting


def _reference_waiting(policy):
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    trace = []

    def holder():
        yield env.timeout(10)
        with machine.request() as req:
            yield req
            yield env.timeout(10)

    def waiter():
        yield env.timeout(11)
        trace.append((env.now, "queued"))
        while True:
            try:
                with machine.request() as req:
                    yield req
                    trace.append((env.now, "started"))
                    yield env.timeout(1)
                    trace.append((env.now, "finished"))
                    return
            except simpy.Interrupt:
                trace.append((env.now, "interrupted"))
                if policy == "abandon":
                    return

    env.process(holder())
    proc = env.process(waiter())

    def recall():
        yield env.timeout(13)
        proc.interrupt("recall")

    env.process(recall())
    env.run(until=21.5)
    return trace, len(machine.queue)


def _low_level_waiting(policy):
    built = {}

    def start(env):
        machine = built["machine"] = DynamicResource(env, "C3", "machine")

        def publish(status):
            env.publish_event(
                "waiters-1", {"status": status}, task="waiter", instance=0
            )

        def holder():
            yield env.timeout(10)
            with machine.request() as req:
                yield req
                yield env.timeout(10)

        def waiter():
            yield env.timeout(11)
            publish("queued")
            while True:
                try:
                    with machine.request() as req:
                        yield req
                        publish("started")
                        yield env.timeout(1)
                        env.publish_event(
                            "waiters-1", {"event": "worked"}, task="waiter", instance=0
                        )
                        return
                except simpy.Interrupt:
                    publish("interrupted")
                    if policy == "abandon":
                        return

        env.process(holder())
        proc = env.process(waiter())

        def recall():
            yield env.timeout(13)
            proc.interrupt("recall")

        env.process(recall())

    records, _env = run_low_level(_machine("C3"), start, 21.5)
    return records, len(built["machine"].queue.items)


def _builder_waiting(policy):
    app = SimulationContext("C3", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=10)
    app.add_service("work", dist="constant", value=1)

    @app.task(service_id="hold", resource_id="machine")
    def holder(task_id):
        return {"event": "held"}

    @app.task(service_id="work", resource_id="machine", on_interrupt=policy)
    def waiter(task_id):
        return {"event": "worked"}

    def script(context):
        yield context.env.timeout(10)
        context.spawn(holder(0, entity=Entity("holders-1", "holders")))
        yield context.env.timeout(1)
        context.spawn(waiter(0, entity=Entity("waiters-1", "waiters")))
        yield context.env.timeout(2)
        context.interrupt("waiter", cause="recall")

    app.add_process(script)
    records = run_builder(app, 21.5)
    return records, len(app.get_resource("machine").queue.items)


def test_c3_interrupt_while_waiting():
    """Interrupted at 13 while queued: with abandon it leaves and is never served,
    and no request is left in the queue; with resume it queues again and is served
    when the holder leaves at 20."""
    for policy, served in (
        ("abandon", []),
        ("resume", [(20, "started"), (21, "finished")]),
    ):
        trace, queued = _reference_waiting(policy)
        assert trace == [(11, "queued"), (13, "interrupted"), *served]
        assert queued == 0
        for records, left in (
            _low_level_waiting(policy),
            _builder_waiting(policy),
            (lambda r: (r[0], len(r[1].get_resource("machine").queue.items)))(
                run_yaml(f"c3_interrupt_waiting_{policy}.yaml")
            ),
        ):
            got = _trace(records, "waiters-1", queued=True)
            assert got == [(float(t), s) for t, s in trace]
            if policy == "abandon":
                assert left == 0


# ------------------------------------------------------ interrupt service


def _draws(seed=7, n=2):
    """The first `n` service times the seeded stream of `service.work` gives."""
    config = DistributionConfig(dist="exponential", rate=0.2)
    config.stream = "service.work"
    sampler = Sampler(seed=seed)
    return [sampler.sample(config) for _ in range(n)]


def _reference_service(policy):
    first, second = _draws()
    assert first > 2  # the interrupt at 12 falls inside the first service
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    trace = []

    def worker():
        yield env.timeout(10)
        remaining = first
        started = False
        while True:
            try:
                with machine.request() as req:
                    yield req
                    trace.append((env.now, "resumed" if started else "started"))
                    started = True
                    since = env.now
                    yield env.timeout(remaining)
                    trace.append((env.now, "finished"))
                    return
            except simpy.Interrupt:
                trace.append((env.now, "interrupted"))
                if policy == "abandon":
                    return
                remaining = (
                    remaining - (env.now - since) if policy == "resume" else second
                )

    proc = env.process(worker())

    def recall():
        yield env.timeout(12)
        proc.interrupt("recall")

    env.process(recall())
    env.run(until=19.9)
    return trace


def _builder_service(policy):
    app = SimulationContext("C3", factor=0.0, random_seed=7)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("work", dist="exponential", rate=0.2)

    @app.task(service_id="work", resource_id="machine", on_interrupt=policy)
    def worker(task_id):
        return {"event": "worked"}

    def script(context):
        yield context.env.timeout(10)
        context.spawn(worker(0, entity=Entity("workers-1", "workers")))
        yield context.env.timeout(2)
        context.interrupt("worker", cause="recall")

    app.add_process(script)
    return run_builder(app, 19.9)


def _low_level_service(policy):
    first, second = _draws()

    def start(env):
        machine = DynamicResource(env, "C3", "machine")

        def publish(value):
            env.publish_event("workers-1", value, task="worker", instance=0)

        def worker():
            yield env.timeout(10)
            remaining, started = first, False
            while True:
                try:
                    with machine.request() as req:
                        yield req
                        publish({"status": "resumed" if started else "started"})
                        started, since = True, env.now
                        yield env.timeout(remaining)
                        publish({"event": "worked"})
                        return
                except simpy.Interrupt:
                    publish({"status": "interrupted"})
                    if policy == "abandon":
                        return
                    remaining = (
                        remaining - (env.now - since) if policy == "resume" else second
                    )

        proc = env.process(worker())

        def recall():
            yield env.timeout(12)
            proc.interrupt("recall")

        env.process(recall())

    return run_low_level(_machine("C3"), start, 19.9)[0]


def test_c3_interrupt_during_service():
    """Interrupted at 12 in service: resume serves the rest, restart draws a new
    time from the seeded stream, and abandon ends with the unit given back."""
    first, second = _draws()
    expected = {
        "resume": [
            (10, "started"),
            (12, "interrupted"),
            (12, "resumed"),
            (10 + first, "finished"),
        ],
        "restart": [(10, "started"), (12, "interrupted"), (12, "resumed")]
        + ([(12 + second, "finished")] if 12 + second < 19.9 else []),
        "abandon": [(10, "started"), (12, "interrupted")],
    }
    for policy in POLICIES:
        expected[policy] = [(t, s) for t, s in expected[policy] if t < 19.9]
        assert _reference_service(policy) == expected[policy]
        for records in (
            _low_level_service(policy),
            _builder_service(policy),
            run_yaml(f"c3_interrupt_service_{policy}.yaml")[0],
        ):
            got = _trace(records, "workers-1")
            assert [(round(t, 3), s) for t, s in got] == [
                (round(t, 3), s) for t, s in expected[policy]
            ], policy


def test_c3_two_interrupts_at_one_instant():
    """Two interrupts at 12 arrive in order: the worker is interrupted twice, queues
    again after each, and resumes once, ending at 15."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    trace = []

    def worker():
        yield env.timeout(10)
        remaining, started, since = 5.0, False, None
        while True:
            try:
                with machine.request() as req:
                    yield req
                    trace.append((env.now, "resumed" if started else "started"))
                    started, since = True, env.now
                    yield env.timeout(remaining)
                    trace.append((env.now, "finished"))
                    return
            except simpy.Interrupt:
                trace.append((env.now, "interrupted"))
                if since is not None:
                    remaining -= env.now - since
                    since = None

    proc = env.process(worker())

    def recall():
        yield env.timeout(12)
        proc.interrupt("first")
        proc.interrupt("second")

    env.process(recall())
    env.run(until=19.9)
    expected = [
        (10, "started"),
        (12, "interrupted"),
        (12, "interrupted"),
        (12, "resumed"),
        (15, "finished"),
    ]
    assert trace == expected

    app = SimulationContext("C3", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_service("work", dist="constant", value=5)

    @app.task(service_id="work", resource_id="machine")
    def worker(task_id):
        return {"event": "worked"}

    def script(context):
        yield context.env.timeout(10)
        context.spawn(worker(0, entity=Entity("workers-1", "workers")))
        yield context.env.timeout(2)
        context.interrupt("worker", cause="first")
        context.interrupt("worker", cause="second")

    app.add_process(script)
    for records in (run_builder(app, 19.9), run_yaml("c3_two_interrupts.yaml")[0]):
        assert _trace(records, "workers-1") == [(float(t), s) for t, s in expected]


# ------------------------------------------------------------- preemption


def _reference_preemption(lows, highs, cap, gap=0.5):
    """(time, user, status) on a PreemptiveResource: lows from 10 (each `gap` s
    apart) for 10 s, highs at 13 for 5 s; a preempted low resumes."""
    env = simpy.Environment()
    machine = simpy.PreemptiveResource(env, capacity=cap)
    trace = []

    def user(name, priority, at, hold):
        yield env.timeout(at)
        remaining, started, since = hold, False, None
        while True:
            try:
                with machine.request(priority=priority) as req:
                    yield req
                    trace.append((env.now, name, "resumed" if started else "started"))
                    started, since = True, env.now
                    yield env.timeout(remaining)
                    trace.append((env.now, name, "finished"))
                    return
            except simpy.Interrupt:
                trace.append((env.now, name, "preempted"))
                remaining -= env.now - since

    for i, p in enumerate(lows):
        env.process(user(f"low{i}", p, 10 + gap * i, 10))
    for i, p in enumerate(highs):
        env.process(user(f"high{i}", p, 13, 5))
    env.run(until=19.9)
    return trace


def _desflow_trace(records):
    out = []
    for r in records:
        if r["stream_type"] != "event" or not r["key"].endswith("-1"):
            continue
        status = r["value"].get("status", "finished")
        if status != "queued":
            out.append((r["sim_ts"], r["key"][:-2], status))
    return out


def _builder_preemption(lows, highs, cap, gap=0.5):
    app = SimulationContext("C3", factor=0.0)
    app.add_resource(
        "machine", current_cap=cap, max_cap=cap, queue="priority", preempt=True
    )
    app.add_service("long", dist="constant", value=10)
    app.add_service("short", dist="constant", value=5)

    @app.task(service_id="long", resource_id="machine", priority="attr.p")
    def low(task_id):
        return {"event": "low_done"}

    @app.task(service_id="short", resource_id="machine", priority="attr.p")
    def high(task_id):
        return {"event": "high_done"}

    def script(context):
        events = [(10 + gap * i, f"low{i}", low, p) for i, p in enumerate(lows)]
        events += [(13, f"high{i}", high, p) for i, p in enumerate(highs)]
        for at, name, task, p in sorted(events, key=lambda e: e[0]):
            if at > context.env.now:
                yield context.env.timeout(at - context.env.now)
            entity = Entity(f"{name}-1", name, {"p": p})
            context.spawn(task(context.next_instance(task.__name__), entity=entity))

    app.add_process(script)
    return run_builder(app, 19.9)


def _check_preemption(lows, highs, cap, blueprint, gap=0.5):
    expected = _reference_preemption(lows, highs, cap, gap)
    for records in (
        _builder_preemption(lows, highs, cap, gap),
        run_yaml(blueprint)[0],
    ):
        assert _desflow_trace(records) == [(float(t), n, s) for t, n, s in expected]
    return expected


def test_c3_priority_preemption():
    """The high request of 13 takes the unit from the low user, which queues again
    and resumes when the high one ends at 18."""
    assert _check_preemption((2,), (1,), 1, "c3_preemption.yaml") == [
        (10, "low0", "started"),
        (13, "low0", "preempted"),
        (13, "high0", "started"),
        (18, "high0", "finished"),
        (18, "low0", "resumed"),
    ]


def test_c3_priority_preemption_equal_priorities():
    """Equal priorities do not preempt, the requests being made at different
    times: the second waits until 20."""
    assert _check_preemption((1,), (1,), 1, "c3_preemption_equal.yaml") == [
        (10, "low0", "started"),
    ]


def test_c3_two_preemptions_at_one_instant():
    """Two high requests at 13 on a machine of 2 each take a unit: first from the
    user whose key sorts last (priority 3), then from the next (priority 2). Both
    interrupts are urgent events, so both users learn of it before either high
    request is granted."""
    assert _check_preemption((3, 2), (1, 1), 2, "c3_two_preemptions.yaml") == [
        (10, "low0", "started"),
        (10.5, "low1", "started"),
        (13, "low0", "preempted"),
        (13, "low1", "preempted"),
        (13, "high0", "started"),
        (13, "high1", "started"),
        (18, "high0", "finished"),
        (18, "high1", "finished"),
        (18, "low1", "resumed"),
        (18, "low0", "resumed"),
    ]


def test_c3_preemption_tie_between_users():
    """Two users of priority 1 got the machine of 2 at 10, so their keys are equal.
    SimPy 4.1.2 picks the user to preempt with `max`, which returns the first of
    equal keys in the order the units were granted: low0, not low1."""
    expected = _check_preemption((1, 1), (0,), 2, "c3_preemption_tie.yaml", gap=0)
    assert expected[:4] == [
        (10, "low0", "started"),
        (10, "low1", "started"),
        (13, "low0", "preempted"),
        (13, "high0", "started"),
    ]
    trace = []

    def start(env):
        machine = DynamicResource(env, "C3", "machine", queue="priority", preempt=True)

        def user(name, priority, at, hold):
            yield env.timeout(at)
            remaining, started, since = hold, False, None
            while True:
                try:
                    with machine.request(priority=priority) as req:
                        yield req
                        trace.append(
                            (env.now, name, "resumed" if started else "started")
                        )
                        started, since = True, env.now
                        yield env.timeout(remaining)
                        trace.append((env.now, name, "finished"))
                        return
                except simpy.Interrupt:
                    trace.append((env.now, name, "preempted"))
                    remaining -= env.now - since

        env.process(user("low0", 1, 10, 10))
        env.process(user("low1", 1, 10, 10))
        env.process(user("high0", 0, 13, 5))

    run_low_level(_machine("C3", cap=2), start, 19.9)
    assert trace == expected


def test_c3_priority_preemption_low_level():
    """The low-level resource preempts as SimPy's PreemptiveResource does."""
    expected = _reference_preemption((3, 2), (1, 1), 2)

    trace = []

    def start(env):
        machine = DynamicResource(env, "C3", "machine", queue="priority", preempt=True)

        def user(name, priority, at, hold):
            yield env.timeout(at)
            remaining, started, since = hold, False, None
            while True:
                try:
                    with machine.request(priority=priority) as req:
                        yield req
                        trace.append(
                            (env.now, name, "resumed" if started else "started")
                        )
                        started, since = True, env.now
                        yield env.timeout(remaining)
                        trace.append((env.now, name, "finished"))
                        return
                except simpy.Interrupt as interrupt:
                    assert interrupt.cause.resource is machine
                    trace.append((env.now, name, "preempted"))
                    remaining -= env.now - since

        for i, p in enumerate((3, 2)):
            env.process(user(f"low{i}", p, 10 + 0.5 * i, 10))
        for i, p in enumerate((1, 1)):
            env.process(user(f"high{i}", p, 13, 5))

    run_low_level(_machine("C3", cap=2), start, 19.9)
    assert trace == expected

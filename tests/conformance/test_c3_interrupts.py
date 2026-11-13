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

import pytest
import simpy

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicResource,
    Entity,
    Sampler,
    SimParameter,
)
from desflow.builder.context import SimulationContext
from tests.conformance.b3 import Job, run_desflow, run_simpy
from tests.conformance.harness import (
    run_builder,
    run_low_level,
    run_yaml,
    run_yaml_text,
)

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


def _machine(sim_id, cap=1, queue="fifo"):
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
    return records, len(built["machine"].queue)


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
    return records, len(app.get_resource("machine").queue)


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
            (lambda r: (r[0], len(r[1].get_resource("machine").queue)))(
                run_yaml(f"c3_interrupt_waiting_{policy}.yaml")
            ),
        ):
            got = _trace(records, "waiters-1", queued=True)
            assert got == [(float(t), s) for t, s in trace]
            if policy == "abandon":
                assert left == 0


# ------------------------------------------------------ interrupt service


def _draws(seed=2, n=2):
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
    app = SimulationContext("C3", factor=0.0, random_seed=2)
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
    """Two interrupts at 12 arrive in order: the worker, which keeps its request, is
    interrupted, resumes, is interrupted again, resumes again, and ends at 15."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    trace = []

    def worker():
        yield env.timeout(10)
        remaining, started, since = 5.0, False, None
        with machine.request() as req:
            yield req
            while True:
                try:
                    trace.append((env.now, "resumed" if started else "started"))
                    started, since = True, env.now
                    yield env.timeout(remaining)
                    trace.append((env.now, "finished"))
                    return
                except simpy.Interrupt:
                    trace.append((env.now, "interrupted"))
                    remaining -= env.now - since

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
        (12, "resumed"),
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


# ------------------------------------------------- a resumed job keeps its unit


def _events(records):
    """(time, key, status) of every event but `queued`; a finished event is `worked`."""
    return [
        (float(r["sim_ts"]), r["key"], r["value"].get("status", "worked"))
        for r in records
        if r["stream_type"] == "event" and r["value"].get("status") != "queued"
    ]


def _keep_reference(policy, n):
    """`n` workers hold a machine of `n` units for 5, are interrupted in order at 2,
    and keep their request; `n` rivals ask for a unit at 1."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=n)
    trace = []

    def worker(name):
        remaining, since = 5.0, None
        with machine.request() as req:
            yield req
            started = False
            while True:
                try:
                    trace.append((env.now, name, "resumed" if started else "started"))
                    started, since = True, env.now
                    yield env.timeout(remaining)
                    trace.append((env.now, name, "worked"))
                    return
                except simpy.Interrupt:
                    trace.append((env.now, name, "interrupted"))
                    remaining = (
                        remaining - (env.now - since) if policy == "resume" else 5.0
                    )

    def rival(name):
        yield env.timeout(1)
        with machine.request() as req:
            yield req
            trace.append((env.now, name, "started"))
            yield env.timeout(2)
            trace.append((env.now, name, "worked"))

    workers = [env.process(worker(f"workers-{i}")) for i in range(1, n + 1)]
    for i in range(1, n + 1):
        env.process(rival(f"rival-{i}"))

    def recall():
        yield env.timeout(2)
        for process in workers:
            process.interrupt("recall")

    env.process(recall())
    env.run(until=30)
    return trace


@pytest.mark.parametrize("n", [1, 3, 10])
@pytest.mark.parametrize("policy", ["resume", "restart"])
def test_c3_a_plain_interrupt_keeps_the_unit_while_others_wait(tmp_path, policy, n):
    """`n` workers are interrupted at 2 while `n` rivals wait for a unit. Each worker
    keeps its unit and goes on at once, so the rivals start only when the workers
    end, as a SimPy process that holds its request across `except Interrupt` does."""
    expected = _keep_reference(policy, n)
    assert (2.0, "workers-1", "resumed") in expected
    assert min(t for t, key, s in expected if key == "rival-1" and s == "started") > 2

    app = SimulationContext("C3", factor=0.0)
    app.add_resource("machine", current_cap=n, max_cap=n)
    app.add_service("work", dist="constant", value=5)
    app.add_service("short", dist="constant", value=2)

    @app.task(service_id="work", resource_id="machine", on_interrupt=policy)
    def worker(task_id):
        return {"event": "worked"}

    @app.task(service_id="short", resource_id="machine")
    def rival(task_id):
        return {"event": "worked"}

    def script(context):
        for i in range(1, n + 1):
            context.spawn(worker(0, entity=Entity(f"workers-{i}", "workers")))
        yield context.env.timeout(1)
        for i in range(1, n + 1):
            context.spawn(rival(0, entity=Entity(f"rival-{i}", "rival")))
        yield context.env.timeout(1)
        context.interrupt("worker", cause="recall")

    app.add_process(script)
    assert _events(run_builder(app, 30)) == expected
    assert app.get_resource("machine").count == 0

    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        f"resources: {{machine: {{current_cap: {n}, max_cap: {n}}}}}\n"
        "services: {work: {dist: constant, value: 5}, short: {dist: constant, value: 2}}\n"
        "arrivals:\n"
        "  workers: {replay: {path: workers.csv}, spawn: worker}\n"
        "  rival: {replay: {path: rival.csv}, spawn: rival}\n"
        "tasks:\n"
        f"  worker: {{service: work, resource: machine, on_interrupt: {policy}, "
        "payload: {event: worked}}\n"
        "  rival: {service: short, resource: machine, payload: {event: worked}}\n"
        "scenario: [{at: 2, interrupt: {task: worker, cause: recall}}]\n"
        "run: {until: 30}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"workers": [0] * n, "rival": [1] * n})
    assert outcome.error is None
    assert _events(outcome.records) == expected
    assert outcome.app_or_env.get_resource("machine").count == 0


def _two_resources_reference(n):
    """`n` workers need a tray and a machine; the machine is busy until 8. They hold
    the tray from 1 and are interrupted at 4. They keep the tray and ask for the
    machine again; `n` rivals have waited for a tray since 2."""
    env = simpy.Environment()
    tray = simpy.Resource(env, capacity=n)
    machine = simpy.Resource(env, capacity=n)
    trace = []

    def blocker():
        with machine.request() as req:
            yield req
            yield env.timeout(8)

    def worker(name):
        yield env.timeout(1)
        held, remaining, since = {}, 3.0, None
        started = False
        try:
            while True:
                try:
                    for label, resource in (("tray", tray), ("machine", machine)):
                        if label not in held:
                            held[label] = resource.request()
                        yield held[label]
                    trace.append((env.now, name, "resumed" if started else "started"))
                    started, since = True, env.now
                    yield env.timeout(remaining)
                    trace.append((env.now, name, "worked"))
                    return
                except simpy.Interrupt:
                    trace.append((env.now, name, "interrupted"))
                    if since is not None:
                        remaining -= env.now - since
                        since = None
                    for label in list(held):
                        if not held[label].triggered:
                            held[label].cancel()
                            del held[label]
        finally:
            for request in held.values():
                request.resource.release(request)

    def rival(name):
        yield env.timeout(2)
        with tray.request() as req:
            yield req
            trace.append((env.now, name, "started"))
            yield env.timeout(1)
            trace.append((env.now, name, "worked"))

    for _ in range(n):
        env.process(blocker())
    workers = [env.process(worker(f"workers-{i}")) for i in range(1, n + 1)]
    for i in range(1, n + 1):
        env.process(rival(f"rival-{i}"))

    def recall():
        yield env.timeout(4)
        for process in workers:
            process.interrupt("recall")

    env.process(recall())
    env.run(until=30)
    return trace


@pytest.mark.parametrize("n", [1, 3, 10])
def test_c3_an_interrupt_in_a_queue_keeps_the_unit_already_granted(tmp_path, n):
    """A worker holds a tray and waits for a machine when it is interrupted. It keeps
    the tray, which a rival has waited for, and asks for the machine again at the back
    of the line, as a SimPy process that asks only for the request it lost does."""
    expected = _two_resources_reference(n)
    assert min(t for t, key, s in expected if key == "rival-1" and s == "started") > 8

    app = SimulationContext("C3", factor=0.0)
    app.add_resource("tray", current_cap=n, max_cap=n)
    app.add_resource("machine", current_cap=n, max_cap=n)
    app.add_service("hold", dist="constant", value=8)
    app.add_service("work", dist="constant", value=3)
    app.add_service("short", dist="constant", value=1)

    @app.task(service_id="hold", resource_id="machine")
    def blocker(task_id):
        return {"event": "worked"}

    @app.task(service_id="work", resource_id=["tray", "machine"])
    def worker(task_id):
        return {"event": "worked"}

    @app.task(service_id="short", resource_id="tray")
    def rival(task_id):
        return {"event": "worked"}

    def script(context):
        for i in range(1, n + 1):
            context.spawn(blocker(0, entity=Entity(f"blocker-{i}", "blocker")))
        yield context.env.timeout(1)
        for i in range(1, n + 1):
            context.spawn(worker(0, entity=Entity(f"workers-{i}", "workers")))
        yield context.env.timeout(1)
        for i in range(1, n + 1):
            context.spawn(rival(0, entity=Entity(f"rival-{i}", "rival")))
        yield context.env.timeout(2)
        context.interrupt("worker", cause="recall")

    app.add_process(script)
    got = [e for e in _events(run_builder(app, 30)) if not e[1].startswith("blocker")]
    assert got == expected
    assert app.get_resource("tray").count == 0
    assert app.get_resource("machine").count == 0

    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        "resources:\n"
        f"  tray: {{current_cap: {n}, max_cap: {n}}}\n"
        f"  machine: {{current_cap: {n}, max_cap: {n}}}\n"
        "services: {hold: {dist: constant, value: 8}, work: {dist: constant, value: 3}, "
        "short: {dist: constant, value: 1}}\n"
        "arrivals:\n"
        "  blocker: {replay: {path: blocker.csv}, spawn: blocker}\n"
        "  workers: {replay: {path: workers.csv}, spawn: worker}\n"
        "  rival: {replay: {path: rival.csv}, spawn: rival}\n"
        "tasks:\n"
        "  blocker: {service: hold, resource: machine, payload: {event: worked}}\n"
        "  worker: {service: work, resource: [tray, machine], payload: {event: worked}}\n"
        "  rival: {service: short, resource: tray, payload: {event: worked}}\n"
        "scenario: [{at: 4, interrupt: {task: worker, cause: recall}}]\n"
        "run: {until: 30}\n"
    )
    outcome = run_yaml_text(
        tmp_path, text, {"blocker": [0] * n, "workers": [1] * n, "rival": [2] * n}
    )
    assert outcome.error is None
    got = [e for e in _events(outcome.records) if not e[1].startswith("blocker")]
    assert got == expected
    app = outcome.app_or_env
    assert app.get_resource("tray").count == 0
    assert app.get_resource("machine").count == 0


def _abandon_reference(n):
    """`n` workers are interrupted at 2 and ask again at once; the unit goes back
    first, so `n` rivals that have waited since 1 are served before them."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=n)
    trace = []

    def worker(name):
        for label in ("work", "again"):
            try:
                with machine.request() as req:
                    yield req
                    trace.append((env.now, name, "started"))
                    yield env.timeout(5 if label == "work" else 1)
                    trace.append((env.now, name, "worked"))
                    return
            except simpy.Interrupt:
                trace.append((env.now, name, "interrupted"))

    def rival(name):
        yield env.timeout(1)
        with machine.request() as req:
            yield req
            trace.append((env.now, name, "started"))
            yield env.timeout(2)
            trace.append((env.now, name, "worked"))

    workers = [env.process(worker(f"workers-{i}")) for i in range(1, n + 1)]
    for i in range(1, n + 1):
        env.process(rival(f"rival-{i}"))

    def recall():
        yield env.timeout(2)
        for process in workers:
            process.interrupt("recall")

    env.process(recall())
    env.run(until=30)
    return trace


@pytest.mark.parametrize("n", [1, 3])
def test_c3_abandon_with_a_next_task_gives_the_unit_back_and_asks_again(tmp_path, n):
    """Pins unchanged behaviour: `on_interrupt: abandon` with `on_interrupt_next`
    gives the unit back, so rivals that wait are served before the task that asks
    again. This is the way to give a unit back after 0.29.0."""
    expected = _abandon_reference(n)
    assert [s for t, k, s in expected if t == 2 and k == "rival-1"] == ["started"]
    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        f"resources: {{machine: {{current_cap: {n}, max_cap: {n}}}}}\n"
        "services: {work: {dist: constant, value: 5}, short: {dist: constant, value: 2}, "
        "one: {dist: constant, value: 1}}\n"
        "arrivals:\n"
        "  workers: {replay: {path: workers.csv}, spawn: worker}\n"
        "  rival: {replay: {path: rival.csv}, spawn: rival}\n"
        "tasks:\n"
        "  worker: {service: work, resource: machine, on_interrupt: abandon, "
        "on_interrupt_next: again, payload: {event: worked}}\n"
        "  again: {service: one, resource: machine, payload: {event: worked}}\n"
        "  rival: {service: short, resource: machine, payload: {event: worked}}\n"
        "scenario: [{at: 2, interrupt: {task: worker, cause: recall}}]\n"
        "run: {until: 30}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"workers": [0] * n, "rival": [1] * n})
    assert outcome.error is None
    assert _events(outcome.records) == expected
    assert outcome.app_or_env.get_resource("machine").count == 0


# ------------------------------------------- a unit the task before kept

KEPT_CASES = {
    # case: (task B waits for, interrupt B at 3, units lost to a breakdown)
    "renege": ("a bench with patience 5", True, False),
    "renege_plain": ("a bench with patience 5", False, False),
    "failed_wait": ("a signal that fails at 5", True, False),
    "failed_wait_plain": ("a signal that fails at 5", False, False),
    "failed_wait_on_fail": ("a signal that fails at 5, then task C", False, False),
    "breakdown": ("a bench", False, True),
}


def _kept_reference(case, n, arrivals=False):
    """`n` entities run A, which keeps a picker, then B. B waits for a bench held
    until 10, or for a signal that fails at 5. `n` rivals ask for a picker at 1. In
    plain SimPy the flow's process holds the picker request from A until B starts or
    fails; the interrupt at 3 reaches B, and a breakdown at 3 to 5 is a request of
    priority -1 on a `PreemptiveResource`, which takes every picker. With
    `arrivals`, the entities of each kind come from one process that makes one per
    step, as a YAML arrival does. The reference fits entities that start together: a
    desflow breakdown interrupts in the order of the resource's `users`, and the
    reference preempts in the order its processes were made."""
    _what, interrupted, breakdown = KEPT_CASES[case]
    env = simpy.Environment()
    picker = simpy.PreemptiveResource(env, capacity=n)
    bench = simpy.Resource(env, capacity=n)
    go = env.event()
    trace, flows = [], []

    def note(name, task, status):
        trace.append((env.now, name, task, status))

    def hog(i):
        with bench.request() as req:
            yield req
            note(f"hog-{i}", "hog", "started")
            yield env.timeout(10)
            note(f"hog-{i}", "hog", "worked")

    def flow(i):
        name = f"a-{i}"
        pick = picker.request()
        yield pick
        note(name, "A", "started")
        yield env.timeout(2)
        note(name, "A", "worked")
        left = 5.0 if case.startswith("renege") else None
        while True:
            since = env.now
            if case.startswith("failed_wait"):
                target = go
            else:
                target = bench.request()
            try:
                if left is None:
                    yield target
                else:
                    yield target | env.timeout(left)
                    if not target.triggered:
                        target.cancel()
                        note(name, "B", "reneged")
                        picker.release(pick)
                        return
            except simpy.Interrupt:
                note(name, "B", "interrupted")
                if target is not go:
                    target.cancel()
                if left is not None:
                    left -= env.now - since
                if pick not in picker.users:
                    pick = None  # the breakdown took it
                continue
            except RuntimeError:
                note(name, "B", "failed")
                if case == "failed_wait_on_fail":
                    # on_fail hands the unit on: C releases it when it starts
                    third = bench.request()
                    yield third
                    note(name, "C", "started")
                    picker.release(pick)
                    yield env.timeout(4)
                    note(name, "C", "worked")
                    bench.release(third)
                    return
                picker.release(pick)
                return
            break
        note(name, "B", "started")
        if pick is not None:
            picker.release(pick)
        yield env.timeout(4)
        note(name, "B", "worked")
        bench.release(target)

    def rival(i):
        if not arrivals:
            yield env.timeout(1)
        with picker.request() as req:
            yield req
            note(f"rival-{i}", "rival", "started")
            yield env.timeout(1)
            note(f"rival-{i}", "rival", "worked")

    def source():
        yield env.timeout(5)
        go.fail(RuntimeError("nope"))

    def recall():
        yield env.timeout(3)
        for process in flows:
            process.interrupt("recall")

    def breaker():
        yield env.timeout(3)
        taken = [picker.request(priority=-1) for _ in range(n)]
        yield env.all_of(taken)
        yield env.timeout(2)
        for req in taken:
            picker.release(req)

    def arrive(make, at, found=None):
        # The rows of one time start in one step, before any entity takes a step
        if at:
            yield env.timeout(at)
        for i in range(1, n + 1):
            process = env.process(make(i))
            if found is not None:
                found.append(process)
        yield from ()

    if arrivals:
        env.process(arrive(hog, 0))
        env.process(arrive(flow, 0, flows))
        env.process(arrive(rival, 1))
    else:
        for i in range(1, n + 1):
            env.process(hog(i))
        for i in range(1, n + 1):
            flows.append(env.process(flow(i)))
        for i in range(1, n + 1):
            env.process(rival(i))
    if interrupted:
        env.process(recall())
    if breakdown:
        env.process(breaker())
    if case.startswith("failed_wait"):
        env.process(source())
    env.run(until=40)
    return trace


def _kept_events(records):
    return [
        (float(r["sim_ts"]), r["key"], r["task"], r["value"].get("status", "worked"))
        for r in records
        if r["stream_type"] == "event"
        and r["value"].get("status") != "queued"
        and r["task"] != "source"
    ]


def _kept_builder(case, n):
    _what, interrupted, breakdown = KEPT_CASES[case]
    app = SimulationContext("C3", factor=0.0)
    app.add_resource("picker", current_cap=n, max_cap=n)
    app.add_resource("bench", current_cap=n, max_cap=n)
    for name, value in (("pick", 2), ("hold", 10), ("work", 4), ("one", 1)):
        app.add_service(name, dist="constant", value=value)
    app.add_signal("go")

    @app.task(service_id="hold", resource_id="bench", name="hog")
    def hog(task_id):
        return {"event": "x"}

    @app.task(
        service_id="pick", resource_id="picker", keep=["picker"], next="B", name="A"
    )
    def first(task_id):
        return {"event": "x"}

    if case == "failed_wait_on_fail":
        second = app.task(
            service_id="work",
            wait={"signal": "go"},
            fail="1 == 2",
            on_fail="C",
            name="B",
        )

        @app.task(service_id="work", resource_id="bench", name="C")
        def third(task_id):
            return {"event": "x"}

    elif case.startswith("failed_wait"):
        second = app.task(service_id="work", wait={"signal": "go"}, name="B")
    elif case.startswith("renege"):
        second = app.task(service_id="work", resource_id="bench", patience=5, name="B")
    else:
        second = app.task(service_id="work", resource_id="bench", name="B")
    second(lambda task_id: {"event": "x"})

    @app.task(service_id="one", resource_id="picker", name="rival")
    def rival(task_id):
        return {"event": "x"}

    @app.task(service_id="one", signal="go", fail="1 == 1", name="source")
    def source(task_id):
        return {"event": "x"}

    def script(context):
        for i in range(1, n + 1):
            context.spawn(hog(0, entity=Entity(f"hog-{i}", "hog")))
        for i in range(1, n + 1):
            context.spawn(first(0, entity=Entity(f"a-{i}", "a")))
        yield context.env.timeout(1)
        for i in range(1, n + 1):
            context.spawn(rival(0, entity=Entity(f"rival-{i}", "rival")))
        yield context.env.timeout(2)
        if interrupted:
            context.interrupt("B", cause="recall")
        if breakdown:
            context.get_resource("picker").fail()
        if case.startswith("failed_wait"):
            yield context.env.timeout(1)
            context.spawn(source(0, entity=Entity("source-1", "source")))
        if breakdown:
            yield context.env.timeout(2)
            context.get_resource("picker").repair()

    app.add_process(script)
    return app


def _kept_yaml(tmp_path, case, n):
    _what, interrupted, breakdown = KEPT_CASES[case]
    wait = {
        "failed_wait": "wait: {signal: go}, ",
        "failed_wait_on_fail": "wait: {signal: go}, fail: '1 == 2', on_fail: C, ",
        "renege": "resource: bench, patience: 5, ",
    }.get(case.split("_plain")[0], "resource: bench, ")
    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        "resources:\n"
        f"  picker: {{current_cap: {n}, max_cap: {n}}}\n"
        f"  bench: {{current_cap: {n}, max_cap: {n}}}\n"
        "services: {pick: {dist: constant, value: 2}, hold: {dist: constant, value: 10}, "
        "work: {dist: constant, value: 4}, one: {dist: constant, value: 1}}\n"
        "signals: {go: {}}\n"
        "arrivals:\n"
        "  hog: {replay: {path: hog.csv}, spawn: hog}\n"
        "  a: {replay: {path: a.csv}, spawn: A}\n"
        "  rival: {replay: {path: rival.csv}, spawn: rival}\n"
        "  source: {replay: {path: source.csv}, spawn: source}\n"
        "tasks:\n"
        "  hog: {service: hold, resource: bench, payload: {event: x}}\n"
        "  A: {service: pick, resource: picker, keep: [picker], next: B, payload: {event: x}}\n"
        f"  B: {{service: work, {wait}payload: {{event: x}}}}\n"
        "  C: {service: work, resource: bench, payload: {event: x}}\n"
        "  rival: {service: one, resource: picker, payload: {event: x}}\n"
        "  source: {service: one, signal: go, fail: '1 == 1', payload: {event: x}}\n"
        + (
            "scenario: [{at: 3, interrupt: {task: B, cause: recall}}]\n"
            if interrupted
            else ""
        )
        + "run: {until: 40, on_unhandled_failure: continue}\n"
    )

    def prepare(app):
        def breaker(context):
            yield context.env.timeout(3)
            context.get_resource("picker").fail()
            yield context.env.timeout(2)
            context.get_resource("picker").repair()

        if breakdown:
            app.add_process(breaker)

    arrivals = {
        "hog": [0] * n,
        "a": [0] * n,
        "rival": [1] * n,
        "source": [4] if case.startswith("failed_wait") else [],
    }
    outcome = run_yaml_text(tmp_path, text, arrivals, prepare)
    assert outcome.error is None
    return outcome


@pytest.mark.parametrize("n", [1, 3])
@pytest.mark.parametrize("case", list(KEPT_CASES))
def test_c3_a_unit_the_task_before_kept_goes_back_when_the_task_ends_without_starting(
    tmp_path, case, n
):
    """A keeps a picker for B. B reneges, or its wait fails, with or without an
    interrupt first, or the picker breaks down while B waits. The unit goes back at
    once (a breakdown: it is gone when the picker is repaired), so a rival that has
    waited for the picker starts then, as with a SimPy process that releases its
    request when it leaves B. With `on_fail: C`, a failed wait hands the unit on, so
    it stays until C starts. The `breakdown` case pins what the old code did too:
    the other cases fail on it."""
    expected = _kept_reference(case, n)
    first_rival = min(
        t for t, k, task, s in expected if task == "rival" and s == "started"
    )
    # C starts at 10, when the hogs leave the bench, and releases the unit then
    if case == "failed_wait_on_fail":
        assert first_rival == 10
    else:
        assert first_rival < 10, case
    got = _kept_events(
        run_builder(_kept_builder(case, n), 40, on_unhandled_failure="continue")
    )
    assert got == expected
    outcome = _kept_yaml(tmp_path, case, n)
    assert _kept_events(outcome.records) == _kept_reference(case, n, arrivals=True)
    app = outcome.app_or_env
    assert app.get_resource("picker").count == 0
    assert app.get_resource("picker").queue == []


def _breakdown_reference(policy, n, arrivals=False):
    """`n` workers hold a tray and a machine and serve 5. The machine breaks at 2 and
    is repaired at 5, which in plain SimPy is a request of priority -1 on a
    `PreemptiveResource` that holds every machine for 3. A worker keeps its tray
    and asks for the machine again; `n` rivals have waited for a tray since 0. The
    reference fits workers that start together: a desflow breakdown interrupts in
    the order of the resource's `users`, and the reference preempts in the order its
    processes were made."""
    env = simpy.Environment()
    tray = simpy.Resource(env, capacity=n)
    machine = simpy.PreemptiveResource(env, capacity=n)
    trace = []

    def note(name, task, status):
        trace.append((env.now, name, task, status))

    def worker(i):
        name = f"w-{i}"
        held = tray.request()
        yield held
        remaining, started, since = 5.0, False, None
        while True:
            req = machine.request()
            try:
                yield req
                note(name, "W", "resumed" if started else "started")
                started, since = True, env.now
                yield env.timeout(remaining)
                note(name, "W", "worked")
                machine.release(req)
                tray.release(held)
                return
            except simpy.Interrupt:
                note(name, "W", "interrupted")
                if since is not None:
                    remaining = (
                        remaining - (env.now - since) if policy == "resume" else 5.0
                    )
                    since = None

    def rival(i):
        with tray.request() as req:
            yield req
            note(f"t-{i}", "T", "started")
            yield env.timeout(1)
            note(f"t-{i}", "T", "worked")

    def breaker():
        yield env.timeout(2)
        taken = [machine.request(priority=-1) for _ in range(n)]
        yield env.all_of(taken)
        yield env.timeout(3)
        for req in taken:
            machine.release(req)

    def arrive(make):
        # The rows of one time start in one step, before any entity takes a step
        for i in range(1, n + 1):
            env.process(make(i))
        yield from ()

    if arrivals:
        env.process(arrive(worker))
        env.process(arrive(rival))
    else:
        for i in range(1, n + 1):
            env.process(worker(i))
        for i in range(1, n + 1):
            env.process(rival(i))
    env.process(breaker())
    env.run(until=40)
    return trace


@pytest.mark.parametrize("n", [1, 3])
@pytest.mark.parametrize("policy", ["resume", "restart"])
def test_c3_a_breakdown_takes_the_broken_unit_and_a_worker_keeps_its_other_unit(
    tmp_path, policy, n
):
    """The machine of a task with a tray and a machine breaks while it is served.
    The machine goes back and the task waits for the repair; the tray, on a resource
    that is not down, stays with the task, so a rival that waits for the tray starts
    when the task ends, as in a SimPy process that asks again for the request it
    lost."""
    expected = _breakdown_reference(policy, n)
    assert min(t for t, k, task, s in expected if task == "T" and s == "started") > 5

    def build():
        app = SimulationContext("C3", factor=0.0)
        app.add_resource("tray", current_cap=n, max_cap=n)
        app.add_resource("machine", current_cap=n, max_cap=n)
        app.add_service("work", dist="constant", value=5)
        app.add_service("one", dist="constant", value=1)
        return app

    app = build()

    @app.task(
        service_id="work",
        resource_id=["tray", "machine"],
        on_interrupt=policy,
        name="W",
    )
    def worker(task_id):
        return {"event": "x"}

    @app.task(service_id="one", resource_id="tray", name="T")
    def rival(task_id):
        return {"event": "x"}

    def script(context):
        for i in range(1, n + 1):
            context.spawn(worker(0, entity=Entity(f"w-{i}", "w")))
        for i in range(1, n + 1):
            context.spawn(rival(0, entity=Entity(f"t-{i}", "t")))
        yield context.env.timeout(2)
        context.get_resource("machine").fail()
        yield context.env.timeout(3)
        context.get_resource("machine").repair()

    app.add_process(script)
    assert _kept_events(run_builder(app, 40)) == expected
    assert app.get_resource("tray").count == 0
    assert app.get_resource("machine").count == 0

    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        f"resources: {{tray: {{current_cap: {n}, max_cap: {n}}}, "
        f"machine: {{current_cap: {n}, max_cap: {n}}}}}\n"
        "services: {work: {dist: constant, value: 5}, one: {dist: constant, value: 1}}\n"
        "arrivals:\n"
        "  w: {replay: {path: w.csv}, spawn: W}\n"
        "  t: {replay: {path: t.csv}, spawn: T}\n"
        "tasks:\n"
        f"  W: {{service: work, resource: [tray, machine], on_interrupt: {policy}, "
        "payload: {event: x}}\n"
        "  T: {service: one, resource: tray, payload: {event: x}}\n"
        "run: {until: 40}\n"
    )

    def prepare(context):
        def breaker(ctx):
            yield ctx.env.timeout(2)
            ctx.get_resource("machine").fail()
            yield ctx.env.timeout(3)
            ctx.get_resource("machine").repair()

        context.add_process(breaker)

    outcome = run_yaml_text(tmp_path, text, {"w": [0] * n, "t": [0] * n}, prepare)
    assert outcome.error is None
    assert _kept_events(outcome.records) == _breakdown_reference(
        policy, n, arrivals=True
    )
    assert outcome.app_or_env.get_resource("tray").count == 0


def _get_reference(n, arrivals=False):
    """`n` workers hold a machine and wait for an item. They are interrupted at 1,
    keep the machine and cancel the get they made, and ask for an item again. `n`
    items arrive at 2 and the workers take them in order."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=n)
    box = simpy.Store(env)
    trace, workers = [], []

    def worker(i):
        name = f"w-{i}"
        held = machine.request()
        yield held
        get = None
        while True:
            try:
                if get is None:
                    get = box.get()
                yield get
                break
            except simpy.Interrupt:
                trace.append((env.now, name, "interrupted"))
                get.cancel()
                get = None
        trace.append((env.now, name, "started"))
        yield env.timeout(2)
        trace.append((env.now, name, "worked"))
        machine.release(held)

    def arrive():
        for i in range(1, n + 1):
            workers.append(env.process(worker(i)))
        yield from ()

    def script():
        yield env.timeout(1)
        for process in workers:
            process.interrupt("recall")
        yield env.timeout(1)
        for i in range(n):
            yield box.put(f"item-{i}")

    env.process(arrive())
    env.process(script())
    env.run(until=20)
    return trace


@pytest.mark.parametrize("n", [1, 3, 10])
def test_c3_an_interrupt_during_a_get_keeps_the_unit_and_cancels_the_get(tmp_path, n):
    """A task holding a machine waits for an item and is interrupted. It keeps the
    machine and its first get is cancelled, so the items put later go to the new
    gets, in order, as in a SimPy process that cancels the get and asks again. The
    old code gave the same result, so this pins that the kept machine does not leave
    a get behind; it fails if the first get is not cancelled."""
    expected = _get_reference(n)
    assert [s for t, k, s in expected if s == "worked"] == ["worked"] * n

    app = SimulationContext("C3", factor=0.0)
    app.add_resource("machine", current_cap=n, max_cap=n)
    app.add_store("box")
    app.add_service("work", dist="constant", value=2)

    @app.task(service_id="work", resource_id="machine", get={"store": "box"}, name="W")
    def worker(task_id):
        return {"event": "x"}

    def script(context):
        for i in range(1, n + 1):
            context.spawn(worker(0, entity=Entity(f"w-{i}", "w")))
        yield context.env.timeout(1)
        context.interrupt("W", cause="recall")
        yield context.env.timeout(1)
        for i in range(n):
            yield context.get_store("box").put(f"item-{i}")

    app.add_process(script)
    got = [(t, k, s) for t, k, task, s in _kept_events(run_builder(app, 20))]
    assert got == expected
    assert app.get_resource("machine").count == 0

    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        f"resources: {{machine: {{current_cap: {n}, max_cap: {n}}}}}\n"
        "stores: {box: {}}\n"
        "services: {work: {dist: constant, value: 2}}\n"
        "arrivals: {w: {replay: {path: w.csv}, spawn: W}, "
        "p: {replay: {path: p.csv}, spawn: P}}\n"
        "tasks:\n"
        "  W: {service: work, resource: machine, get: {store: box}, payload: {event: x}}\n"
        "  P: {put: {store: box}, payload: {event: item}}\n"
        "scenario: [{at: 1, interrupt: {task: W, cause: recall}}]\n"
        "run: {until: 20}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"w": [0] * n, "p": [2] * n})
    assert outcome.error is None
    got = [(t, k, s) for t, k, task, s in _kept_events(outcome.records) if task == "W"]
    assert got == _get_reference(n, arrivals=True)
    assert outcome.app_or_env.get_resource("machine").count == 0


def _end_reference(order, n):
    """`n` workers serve 5 on a machine; `n` other processes end a service at 5 too.
    An interrupt at 5 reaches each worker with no time left, and the worker's loop
    `while left > 0` ends at once, in the interrupt's step, after the other processes
    whose timeouts were made before the interrupt. `order` creates the other processes
    before (`ow`) or after (`wo`) the workers."""
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=n)
    trace, workers = [], []

    def note(name, task, status):
        trace.append((env.now, name, task, status))

    def worker(i):
        name = f"w-{i}"
        req = machine.request()
        yield req
        left, started = 5.0, False
        while left > 0:
            since = env.now
            note(name, "W", "resumed" if started else "started")
            started = True
            try:
                yield env.timeout(left)
                left = 0
            except simpy.Interrupt:
                note(name, "W", "interrupted")
                left -= env.now - since
        note(name, "W", "worked")
        machine.release(req)

    def other(i):
        note(f"o-{i}", "O", "started")
        yield env.timeout(5)
        note(f"o-{i}", "O", "worked")

    def arrive(make, found=None):
        for i in range(1, n + 1):
            process = env.process(make(i))
            if found is not None:
                found.append(process)
        yield from ()

    def recall():
        yield env.timeout(5)
        for process in workers:
            process.interrupt("recall")

    for kind in order:
        if kind == "o":
            env.process(arrive(other))
        else:
            env.process(arrive(worker, workers))
    env.process(recall())
    env.run(until=30)
    return trace


def _preempted_at_its_end_reference(n):
    """`n` jobs of priority 1 serve 5 on a preemptive machine of `n` units, and `n` jobs
    of priority 0 ask at 5 and serve 2. The loop `while left > 0` of a preempted job
    that has no time left ends at once, as the `other_jobs` of SimPy's machine shop
    do, and does not ask for a unit again."""
    env = simpy.Environment()
    machine = simpy.PreemptiveResource(env, capacity=n)
    trace = []

    def job(i):
        left = 5.0
        while left > 0:
            with machine.request(priority=1) as req:
                yield req
                trace.append((env.now, "low", "started"))
                since = env.now
                try:
                    yield env.timeout(left)
                    left = 0
                except simpy.Interrupt:
                    trace.append((env.now, "low", "preempted"))
                    left -= env.now - since
        trace.append((env.now, "low", "worked"))

    def urgent(i):
        yield env.timeout(5)
        with machine.request(priority=0) as req:
            yield req
            trace.append((env.now, "high", "started"))
            yield env.timeout(2)
        trace.append((env.now, "high", "worked"))

    for i in range(1, n + 1):
        env.process(urgent(i))
    for i in range(1, n + 1):
        env.process(job(i))
    env.run(until=30)
    return sorted(trace)


@pytest.mark.parametrize("n", [1, 3, 10])
@pytest.mark.parametrize("order", ["ow", "wo"])
def test_job_interrupted_at_its_end_ends(tmp_path, order, n):
    """A worker interrupted at 5, when its service ends, has no time left and ends in
    the interrupt's step, as SimPy's `while left > 0:` loop does. It asks for no unit
    and publishes no `resumed`. A job preempted at the instant its work ends does the
    same (register row I8)."""
    expected = _end_reference(order, n)
    assert [s for _t, _k, _task, s in expected if s == "resumed"] == []
    assert [t for t, _k, task, s in expected if task == "W" and s == "worked"] == [
        5
    ] * n

    app = SimulationContext("C3", factor=0.0)
    app.add_resource("machine", current_cap=n, max_cap=n)
    app.add_service("work", dist="constant", value=5)

    @app.task(service_id="work", resource_id="machine", name="W")
    def worker(task_id):
        return {"event": "x"}

    @app.task(service_id="work", name="O")
    def other(task_id):
        return {"event": "x"}

    def script(context):
        for kind in order:
            for i in range(1, n + 1):
                if kind == "o":
                    context.spawn(other(0, entity=Entity(f"o-{i}", "o")))
                else:
                    context.spawn(worker(0, entity=Entity(f"w-{i}", "w")))
        yield context.env.timeout(5)
        context.interrupt("W", cause="recall")

    app.add_process(script)
    assert _kept_events(run_builder(app, 30)) == expected
    assert app.get_resource("machine").count == 0

    arrivals = "".join(
        "  {0}: {{replay: {{path: {0}.csv}}, spawn: {1}}}\n".format(
            kind, "O" if kind == "o" else "W"
        )
        for kind in order
    )
    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        f"resources: {{machine: {{current_cap: {n}, max_cap: {n}}}}}\n"
        "services: {work: {dist: constant, value: 5}}\n"
        f"arrivals:\n{arrivals}"
        "tasks:\n"
        "  W: {service: work, resource: machine, payload: {event: x}}\n"
        "  O: {service: work, payload: {event: x}}\n"
        "scenario: [{at: 5, interrupt: {task: W, cause: recall}}]\n"
        "run: {until: 30}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"w": [0] * n, "o": [0] * n})
    assert outcome.error is None
    assert _kept_events(outcome.records) == expected
    assert outcome.app_or_env.get_resource("machine").count == 0


@pytest.mark.parametrize("kind", ["resource", "wait_branch"])
@pytest.mark.parametrize("n", [1, 3, 10])
def test_job_preempted_at_its_end_ends(tmp_path, n, kind):
    """A job preempted at 5, when its work ends, ends there and does not ask for the
    unit again, whether it took the unit as `resource` or with a `wait` request
    branch: the preempting job takes it at 5 and the run ends with every unit free
    (register row I8)."""
    expected = _preempted_at_its_end_reference(n)
    assert [e for e in expected if e[2] == "worked" and e[1] == "low"] == [
        (5, "low", "worked")
    ] * n

    def shown(records):
        return sorted(
            (float(t), {"W": "low", "H": "high"}[task], s)
            for t, _k, task, s in _kept_events(records)
            if task in ("W", "H")
        )

    app = SimulationContext("C3", factor=0.0)
    app.add_resource(
        "machine", current_cap=n, max_cap=n, queue="priority", preempt=True
    )
    app.add_service("five", dist="constant", value=5)
    app.add_service("two", dist="constant", value=2)

    taking = (
        {"resource_id": "machine"}
        if kind == "resource"
        else {"wait": {"request": "machine"}}
    )

    @app.task(service_id="five", priority=1, name="W", **taking)
    def low(task_id):
        return {"event": "x"}

    @app.task(service_id="two", resource_id="machine", priority=0, name="H")
    def high(task_id):
        return {"event": "x"}

    def script(context):
        for i in range(1, n + 1):
            context.spawn(low(0, entity=Entity(f"w-{i}", "w")))
        yield context.env.timeout(5)
        for i in range(1, n + 1):
            context.spawn(high(0, entity=Entity(f"h-{i}", "h")))

    app.add_process(script)
    assert shown(run_builder(app, 30)) == expected
    assert app.get_resource("machine").count == 0
    assert app.get_resource("machine").queue == []

    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        f"resources: {{machine: {{current_cap: {n}, max_cap: {n}, queue: priority, preempt: true}}}}\n"
        "services: {five: {dist: constant, value: 5}, two: {dist: constant, value: 2}}\n"
        "arrivals:\n"
        "  h: {replay: {path: h.csv}, spawn: H}\n"
        "  w: {replay: {path: w.csv}, spawn: W}\n"
        "tasks:\n"
        f"  W: {{service: five, {'resource: machine' if kind == 'resource' else 'wait: {request: machine}'}, "
        "priority: 1, payload: {event: x}}\n"
        "  H: {service: two, resource: machine, priority: 0, payload: {event: x}}\n"
        "run: {until: 30}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"w": [0] * n, "h": [5] * n})
    assert outcome.error is None
    assert shown(outcome.records) == expected
    assert outcome.app_or_env.get_resource("machine").count == 0


def _put_reference(order, n, arrivals=False):
    """`n` workers serve 3 and put into a full store of 1. At 5 they are interrupted,
    cancel the put and make it again in the same step, and `n` other processes put
    at 5. Takes at 8, 9 and on serve the puts in the order they were made, so the
    workers go first."""
    env = simpy.Environment()
    box = simpy.Store(env, capacity=1)
    box.items.append("x")
    trace, workers = [], []

    def note(name, task, status):
        trace.append((env.now, name, task, status))

    def worker(i):
        name = f"w-{i}"
        note(name, "W", "started")
        yield env.timeout(3)
        put = None
        while True:
            try:
                if put is None:
                    put = box.put(name)
                yield put
                break
            except simpy.Interrupt:
                note(name, "W", "interrupted")
                put.cancel()
                put = None
                note(name, "W", "resumed")
        note(name, "W", "worked")

    def other(i):
        yield env.timeout(5)
        yield box.put(f"p-{i}")
        note(f"p-{i}", "P", "worked")

    def taker(k):
        yield env.timeout(8 + k)
        yield box.get()

    def arrive(make, count, found=None):
        for i in range(count):
            process = env.process(make(i + 1 if make is not taker else i))
            if found is not None:
                found.append(process)
        yield from ()

    def recall():
        yield env.timeout(5)
        for process in workers:
            process.interrupt("recall")

    for kind in order:
        if kind == "p":
            env.process(arrive(other, n))
        else:
            env.process(arrive(worker, n, workers))
    for k in range(2 * n):
        env.process(taker(k))
    env.process(recall())
    env.run(until=30)
    return trace


@pytest.mark.parametrize("n", [1, 3])
@pytest.mark.parametrize("order", ["pw", "wp"])
def test_c3_an_interrupt_in_a_put_after_the_service_does_not_serve_again(
    tmp_path, order, n
):
    """A worker interrupted while its put waits, after its service, makes the put again
    at once and does not serve again, so its put is queued before the put of a process
    that starts it at the same instant, as with a SimPy process that cancels the put and
    asks again."""
    expected = _put_reference(order, n)
    puts = [k for _t, k, task, s in expected if task in ("W", "P") and s == "worked"]
    assert puts[:n] == [f"w-{i}" for i in range(1, n + 1)]

    app = SimulationContext("C3", factor=0.0)
    app.add_store("box", current_cap=1, max_cap=1, items=["x"])
    app.add_service("work", dist="constant", value=3)

    @app.task(service_id="work", put={"store": "box"}, name="W")
    def worker(task_id):
        return {"event": "x"}

    @app.task(put={"store": "box"}, name="P")
    def other(task_id):
        return {"event": "x"}

    @app.task(get={"store": "box"}, name="G")
    def taker(task_id):
        return {"event": "x"}

    def script(context):
        for i in range(1, n + 1):
            context.spawn(worker(0, entity=Entity(f"w-{i}", "w")))
        yield context.env.timeout(5)
        context.interrupt("W", cause="recall")
        for i in range(1, n + 1):
            context.spawn(other(0, entity=Entity(f"p-{i}", "p")))
        yield context.env.timeout(3)
        for k in range(2 * n):
            context.spawn(taker(0, entity=Entity(f"g-{k}", "g")))
            yield context.env.timeout(1)

    app.add_process(script)
    got = [e for e in _kept_events(run_builder(app, 30)) if e[2] != "G"]
    assert got == expected
    assert len(app.get_store("box").items) == 1  # x and 2n puts, 2n takes

    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        "stores: {box: {current_cap: 1, max_cap: 1, items: [x]}}\n"
        "services: {work: {dist: constant, value: 3}}\n"
        "arrivals:\n"
        "  w: {replay: {path: w.csv}, spawn: W}\n"
        "  p: {replay: {path: p.csv}, spawn: P}\n"
        "  g: {replay: {path: g.csv}, spawn: G}\n"
        "tasks:\n"
        "  W: {service: work, put: {store: box}, payload: {event: x}}\n"
        "  P: {put: {store: box}, payload: {event: x}}\n"
        "  G: {get: {store: box}, payload: {event: x}}\n"
        "scenario: [{at: 5, interrupt: {task: W, cause: recall}}]\n"
        "run: {until: 30}\n"
    )
    outcome = run_yaml_text(
        tmp_path, text, {"w": [0] * n, "p": [5] * n, "g": [8 + k for k in range(2 * n)]}
    )
    assert outcome.error is None
    got = [e for e in _kept_events(outcome.records) if e[2] != "G"]
    assert got == _put_reference(order, n, arrivals=True)


END_CASES = ("raises", "fail_condition", "no_next")


def _end_of_flow_reference(case, n, arrivals=False):
    """`n` entities hold a picker for a task that keeps it for a next task that does
    not come: the function raises, a `fail` condition holds after the service, or the
    task has no `next`. A SimPy `with` block gives the picker back on each exit, so
    the `n` rivals that ask at 0 start then."""
    env = simpy.Environment()
    picker = simpy.Resource(env, capacity=n)
    trace = []

    def note(name, task, status):
        trace.append((env.now, name, task, status))

    def holder(i):
        name = f"a-{i}"
        with picker.request() as req:
            yield req
            note(name, "A", "started")
            if case == "raises":
                note(name, "A", "failed")
                return
            yield env.timeout(2)
            note(name, "A", "failed" if case == "fail_condition" else "worked")

    def rival(i):
        with picker.request() as req:
            yield req
            note(f"r-{i}", "R", "started")
            yield env.timeout(1)
            note(f"r-{i}", "R", "worked")

    def arrive(make):
        for i in range(1, n + 1):
            env.process(make(i))
        yield from ()

    env.process(arrive(holder))
    env.process(arrive(rival))
    env.run(until=30)
    return trace


@pytest.mark.parametrize("n", [1, 3])
@pytest.mark.parametrize("case", END_CASES)
def test_c3_a_kept_unit_goes_back_when_the_flow_ends_without_a_next_task(
    tmp_path, case, n
):
    """A task keeps a unit for a next task that does not come, because its function
    raises, its `fail` condition holds, or it has no `next`. The unit goes back when
    the task ends, so the rivals that wait for it start then, as a SimPy `with` block
    gives the unit back on each exit."""
    expected = _end_of_flow_reference(case, n)
    assert min(t for t, _k, task, s in expected if task == "R" and s == "started") < 3

    app = SimulationContext("C3", factor=0.0)
    app.add_resource("picker", current_cap=n, max_cap=n)
    app.add_service("pick", dist="constant", value=2)
    app.add_service("one", dist="constant", value=1)
    kwargs = {"fail": "1 == 1"} if case == "fail_condition" else {}

    @app.task(
        service_id="pick",
        resource_id="picker",
        keep=["picker"],
        name="A",
        **kwargs,
    )
    def holder(task_id):
        if case == "raises":
            raise RuntimeError("boom")
        return {"event": "x"}

    @app.task(service_id="one", resource_id="picker", name="R")
    def rival(task_id):
        return {"event": "x"}

    def script(context):
        for i in range(1, n + 1):
            context.spawn(holder(0, entity=Entity(f"a-{i}", "a")))
        for i in range(1, n + 1):
            context.spawn(rival(0, entity=Entity(f"r-{i}", "r")))
        yield context.env.timeout(0)

    app.add_process(script)
    outcome = run_builder(app, 30, raises=True, on_unhandled_failure="continue")
    assert outcome.error is None
    assert _kept_events(outcome.records) == expected
    assert app.get_resource("picker").count == 0

    if case == "raises":
        return  # a function that raises has no YAML form
    fail = ", fail: '1 == 1'" if case == "fail_condition" else ""
    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        f"resources: {{picker: {{current_cap: {n}, max_cap: {n}}}}}\n"
        "services: {pick: {dist: constant, value: 2}, one: {dist: constant, value: 1}}\n"
        "arrivals:\n"
        "  a: {replay: {path: a.csv}, spawn: A}\n"
        "  r: {replay: {path: r.csv}, spawn: R}\n"
        "tasks:\n"
        f"  A: {{service: pick, resource: picker, keep: [picker]{fail}, payload: {{event: x}}}}\n"
        "  R: {service: one, resource: picker, payload: {event: x}}\n"
        "run: {until: 30, on_unhandled_failure: continue}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"a": [0] * n, "r": [0] * n})
    assert outcome.error is None
    assert _kept_events(outcome.records) == _end_of_flow_reference(
        case, n, arrivals=True
    )
    assert outcome.app_or_env.get_resource("picker").count == 0


def _siblings_reference():
    """One entity runs two child flows. K takes a unit of q, serves 1, and goes on to
    D, which waits for a bench until 8 and gives q back when it starts. L serves 3
    and ends. A process waits for both. In plain SimPy the end of L does not touch
    K's request, so a rival that asks for q at 0.5 starts at 8."""
    env = simpy.Environment()
    q = simpy.Resource(env, capacity=1)
    bench = simpy.Resource(env, capacity=1)
    trace = []

    def note(name, task, status):
        trace.append((env.now, name, task, status))

    def hog():
        with bench.request() as req:
            yield req
            note("h", "H", "started")
            yield env.timeout(8)
            note("h", "H", "worked")

    def first():
        pick = q.request()
        yield pick
        yield env.timeout(1)
        with bench.request() as req:
            yield req
            note("e", "D", "started")
            q.release(pick)
            yield env.timeout(2)
            note("e", "D", "worked")

    def second():
        yield env.timeout(3)

    def parent():
        yield env.all_of([env.process(first()), env.process(second())])
        note("e", "P", "started")
        note("e", "P", "worked")

    def rival():
        yield env.timeout(0.5)
        with q.request() as req:
            yield req
            note("r", "R", "started")
            yield env.timeout(1)
            note("r", "R", "worked")

    env.process(hog())
    env.process(parent())
    env.process(rival())
    env.run(until=30)
    return trace


def test_c3_a_child_flow_that_ends_does_not_give_back_a_unit_its_sibling_keeps(
    tmp_path,
):
    """Two child flows of one entity share its kept units. The one with no next task
    ends at 3 and gives back only its own units, so the unit its sibling keeps for D
    stays until D starts at 8, and the rival that waits for it starts then."""
    expected = _siblings_reference()
    assert [(t, k) for t, k, task, s in expected if task == "R" and s == "started"] == [
        (8, "r")
    ]
    shown = {"D", "R", "P", "H"}

    app = SimulationContext("C3", factor=0.0)
    app.add_resource("q", current_cap=1, max_cap=1)
    app.add_resource("bench", current_cap=1, max_cap=1)
    for name, value in (("one", 1), ("three", 3), ("two", 2), ("eight", 8)):
        app.add_service(name, dist="constant", value=value)

    @app.task(service_id="one", resource_id="q", keep=["q"], next="D", name="K")
    def first(task_id):
        return {"event": "x"}

    @app.task(service_id="two", resource_id="bench", name="D")
    def third(task_id):
        return {"event": "x"}

    @app.task(service_id="three", name="L")
    def second(task_id):
        return {"event": "x"}

    @app.task(wait={"all": {"k": {"task": "K"}, "l": {"task": "L"}}}, name="P")
    def parent(task_id):
        return {"event": "x"}

    @app.task(service_id="eight", resource_id="bench", name="H")
    def hog(task_id):
        return {"event": "x"}

    @app.task(service_id="one", resource_id="q", name="R")
    def rival(task_id):
        return {"event": "x"}

    def script(context):
        context.spawn(hog(0, entity=Entity("h", "h")))
        context.spawn(parent(0, entity=Entity("e", "e")))
        yield context.env.timeout(0.5)
        context.spawn(rival(0, entity=Entity("r", "r")))

    app.add_process(script)
    got = [e for e in _kept_events(run_builder(app, 30)) if e[2] in shown]
    assert got == [e for e in expected if e[2] in shown]
    for name in ("q", "bench"):
        assert app.get_resource(name).count == 0
        assert app.get_resource(name).queue == []

    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        "resources: {q: {current_cap: 1, max_cap: 1}, bench: {current_cap: 1, max_cap: 1}}\n"
        "services: {one: {dist: constant, value: 1}, three: {dist: constant, value: 3}, "
        "two: {dist: constant, value: 2}, eight: {dist: constant, value: 8}}\n"
        "arrivals:\n"
        "  h: {replay: {path: h.csv}, spawn: H}\n"
        "  e: {replay: {path: e.csv}, spawn: P}\n"
        "  r: {replay: {path: r.csv}, spawn: R}\n"
        "tasks:\n"
        "  K: {service: one, resource: q, keep: [q], next: D, payload: {event: x}}\n"
        "  D: {service: two, resource: bench, payload: {event: x}}\n"
        "  L: {service: three, payload: {event: x}}\n"
        "  P: {wait: {all: {k: {task: K}, l: {task: L}}}, payload: {event: x}}\n"
        "  H: {service: eight, resource: bench, payload: {event: x}}\n"
        "  R: {service: one, resource: q, payload: {event: x}}\n"
        "run: {until: 30}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"h": [0], "e": [0], "r": [0.5]})
    assert outcome.error is None
    got = [e for e in _kept_events(outcome.records) if e[2] in shown]
    assert [(t, task, s) for t, _k, task, s in got] == [
        (t, task, s) for t, _k, task, s in expected if task in shown
    ]
    for name in ("q", "bench"):
        assert outcome.app_or_env.get_resource(name).count == 0


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


# --------------------------------------------------------- preemption cause


def _preemption_cases():
    """Jobs and the capacity of a preemptive resource, by case."""
    return {
        "one preemption": (
            1,
            [Job("low", 0, 10, priority=2), Job("high", 3, 5, priority=1)],
        ),
        "two at one instant": (
            2,
            [
                Job("low0", 0, 10, priority=3),
                Job("low1", 0.5, 10, priority=2),
                Job("high0", 3, 5, priority=1),
                Job("high1", 3, 5, priority=1),
            ],
        ),
        "tie between users": (
            2,
            [
                Job("low0", 0, 10, priority=1),
                Job("low1", 0, 10, priority=1),
                Job("high", 3, 5, priority=0),
            ],
        ),
        "preempted twice": (
            1,
            [
                Job("low", 0, 10, priority=2),
                Job("mid", 3, 2, priority=1),
                Job("high", 6, 1, priority=0),
            ],
        ),
    }


def test_c3_preemption_cause(tmp_path):
    """The cause of a preemption is SimPy's `Preempted`: who took the unit and since
    when the victim held it. The `preempted` record carries them as `preempted_by`,
    the key of the preempting entity, and `usage_since`; a user preempted twice
    reports the time of its second grant."""
    for name, (cap, jobs) in _preemption_cases().items():
        spec = {
            "current_cap": cap,
            "max_cap": cap,
            "queue": "priority",
            "preempt": True,
        }
        folder = tmp_path / name.replace(" ", "_")
        folder.mkdir()
        reference = run_simpy(
            lambda env, cap=cap: simpy.PreemptiveResource(env, capacity=cap), jobs, 20
        )
        assert reference.causes, name
        for result in run_desflow(folder, spec, jobs, 20):
            assert result.causes == reference.causes, name
            assert result.trace == reference.trace, name
    twice = run_simpy(
        lambda env: simpy.PreemptiveResource(env, capacity=1),
        _preemption_cases()["preempted twice"][1],
        20,
    )
    assert twice.causes == [(3, "low", "mid", 0), (6, "low", "high", 5)]


def test_c3_preemption_record_shape(tmp_path):
    """The `preempted` record holds the resource, the preempting entity and the time
    the victim was granted the unit, in that order after the status."""
    cap, jobs = _preemption_cases()["one preemption"]
    spec = {"current_cap": cap, "max_cap": cap, "queue": "priority", "preempt": True}
    for result in run_desflow(tmp_path, spec, jobs, 20)[1:]:
        (record,) = [
            r["value"]
            for r in result.records
            if r["stream_type"] == "event" and r["value"].get("status") == "preempted"
        ]
        assert record == {
            "path_id": "B3.service.low",
            "sim_id": "B3",
            "service": "low",
            "status": "preempted",
            "resource": "machine",
            "preempted_by": "high-1",
            "usage_since": 0,
        }
        assert list(record) == [
            "path_id",
            "sim_id",
            "service",
            "status",
            "resource",
            "preempted_by",
            "usage_since",
        ]


def test_c3_preemption_by_a_process_that_is_not_a_task():
    """A request made by a process that no task runs preempts too, and the record
    reports no preempting entity."""

    def reference():
        env = simpy.Environment()
        machine = simpy.PreemptiveResource(env, capacity=1)
        causes = []

        def victim():
            with machine.request(priority=2) as req:
                yield req
                try:
                    yield env.timeout(10)
                except simpy.Interrupt as interrupt:
                    causes.append(interrupt.cause)

        def intruder():
            yield env.timeout(3)
            with machine.request(priority=1) as req:
                yield req
                yield env.timeout(1)

        env.process(victim())
        process = env.process(intruder())
        env.run(until=20)
        return causes, process

    causes, process = reference()
    assert causes[0].by is process and causes[0].usage_since == 0

    app = SimulationContext("C3n", factor=0.0)
    app.add_resource(
        "machine", current_cap=1, max_cap=1, queue="priority", preempt=True
    )
    app.add_service("long", dist="constant", value=10)

    @app.task(service_id="long", resource_id="machine", priority=2)
    def low(task_id):
        return {"event": "done"}

    def script(context):
        context.spawn(low(context.next_instance("low"), entity=Entity("low-1", "low")))
        yield context.env.timeout(3)
        machine = context.get_resource("machine")
        with machine.request(priority=1) as req:
            yield req
            yield context.env.timeout(1)

    app.add_process(script)
    records = run_builder(app, 20)
    (value,) = [
        r["value"]
        for r in records
        if r["stream_type"] == "event" and r["value"].get("status") == "preempted"
    ]
    assert value["preempted_by"] is None and value["usage_since"] == 0


def test_c3_an_error_outside_the_function_gives_back_a_kept_unit_in_a_child_flow():
    """A child flow's task that keeps a unit gives it back when an error raised outside
    its function ends the flow, as a SimPy `with` block does, so a rival is served."""
    app = SimulationContext("C3", factor=0.0)
    app.add_resource("picker", current_cap=1, max_cap=1)
    app.add_service("broken", dist="constant", value=2)
    app.add_service("one", dist="constant", value=1)
    started = []

    @app.task(service_id="broken", resource_id="picker", keep=["picker"], name="kid")
    def kid(task):
        return {}

    @app.task(wait={"any": {"k": {"task": "kid"}, "t": {"timeout": 1}}}, name="parent")
    def parent(task):
        return {}

    @app.task(service_id="one", resource_id="picker", name="rival")
    def rival(task):
        started.append(app.env.now)
        return {}

    def start(ctx):
        sample = ctx.sampler.sample

        def raising(config):
            if getattr(config, "value", None) == 2:
                raise ValueError("sampler")
            return sample(config)

        ctx.sampler.sample = raising
        ctx.spawn(parent(0, entity=Entity("e", "e", {})))
        ctx.spawn(rival(0, entity=Entity("r", "r", {})))
        yield ctx.env.timeout(0)

    app.add_process(start)
    run_builder(app, 5, on_unhandled_failure="continue")
    assert started == [0.0]
    assert app.get_resource("picker").count == 0


# ----------------------------------------- an instance is reachable from its spawn


def _spawn_then_interrupt_reference(n, plain=False):
    """`n` parents each start a child at 1 and interrupt the children alive at once.
    The child runs to its first `yield` first, then the interrupt reaches it there
    (SimPy's `Initialize` is scheduled before the `Interruption`). The child ends on
    the interrupt, as `on_interrupt: abandon` does. With `plain` the child has no
    `yield`, ends in its first step, and the interrupt finds nothing alive."""
    env = simpy.Environment()
    trace, alive = [], []

    def child(name):
        if plain:
            trace.append((env.now, name, "C", "worked"))
            return
        trace.append((env.now, name, "C", "started"))
        try:
            yield env.timeout(5)
            trace.append((env.now, name, "C", "worked"))
        except simpy.Interrupt:
            trace.append((env.now, name, "C", "interrupted"))

    def parent(i):
        yield env.timeout(1)
        alive.append(env.process(child(f"p-{i}")))
        for process in list(alive):
            if process.is_alive:
                process.interrupt("stop")

    for i in range(1, n + 1):
        env.process(parent(i))
    env.run(until=30)
    return trace


def _interrupt_yaml(n, plain):
    child = "{service: long, on_interrupt: abandon, on_interrupt_next: D, payload: {event: x}}"
    if plain:
        child = "{on_interrupt: abandon, on_interrupt_next: D, payload: {event: x}}"
    actions = "on:\n      - spawn: {task: C}\n      - interrupt: {task: C, cause: stop}"
    return (
        "simulation: {sim_id: C3, factor: 0}\n"
        "services: {one: {dist: constant, value: 1}, long: {dist: constant, value: 5}}\n"
        "arrivals:\n"
        "  p: {replay: {path: p.csv}, spawn: P}\n"
        "tasks:\n"
        "  P:\n"
        "    service: one\n"
        f"    {actions}\n"
        "    payload: {event: x}\n"
        f"  C: {child}\n"
        "  D: {payload: {event: x}}\n"
        "run: {until: 30}\n"
    )


def _interrupt_builder(n, form, plain):
    app = SimulationContext("C3", factor=0.0)
    app.add_service("long", dist="constant", value=5)

    if plain:

        @app.task(name="C", on_interrupt="abandon", on_interrupt_next="D")
        def child(task_id):
            return {"event": "x"}

    else:

        @app.task(
            service_id="long", name="C", on_interrupt="abandon", on_interrupt_next="D"
        )
        def child(task_id):
            return {"event": "x"}

    @app.task(name="D")
    def after(task_id):
        return {"event": "x"}

    def script(context):
        yield context.env.timeout(1)
        for i in range(1, n + 1):
            entity = Entity(f"p-{i}", "p")
            if form == "builder_spawn":
                context.spawn(child(0, entity=entity))
            else:
                context.call("C", entity=entity)
            context.interrupt("C", cause="stop")

    app.add_process(script)
    return app


@pytest.mark.parametrize("plain", [False, True])
@pytest.mark.parametrize("n", [1, 3, 10])
@pytest.mark.parametrize("form", ["on", "builder_spawn", "builder_call"])
def test_interrupt_reaches_an_instance_of_this_step(tmp_path, form, n, plain):
    """A task instance can be interrupted in the step it starts in: it runs up to its
    first `yield`, and the interrupt reaches it there, as SimPy's process does. This
    holds for a `spawn` and an `interrupt` listed one after the other in `on:`, for
    `context.spawn` and for `context.call` (register row I9). A task's own `spawn:`
    and `interrupt:` keys run in a fixed order, interrupt first, so they cannot do it. A child with no `yield` ends in its
    first step, and the interrupt does nothing."""
    expected = _spawn_then_interrupt_reference(n, plain)
    if form.startswith("builder"):
        app = _interrupt_builder(n, form, plain)
        got = [e for e in _kept_events(run_builder(app, 30)) if e[2] == "C"]
    else:
        outcome = run_yaml_text(tmp_path, _interrupt_yaml(n, plain), {"p": [0] * n})
        assert outcome.error is None
        got = [e for e in _kept_events(outcome.records) if e[2] == "C"]

    def shape(rows):
        return sorted((t, task, s) for t, _k, task, s in rows)

    assert shape(got) == shape(expected)


# ------------------------------------------- a run failure runs no `on:` action

D3_TEXT = (
    "simulation: {{sim_id: D3, factor: 0}}\n"
    "services: {{one: {{dist: constant, value: 1}}}}\n"
    "arrivals:\n"
    "  a: {{replay: {{path: a.csv}}, spawn: {start}}}\n"
    "tasks:\n"
    "  W:\n"
    "    wait: {{task: A}}\n"
    "    on:\n"
    "      - spawn: {{task: N}}\n"
    "    payload: {{event: x}}\n"
    "  A:\n"
    "    service: one\n"
    '    next: {{when: [{{condition: "{condition}", task: B}}], default: B}}\n'
    "    on:\n"
    "      - spawn: {{task: M}}\n"
    "    payload: {{event: x}}\n"
    "  B: {{payload: {{event: x}}}}\n"
    "  M: {{payload: {{event: x}}}}\n"
    "  N: {{payload: {{event: x}}}}\n"
    "run: {{until: 30}}\n"
)


def _no_statement_after_a_raise_reference(n, fails, waits):
    """`n` processes run a child that raises when `fails`. The child, or a parent that
    yields the child, starts a marker process in the statement after the raise, which
    only runs when nothing raised. Returns the markers that ran and the error."""
    env = simpy.Environment()
    ran = []

    def child():
        yield env.timeout(1)
        if fails:
            raise ZeroDivisionError("division by zero")
        ran.append("child marker")

    def parent():
        yield env.process(child())
        ran.append("parent marker")

    for _ in range(n):
        env.process(parent() if waits else child())
    error = None
    try:
        env.run(until=30)
    except ZeroDivisionError as exc:
        error = exc
    return sorted(set(ran)), error


@pytest.mark.parametrize("n", [1, 3, 10])
@pytest.mark.parametrize("start", ["A", "W"])
@pytest.mark.parametrize("fails", [True, False])
def test_on_actions_do_not_run_after_a_run_failure(tmp_path, fails, start, n):
    """A task that ends in a run failure (here a `next` condition that cannot be
    evaluated), and a parent whose wait fails because its child did, run none of
    their `on:` actions: like a SimPy process that raises, they do not reach the
    statements after the error. The same tasks without a failure run them
    (register row D3)."""
    waits = start == "W"
    ran, error = _no_statement_after_a_raise_reference(n, fails, waits)
    assert (error is not None) is fails
    condition = "1 / 0 > 1" if fails else "1 / 1 > 1"
    text = D3_TEXT.format(start=start, condition=condition)
    outcome = run_yaml_text(tmp_path, text, {"a": [0] * n})
    tasks = {r["task"] for r in outcome.records if r["stream_type"] == "event"}
    markers = {"M": "child marker", "N": "parent marker"}
    assert sorted(markers[t] for t in tasks & set(markers)) == ran
    if fails:
        assert "division by zero" in str(outcome.error)
    else:
        assert outcome.error is None


@pytest.mark.parametrize("n", [1, 3, 10])
@pytest.mark.parametrize("form", ["root", "child", "handled"])
def test_on_actions_do_not_run_after_a_task_function_raises(form, n):
    """A task function that raises ends its task in a run failure, so its `on:`
    actions do not run, as the statement after a raise does not in SimPy. When the
    task has an `on_fail` that handles the failure, the actions run (register row
    D3)."""
    app = SimulationContext("D3", factor=0.0)
    app.add_service("one", dist="constant", value=1)

    @app.task(service_id="one", name="M")
    def marker(task_id):
        return {"event": "m"}

    @app.task(name="H")
    def handler(task_id):
        return {"event": "h"}

    options = {"fail": "False", "on_fail": "H"} if form == "handled" else {}

    @app.task(name="B", on=[{"spawn": {"task": "M"}}], **options)
    def boom(task_id):
        raise ValueError("boom")

    @app.task(wait={"task": "B"}, name="P")
    def parent(task_id):
        return {"event": "p"}

    def script(context):
        for i in range(n):
            if form == "child":
                context.spawn(parent(0, entity=Entity(f"p-{i}", "p")))
            else:
                context.spawn(boom(0, entity=Entity(f"p-{i}", "p")))
        yield context.env.timeout(0)

    app.add_process(script)
    outcome = run_builder(app, 30, raises=True, on_unhandled_failure="continue")
    ran = {r["task"] for r in outcome.records if r["stream_type"] == "event"}
    if form == "handled":
        assert "M" in ran
    else:
        assert "M" not in ran


# ------------------------------------------------------ interrupts in entry order


def _entry_order_reference(entries):
    """Workers enter at the given times and one interrupt at 20 reaches them in the
    order they entered, as a loop over their processes does."""
    env = simpy.Environment()
    order, workers = [], []

    def worker(name):
        try:
            yield env.timeout(30)
        except simpy.Interrupt:
            order.append(name)

    def enter(name, at):
        yield env.timeout(at)
        workers.append(env.process(worker(name)))

    def recall():
        yield env.timeout(20)
        for process in workers:
            process.interrupt("recall")

    for i, at in enumerate(entries, start=1):
        env.process(enter(f"w-{i}", at))
    env.process(recall())
    env.run(until=40)
    return order


@pytest.mark.parametrize("n", [1, 3, 10])
def test_interrupts_arrive_in_entry_order(tmp_path, n):
    """Instances reached by one `interrupt` receive it in the order they entered the
    task, tied entries in file order. Half of the workers enter at 0 and the rest at
    5 (SimPy: a loop over the processes in the order they were made)."""
    entries = [0 if i <= n // 2 else 5 for i in range(1, n + 1)]
    expected = _entry_order_reference(entries)
    assert len(expected) == n

    text = (
        "simulation: {sim_id: C3, factor: 0}\n"
        "services: {long: {dist: constant, value: 30}}\n"
        "arrivals:\n"
        "  w: {replay: {path: w.csv}, spawn: W}\n"
        "tasks:\n"
        "  W: {service: long, on_interrupt: abandon, on_interrupt_next: D, "
        "payload: {event: x}}\n"
        "  D: {payload: {event: x}}\n"
        "scenario: [{at: 20, interrupt: {task: W, cause: recall}}]\n"
        "run: {until: 40}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {"w": entries})
    assert outcome.error is None
    started = [
        r["key"]
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["task"] == "W"
        and r["value"].get("status") == "started"
    ]
    got = [
        r["key"]
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["task"] == "W"
        and r["value"].get("status") == "interrupted"
    ]
    assert got == started
    assert got == expected

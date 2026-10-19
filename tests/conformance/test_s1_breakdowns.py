"""S1 breakdowns: a machine that fails during service and is repaired.

Jobs arrive every 3 seconds from 3 and take 5 seconds on one machine, which fails at
15 and is repaired at 19, on a calendar clock. A plain SimPy reference models the
breakdown as a repair request of the highest priority on a `PreemptiveResource`:
it preempts the job in service (`work: interrupt`), or waits for it to finish and
holds the machine until 19 (`work: finish`). desflow's down state must give the
same times in all three APIs, under each `on_interrupt` policy.
"""

import itertools

import simpy

from desflow import (
    BreakdownConfig,
    CapacityConfig,
    DistributionConfig,
    DynamicResource,
    Sampler,
    SimParameter,
    run_breakdowns,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder, run_low_level, run_yaml

UNTIL = 30
POLICIES = ("resume", "restart", "abandon")


def _trace(records):
    """(time, job, status) of every job event, and the machine's down and repaired."""
    out = []
    for r in records:
        if r["stream_type"] != "event":
            continue
        status = r["value"].get("status", "finished")
        if status == "queued":
            continue
        if r["key"] == "machine":
            out.append((r["sim_ts"], "machine", status))
        else:
            out.append((r["sim_ts"], int(r["key"].split("-")[1]), status))
    return out


def _reference(policy, work="interrupt", cap_at=None):
    """The same trace from plain SimPy. `cap_at`: a second unit added at that time."""
    env = simpy.Environment()
    machine = simpy.PreemptiveResource(env, capacity=2 if cap_at else 1)
    trace = []

    def blocker():
        # The planned capacity is 1 until cap_at: the second unit is held until then
        with machine.request(priority=-5) as req:
            yield req
            yield env.timeout(cap_at)

    def job(n):
        remaining, started = 5.0, False
        while True:
            since = None
            try:
                with machine.request(priority=0) as req:
                    yield req
                    trace.append((env.now, n, "resumed" if started else "started"))
                    started, since = True, env.now
                    yield env.timeout(remaining)
                    trace.append((env.now, n, "finished"))
                    return
            except simpy.Interrupt:
                trace.append((env.now, n, "interrupted"))
                if policy == "abandon":
                    return
                if since is not None:
                    remaining = (
                        remaining - (env.now - since) if policy == "resume" else 5.0
                    )

    def source():
        for n in itertools.count(1):
            yield env.timeout(3)
            env.process(job(n))

    def breakdown():
        yield env.timeout(15)
        trace.append((env.now, "machine", "down"))
        held = [
            machine.request(priority=-1, preempt=work == "interrupt")
            for _ in range(2 if cap_at else 1)
        ]
        yield env.timeout(4)
        for req in held:
            if req.triggered:
                machine.release(req)
            else:
                req.cancel()
        trace.append((env.now, "machine", "repaired"))

    if cap_at:
        env.process(blocker())
    env.process(breakdown())
    env.process(source())
    env.run(until=UNTIL)
    return trace


def _params(work="interrupt"):
    return SimParameter(
        sim_id="S1",
        resources={"machine": CapacityConfig(current_cap=1, max_cap=2)},
        service={"work": DistributionConfig(dist="constant", value=5)},
        arrival={"jobs": DistributionConfig(dist="constant", value=3)},
        breakdowns={
            "machine": BreakdownConfig(
                time_to_failure=DistributionConfig(dist="constant", value=15),
                repair=DistributionConfig(dist="constant", value=4),
                work=work,
            )
        },
    )


def _low_level(policy, work="interrupt", cap_at=None):
    def start(env):
        machine = DynamicResource(env, "S1", "machine")
        sampler = Sampler(seed=1)

        def job(n):
            key, remaining, started = f"jobs-{n}", None, False

            def publish(value):
                env.publish_event(key, value, task="work", instance=n - 1)

            while True:
                since = None
                request = machine.request()
                try:
                    yield request
                    publish({"status": "resumed" if started else "started"})
                    started = True
                    if remaining is None:
                        remaining = sampler.sample(
                            env.registry.get_config("S1.service.work")
                        )
                    since = env.now
                    yield env.timeout(remaining)
                    publish({"event": "done"})
                    machine.release(request)
                    return
                except simpy.Interrupt as interrupt:
                    request.cancel()
                    machine.release(request)
                    publish({"status": "interrupted", "cause": interrupt.cause})
                    if policy == "abandon":
                        return
                    if since is not None:
                        remaining = (
                            remaining - (env.now - since)
                            if policy == "resume"
                            else None
                        )

        def source():
            for n in itertools.count(1):
                yield env.timeout(3)
                env.process(job(n))

        def capacity():
            yield env.timeout(cap_at)
            env.registry.update("S1.resources.machine.current_cap", 2)

        env.process(run_breakdowns(env, machine, sampler, work=work))
        env.process(source())
        if cap_at:
            env.process(capacity())

    return run_low_level(_params(work), start, UNTIL)[0]


def _builder(policy, work="interrupt", cap_at=None):
    app = SimulationContext("S1", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=2)
    app.add_breakdown(
        "machine",
        time_to_failure={"dist": "constant", "value": 15},
        repair={"dist": "constant", "value": 4},
        work=work,
    )
    app.add_service("work", dist="constant", value=5)
    app.add_arrival("jobs", dist="constant", value=3)
    if cap_at:
        app.add_step(cap_at, path="S1.resources.machine.current_cap", value=2)

    @app.task(service_id="work", resource_id="machine", on_interrupt=policy)
    def work_task(task_id):
        return {"event": "done"}

    @app.arrival_loop("jobs")
    def jobs(context):
        while True:
            entity = yield context.wait_for_arrival("jobs")
            context.spawn(work_task(context.next_instance("work_task"), entity=entity))

    return run_builder(app, UNTIL)


def _check(policy, work="interrupt", cap_at=None, blueprint=None):
    expected = [(float(t), n, s) for t, n, s in _reference(policy, work, cap_at)]
    for records in (
        _low_level(policy, work, cap_at),
        _builder(policy, work, cap_at),
        *([run_yaml(blueprint)[0]] if blueprint else []),
    ):
        assert _trace(records) == expected
    return expected


def test_s1_failure_during_service():
    """The machine fails at 15 with job 3 in service; job 4 is served first after
    the repair at 19, then job 3 resumes, restarts or is gone."""
    for policy in POLICIES:
        trace = _check(policy, blueprint=f"s1_breakdown_{policy}.yaml")
        assert (15.0, 3, "interrupted") in trace
        assert (19.0, 4, "started") in trace
    resumed = _check("resume")
    assert (24.0, 3, "resumed") in resumed and (27.0, 3, "finished") in resumed


def test_s1_failure_during_service_work_finishes():
    """With work: finish, job 3 finishes at 18, and job 4 waits for the repair."""
    trace = _check("resume", work="finish", blueprint="s1_breakdown_finish.yaml")
    assert (18.0, 3, "finished") in trace
    assert (19.0, 4, "started") in trace


def test_s1_planned_change_during_repair():
    """A second unit planned at 17, during the repair, does not end it: at 19 two
    jobs start together."""
    trace = _check("resume", cap_at=17)
    assert [t for t, n, s in trace if s in ("started", "resumed") and t == 19.0] == [
        19.0,
        19.0,
    ]

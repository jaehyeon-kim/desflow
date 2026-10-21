"""One job model run in plain SimPy, the low-level API, the builder and YAML.

The Batch 3 resource tests compare the same jobs on one resource across the four.
A job asks for the resource at `at`, holds it for `hold` seconds, goes back to the
queue with what is left if a preemption takes the unit, gives up after `patience`
seconds of waiting, and balks if the waiting line refuses it. Each run returns a
`Result` whose trace is `[(time, job, status)]` with the statuses `started`,
`preempted`, `resumed`, `finished`, `reneged` and `balked`.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import simpy
import yaml

from desflow import CapacityConfig, DynamicResource, Entity, SimParameter
from desflow.core.context import SimulationContext
from tests.conformance import harness

Trace = List[Tuple[float, str, str]]
Observe = Tuple[List[float], Callable[[Any], Any]]


@dataclass
class Job:
    """A user of the resource: when it asks, how long it holds a unit, the priority
    and preempt flag of its request (None: the request names no priority) and how
    long it waits for a unit before it gives up (None: for ever)."""

    name: str
    at: float
    hold: float
    priority: Optional[float] = None
    preempt: bool = True
    patience: Optional[float] = None

    @property
    def request_args(self) -> Dict[str, Any]:
        """The arguments of its `request()` call."""
        args: Dict[str, Any] = {}
        if self.priority is not None:
            args["priority"] = self.priority
        if not self.preempt:
            args["preempt"] = False
        return args


@dataclass
class Result:
    """What a run left: the job trace, the resource, the readings an `observe` took,
    the preemption causes as `(time, victim, preempter, usage_since)` and the
    records of a builder or YAML run."""

    trace: Trace
    resource: Any
    readings: List[Tuple[float, Any]] = field(default_factory=list)
    causes: List[Tuple[float, str, Optional[str], float]] = field(default_factory=list)
    records: List[Dict[str, Any]] = field(default_factory=list)


def who(request: Any) -> str:
    """The job a request belongs to: a low-level or SimPy request is tagged, and a
    task's request is made by the process named after the task."""
    return getattr(request, "job", None) or request.proc.name


def job_process(
    env: Any,
    machine: Any,
    job: Job,
    trace: Trace,
    causes: Optional[List[Any]] = None,
) -> Any:
    """The job as a process on a SimPy-style resource, which a low-level run and a
    plain SimPy run share."""
    yield env.timeout(job.at)
    remaining, started, since = job.hold, False, 0.0
    deadline = None if job.patience is None else env.now + job.patience
    while True:
        try:
            request = machine.request(**job.request_args)
        except RuntimeError:
            trace.append((env.now, job.name, "balked"))
            return
        request.job = job.name
        try:
            with request:
                if deadline is None:
                    yield request
                else:
                    yield request | env.timeout(max(0.0, deadline - env.now))
                    if not request.triggered:
                        trace.append((env.now, job.name, "reneged"))
                        return
                trace.append((env.now, job.name, "resumed" if started else "started"))
                started, since = True, env.now
                yield env.timeout(remaining)
                trace.append((env.now, job.name, "finished"))
                return
        except simpy.Interrupt as interrupt:
            trace.append((env.now, job.name, "preempted"))
            if causes is not None:
                causes.append((env.now, job.name, interrupt.cause))
            remaining -= env.now - since
            deadline = None


def _resolve_causes(
    causes: List[Any], processes: Dict[str, Any]
) -> List[Tuple[float, str, Optional[str], float]]:
    """The causes of a SimPy-style run with the preempter as a job name or None."""
    names = {process: name for name, process in processes.items()}
    return [
        (when, victim, names.get(cause.by), cause.usage_since)
        for when, victim, cause in causes
    ]


def _lower_simpy(env: Any, machine: Any, when: float, capacity: int) -> Any:
    yield env.timeout(when)
    machine._capacity = capacity


def _lower_registry(env: Any, sim_id: str, when: float, capacity: int) -> Any:
    yield env.timeout(when)
    env.registry.update(f"{sim_id}.resources.machine.current_cap", capacity)


def _sample(env: Any, machine: Any, observe: Observe, readings: List[Any]) -> Any:
    times, read = observe
    for moment in times:
        yield env.timeout(moment - env.now)
        readings.append((env.now, read(machine)))


def run_simpy(
    make: Callable[[simpy.Environment], Any],
    jobs: List[Job],
    until: float,
    observe: Optional[Observe] = None,
    lower: Optional[Tuple[float, int]] = None,
) -> Result:
    """Runs the jobs on the resource `make(env)` builds. `lower` is a time and a
    capacity the resource is set to at that time."""
    env = simpy.Environment()
    machine = make(env)
    result = Result([], machine)
    if lower is not None:
        env.process(_lower_simpy(env, machine, *lower))
    causes: List[Any] = []
    processes = {
        job.name: env.process(job_process(env, machine, job, result.trace, causes))
        for job in jobs
    }
    if observe is not None:
        env.process(_sample(env, machine, observe, result.readings))
    env.run(until=until)
    result.causes = _resolve_causes(causes, processes)
    return result


def run_low_level(
    spec: Dict[str, Any],
    jobs: List[Job],
    until: float,
    observe: Optional[Observe] = None,
    sim_id: str = "B3",
) -> Result:
    """Runs the jobs on a `DynamicResource`, or the class `spec["type"]` names."""
    kwargs = {
        key: spec[key] for key in ("queue", "preempt", "queue_limit") if key in spec
    }
    lower = spec.get("lower")
    caps = None
    if "current_cap" in spec or "max_cap" in spec:
        caps = CapacityConfig(spec.get("current_cap"), spec.get("max_cap"))
    resource_type = spec.get("type") or DynamicResource
    result = Result([], None)
    causes: List[Any] = []
    processes: Dict[str, Any] = {}

    def start(env: Any) -> None:
        machine = resource_type(env, sim_id, "machine", **kwargs)
        result.resource = machine
        for job in jobs:
            processes[job.name] = env.process(
                job_process(env, machine, job, result.trace, causes)
            )
        if lower is not None:
            env.process(_lower_registry(env, sim_id, *lower))
        if observe is not None:
            env.process(_sample(env, machine, observe, result.readings))

    params = SimParameter(sim_id=sim_id, resources={"machine": caps}) if caps else None
    harness.run_low_level(params, start, until)
    result.causes = _resolve_causes(causes, processes)
    return result


def builder_app(
    spec: Dict[str, Any], jobs: List[Job], sim_id: str = "B3"
) -> SimulationContext:
    """The jobs as builder tasks, one per job, started by a process at their times."""
    app = SimulationContext(sim_id, factor=0.0)
    resource = {k: v for k, v in spec.items() if k != "lower"}
    app.add_resource("machine", **resource)
    if "lower" in spec:
        when, capacity = spec["lower"]
        app.add_scenario(
            [
                {
                    "at": when,
                    "path": f"{sim_id}.resources.machine.current_cap",
                    "value": capacity,
                }
            ]
        )
    tasks = {}
    for job in jobs:
        app.add_service(job.name, dist="constant", value=job.hold)

        def work(task_id: Any) -> Dict[str, str]:
            return {"event": "done"}

        tasks[job.name] = app.task(
            service_id=job.name,
            resource_id="machine",
            name=job.name,
            priority=job.priority,
            preempt=job.preempt,
            patience=job.patience,
        )(work)

    def script(context: SimulationContext) -> Any:
        for job in sorted(jobs, key=lambda j: j.at):
            if job.at > context.env.now:
                yield context.env.timeout(job.at - context.env.now)
            entity = Entity(f"{job.name}-1", job.name)
            context.spawn(
                tasks[job.name](context.next_instance(job.name), entity=entity)
            )

    app.add_process(script)
    return app


def yaml_app(
    tmp_path: Any,
    spec: Dict[str, Any],
    jobs: List[Job],
    until: float,
    sim_id: str = "B3",
) -> SimulationContext:
    """The same jobs as a blueprint: one task per job, started by a one-row replay."""
    resource = {k: v for k, v in spec.items() if k not in ("type", "lower")}
    if "type" in spec:
        resource["type"] = "__TYPE__"
    document: Dict[str, Any] = {
        "simulation": {"sim_id": sim_id, "factor": 0},
        "resources": {"machine": resource},
        "services": {job.name: {"dist": "constant", "value": job.hold} for job in jobs},
        "arrivals": {
            job.name: {"replay": {"path": f"{job.name}.csv"}, "spawn": job.name}
            for job in jobs
        },
        "tasks": {job.name: _yaml_task(job) for job in jobs},
        "run": {"until": until},
    }
    if "lower" in spec:
        when, capacity = spec["lower"]
        document["scenario"] = [
            {
                "at": when,
                "path": f"{sim_id}.resources.machine.current_cap",
                "value": capacity,
            }
        ]
    for job in jobs:
        (tmp_path / f"{job.name}.csv").write_text(f"time\n{job.at}\n")
    text = yaml.safe_dump(document, sort_keys=False)
    if "type" in spec:
        cls = spec["type"]
        text = text.replace("__TYPE__", f"!python {cls.__module__}.{cls.__name__}")
    path = tmp_path / "model.yaml"
    path.write_text(text)
    return SimulationContext.from_yaml(path)


def _yaml_task(job: Job) -> Dict[str, Any]:
    task: Dict[str, Any] = {
        "service": job.name,
        "resource": "machine",
        "payload": {"event": "done"},
    }
    if job.priority is not None:
        task["priority"] = job.priority
    if not job.preempt:
        task["preempt"] = False
    if job.patience is not None:
        task["patience"] = job.patience
    return task


def run_app(
    app: SimulationContext,
    until: Optional[float] = None,
    observe: Optional[Observe] = None,
) -> Result:
    """Runs a builder or YAML model."""
    egress = harness.RecordingEgress()
    app.add_egress(egress)
    readings: List[Tuple[float, Any]] = []
    if observe is not None:
        times, read = observe
        func, readings = harness.probe(
            times, lambda context: read(context.get_resource("machine"))
        )
        app.add_process(func)
    app.run(until=until)
    result = Result(
        records_trace(egress.records),
        app.get_resource("machine"),
        readings,
        records=egress.records,
    )
    for record in egress.records:
        value = record["value"]
        if record["stream_type"] == "event" and value.get("status") == "preempted":
            by = value["preempted_by"]
            result.causes.append(
                (
                    float(record["sim_ts"]),
                    _job_of(record["key"]),
                    None if by is None else _job_of(by),
                    value["usage_since"],
                )
            )
    return result


def _job_of(key: str) -> str:
    return key.rsplit("-", 1)[0]


def records_trace(records: List[Dict[str, Any]]) -> Trace:
    """The job trace of a desflow run: `queued` is left out, a finished event has no
    status, and a key `<job>-1` names its job."""
    trace: Trace = []
    for record in records:
        if record["stream_type"] != "event":
            continue
        status = record["value"].get("status", "finished")
        if status != "queued":
            trace.append((float(record["sim_ts"]), _job_of(record["key"]), status))
    return trace


def run_desflow(
    tmp_path: Any,
    spec: Dict[str, Any],
    jobs: List[Job],
    until: float,
    observe: Optional[Observe] = None,
) -> List[Result]:
    """Runs the jobs in the low-level API, the builder and YAML, in that order."""
    results = [run_low_level(spec, jobs, until, observe)]
    results.append(run_app(builder_app(spec, jobs), until, observe))
    results.append(run_app(yaml_app(tmp_path, spec, jobs, until), None, observe))
    return results

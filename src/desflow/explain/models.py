"""The shapes `desflow explain` builds: the graph of a model, and its analysis.

`Explanation` is what `context.analyze()` returns, what `render_terminal` prints and
what `to_json` writes. Its JSON form is experimental through 1.x, and
`explain.schema.json` beside this file is its committed schema, which a test keeps
equal to `Explanation.model_json_schema()`.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict

NodeKind = Literal[
    "arrival", "task", "resource", "store", "container", "opaque", "exit"
]
EdgeKind = Literal["spawn", "uses", "next", "takes", "gives"]
Status = Literal["OK", "CONGESTED", "UNSTABLE"]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Node(_Model):
    """One part of the model.

    Attributes:
        id: Unique in the graph: the kind and the name, such as `task:pack_order`.
        kind: `arrival`, `task`, `resource`, `store`, `container`, `opaque` or
            `exit`.
        label: The name in the model, such as `pack_order`.
        opaque: True for code `explain` cannot read, such as a hand-written
            `arrival_loop` or an `add_process` function.
        distribution: An arrival's gaps or a task's service, as its parameters
            such as `{"dist": "exponential", "rate": 1.8}`; None for a replayed
            arrival or a task without a service.
        capacity: A resource's `current_cap`; None for no limit.
        max_capacity: A resource's `max_cap`; None for no limit.
        features: What the queueing formulas leave out, such as `patience`.
        queue: A resource's queue order: `fifo`, `lifo` or `priority`.
        priority: A task's priority as it was given: a number, an expression's
            text, or `function`; None for none, which ranks 0.
    """

    id: str
    kind: NodeKind
    label: str
    opaque: bool = False
    distribution: Optional[Dict[str, Any]] = None
    capacity: Optional[int] = None
    max_capacity: Optional[int] = None
    features: List[str] = []
    queue: Optional[str] = None
    priority: Optional[str] = None


class Edge(_Model):
    """A link between two nodes.

    Attributes:
        source: The id of the node it leaves.
        target: The id of the node it reaches.
        kind: `spawn` (an arrival or a task starts an entity at a task), `uses`
            (a task holds a resource), `next` (a task's route to the next task or
            to the exit), `takes` and `gives` (a task's get and put).
        probability: The share of the source's entities that take a `next` edge,
            where the rules fix it; None where the run decides it.
    """

    source: str
    target: str
    kind: EdgeKind
    probability: Optional[float] = None


class Graph(_Model):
    """A model as nodes and edges, read from its declared linkage.

    Attributes:
        sim_id: The model's sim id.
        nodes: Every node.
        edges: Every edge.
        features: What changes demand or capacity during the run, for the whole
            model, such as `scenario steps`.
    """

    sim_id: str
    nodes: List[Node]
    edges: List[Edge]
    features: List[str] = []


class ArrivalResult(_Model):
    """An arrival's rate and the variability of its gaps.

    Attributes:
        name: The arrival's name.
        rate: λ, entities a second; None when unknown.
        ca2: The squared coefficient of variation of the gaps.
        reason: Why the rate is unknown.
    """

    name: str
    rate: Optional[float] = None
    ca2: Optional[float] = None
    reason: Optional[str] = None


class TaskResult(_Model):
    """A task's rate from flow balance, and its service.

    Attributes:
        name: The task's name.
        rate: λ, instances started a second; None when unknown.
        service_mean: The effective mean service time; 0 without a service.
        cs2: The squared coefficient of variation of the service time.
        reason: Why the rate is unknown.
    """

    name: str
    rate: Optional[float] = None
    service_mean: float = 0.0
    cs2: float = 0.0
    reason: Optional[str] = None


class CapacityRow(_Model):
    """Utilisation at one number of units.

    Attributes:
        capacity: The number of units.
        utilization: ρ at that number.
        status: `OK`, `CONGESTED` or `UNSTABLE`.
    """

    capacity: int
    utilization: float
    status: Status


class ResourceResult(_Model):
    """A resource's load, utilisation, status and wait estimate.

    Attributes:
        name: The resource's name.
        rate: λ, requests a second, summed over its tasks.
        service_mean: τ, the mean service time weighted by each task's rate.
        service_rate: μ = 1 / τ.
        servers: c, its `current_cap`; None for no limit.
        utilization: ρ = λ τ / c.
        status: `OK` below 0.85, `CONGESTED` from 0.85, `UNSTABLE` from 1.
        wait: Wq, the mean wait in the queue, where a formula applies.
        queue_length: Lq = λ Wq.
        in_system: L = λ (Wq + τ).
        method: The formula behind the wait: `kingman` (exact, for a Poisson
            input), `kingman_approx` (an approximation, for other gaps, which
            `--verify` does not compare), `erlang_c` or `no_limit`.
        reason: Why the utilisation is unknown.
        wait_reason: Why the wait is not estimated.
        capacities: ρ at each number of units the resource allows.
    """

    name: str
    rate: Optional[float] = None
    service_mean: Optional[float] = None
    service_rate: Optional[float] = None
    servers: Optional[int] = None
    utilization: Optional[float] = None
    status: Optional[Status] = None
    wait: Optional[float] = None
    queue_length: Optional[float] = None
    in_system: Optional[float] = None
    method: Optional[str] = None
    reason: Optional[str] = None
    wait_reason: Optional[str] = None
    capacities: List[CapacityRow] = []


class HolderResult(_Model):
    """A store's or container's put and get rates.

    Attributes:
        name: Its name.
        kind: `store` or `container`.
        put_rate: Puts a second from the tasks that add to it, where known.
        get_rate: Gets a second from the tasks that take from it, where known.
        reason: Why there is no queueing analysis.
    """

    name: str
    kind: Literal["store", "container"]
    put_rate: Optional[float] = None
    get_rate: Optional[float] = None
    reason: str


class Hint(_Model):
    """A change that brings a resource to the target utilisation.

    Attributes:
        resource: The resource's name.
        kind: `servers` (a number of units) or `rate` (the rate into it at its
            current units).
        value: The number of units, or the rate.
        text: The hint as a sentence.
    """

    resource: str
    kind: Literal["servers", "rate"]
    value: float
    text: str


class Comparison(_Model):
    """One analytical value beside the value a run measured.

    Attributes:
        node: The resource, task or arrival.
        measure: What is compared: `utilization`, `queue_length`, `wait` or
            `throughput`.
        analytical: The value of the analysis; None where it has none.
        measured: The run's value, or the mean of the replications; None when a
            run has no reading for it.
        half_width: The half-width of the replications' interval.
        differs: True when they disagree: by more than 10 percent for one run;
            for replications, by more than the interval's half-width and more
            than 1 percent; None without an analytical value or a reading.
        reason: Why the measure is not compared: where `analytical` is None, or
            where a run has no reading.
    """

    node: str
    measure: str
    analytical: Optional[float] = None
    measured: Optional[float] = None
    half_width: Optional[float] = None
    differs: Optional[bool] = None
    reason: Optional[str] = None


class Verification(_Model):
    """What `--verify` ran and found.

    Attributes:
        until: The end of each run.
        warmup: The model's warm-up; the measures leave it out.
        seed: The seed, or the seed the replications' seeds come from.
        replications: The number of runs, or None for one.
        comparisons: Every comparison.
        note: What the check does and does not show.
    """

    until: float
    warmup: Optional[float] = None
    seed: int
    replications: Optional[int] = None
    comparisons: List[Comparison]
    note: str


class Explanation(_Model):
    """A model's graph and its analysis.

    Attributes:
        schema_version: The version of this shape, `"0"` while it is
            experimental.
        sim_id: The model's sim id.
        target: The utilisation the hints aim for.
        graph: The graph.
        arrivals: Each arrival.
        tasks: Each task.
        resources: Each resource.
        holders: Each store and container.
        bottleneck: The resource with the highest utilisation the formulas cover.
        bottleneck_reason: Why no bottleneck can be named, when none is.
        hints: Changes that bring each resource to `target`.
        verification: What `--verify` found, when it ran.
    """

    schema_version: Literal["0"] = "0"
    sim_id: str
    target: float
    graph: Graph
    arrivals: List[ArrivalResult]
    tasks: List[TaskResult]
    resources: List[ResourceResult]
    holders: List[HolderResult]
    bottleneck: Optional[str] = None
    bottleneck_reason: Optional[str] = None
    hints: List[Hint] = []
    verification: Optional[Verification] = None

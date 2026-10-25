"""The graph of a model, read from the builder's state before the run starts.

`build_graph` reads only what the builder methods and `@app.task` declared, so YAML
and builder models give the same graph, and nothing is built or run. Linkage the
code does not declare, such as a hand-written `arrival_loop`, is an opaque node.

It reads these private attributes of `SimulationContext`, so a rename of any of them
must be made here too (`tests/unit/test_explain.py` covers each): `_arrivals_config`,
`_replays`, `_arrival_spawns`, `_startup_loops`, `_processes`, `_run_scenario`,
`_task_specs`, `_services_config`, `_resources_config`, `_stores_config`,
`_containers_config`. `features.py` and `edges.py` read the rest.
"""

import dataclasses
import math
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from desflow.explain import features
from desflow.explain.edges import EXIT, node_id, task_edges
from desflow.explain.models import Edge, Graph, Node

if TYPE_CHECKING:
    from desflow.core.context import SimulationContext


def build_graph(context: "SimulationContext") -> Graph:
    """Builds the graph of a context from what it declares, without starting it.

    Args:
        context: A builder or YAML context, before or after a run.

    Returns:
        Graph: Its arrivals, tasks, resources, stores, containers, opaque code and
            the exit, with the edges between them.
    """
    nodes: List[Node] = []
    edges: List[Edge] = []
    for name, config in context._arrivals_config.items():
        nodes.append(
            Node(
                id=node_id("arrival", name),
                kind="arrival",
                label=name,
                distribution=_parameters(config),
            )
        )
    for name in context._replays:
        nodes.append(Node(id=node_id("arrival", name), kind="arrival", label=name))
    for arrival, task in context._arrival_spawns.items():
        edges.append(
            Edge(
                source=node_id("arrival", arrival),
                target=node_id("task", task),
                kind="spawn",
            )
        )
    _add_opaque(context, nodes, edges)
    ends = False
    for name, spec in context._task_specs.items():
        service = context._services_config.get(spec.service_id or "")
        nodes.append(
            Node(
                id=node_id("task", name),
                kind="task",
                label=name,
                distribution=_parameters(service),
                features=features.task_features(context, spec),
                priority=features.priority(spec),
            )
        )
        ends = task_edges(spec, nodes, edges) or ends
    for name, capacity in context._resources_config.items():
        current = 1 if capacity.current_cap is None else capacity.current_cap
        most = current if capacity.max_cap is None else capacity.max_cap
        nodes.append(
            Node(
                id=node_id("resource", name),
                kind="resource",
                label=name,
                capacity=_units(current),
                max_capacity=_units(most),
                features=features.resource_features(context, name),
                queue=features.queue_order(context, name),
            )
        )
    for kind, configs in (
        ("store", context._stores_config),
        ("container", context._containers_config),
    ):
        for name in configs:
            nodes.append(Node(id=node_id(kind, name), kind=kind, label=name))
    if ends:
        nodes.append(Node(id=EXIT, kind="exit", label="exit"))
    return Graph(
        sim_id=context.sim_id,
        nodes=nodes,
        edges=edges,
        features=features.model_features(context),
    )


def _parameters(config: Any) -> Optional[Dict[str, Any]]:
    """A distribution config as the parameters it was given."""
    if config is None:
        return None
    return {
        key: value
        for key, value in dataclasses.asdict(config).items()
        if value is not None and key != "stream"
    }


def _units(value: Any) -> Optional[int]:
    """A capacity as a whole number of units, or None for no limit."""
    return None if math.isinf(value) else int(value)


def _add_opaque(
    context: "SimulationContext", nodes: List[Node], edges: List[Edge]
) -> None:
    """Adds a node for each hand-written arrival loop and each `add_process`."""
    loops: Dict[str, int] = {}
    for _, arrival in context._startup_loops:
        if arrival is not None:
            loops[arrival] = loops.get(arrival, 0) + 1
    for arrival, count in loops.items():
        # An arrival with spawn has one loop of its own, which the spawn edge shows
        for number in range(count - (arrival in context._arrival_spawns)):
            node = node_id(
                "opaque", f"{arrival}.arrival_loop" + (f".{number}" if number else "")
            )
            nodes.append(
                Node(
                    id=node,
                    kind="opaque",
                    label=f"arrival_loop of {arrival}",
                    opaque=True,
                )
            )
            edges.append(
                Edge(source=node_id("arrival", arrival), target=node, kind="spawn")
            )
    for number, function in enumerate(context._processes):
        if function == context._run_scenario:
            continue  # the scenario's own process, a model feature
        name = getattr(function, "__name__", type(function).__name__)
        nodes.append(
            Node(
                id=node_id("opaque", f"process.{number}.{name}"),
                kind="opaque",
                label=f"process {name}",
                opaque=True,
            )
        )

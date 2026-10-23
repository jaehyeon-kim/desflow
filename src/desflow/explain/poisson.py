"""Which tasks have a Poisson input, as Burke's and Jackson's theorems carry it.

Sources, read 24 Oct 2026: Burke's theorem (Wikipedia) for the departures of an
M/M/c station, and Jackson's theorem (Wikipedia, Jackson network) for splits and
merges of Poisson flows.
"""

from typing import Dict, List, Optional

from desflow.explain.arrivals import exponential
from desflow.explain.models import Edge, Graph, Node, TaskResult


def inflow(graph: Graph, task: str) -> List[Edge]:
    """The spawn and next edges into a task.

    Args:
        graph: The graph.
        task: The task's node id.

    Returns:
        List[Edge]: The edges.
    """
    return [e for e in graph.edges if e.target == task and e.kind in ("spawn", "next")]


def poisson_tasks(
    graph: Graph, tasks: Dict[str, TaskResult], stations: Dict[str, bool]
) -> Dict[str, bool]:
    """Whether each task's input is a Poisson process.

    An exponential arrival is Poisson, a fixed or weighted split of a Poisson flow is
    Poisson, and so is a merge of them; a task that leaves its input's timing
    unchanged (no service), or an M/M/c station (`stations`), passes a Poisson flow
    on (Burke's theorem). A task's spawn starts a copy of its own flow, which is not
    independent of it, so a spawned input is not counted as Poisson.

    Args:
        graph: The graph.
        tasks: Each task's result, by node id.
        stations: For each task, whether it passes a Poisson input on.

    Returns:
        Dict[str, bool]: By task node id.
    """
    nodes = {node.id: node for node in graph.nodes}
    flags = {task: result.rate is not None for task, result in tasks.items()}
    changed = True
    while changed:
        changed = False
        for task in flags:
            if flags[task] and not all(
                _poisson_edge(e, nodes, flags, stations) for e in inflow(graph, task)
            ):
                flags[task] = False
                changed = True
    return flags


def _poisson_edge(
    edge, nodes: Dict[str, Node], flags: Dict[str, bool], stations: Dict[str, bool]
) -> bool:
    source = nodes[edge.source]
    if source.kind == "arrival":
        return exponential(source)
    if source.kind != "task" or edge.kind != "next":
        return False
    return flags.get(source.id, False) and stations.get(source.id, False)


def direct_arrival(graph: Graph, task: str) -> Optional[str]:
    """The arrival that alone starts `task`, or None when anything else does.

    Args:
        graph: The graph.
        task: The task's node id.

    Returns:
        Optional[str]: The arrival's name, or None.
    """
    edges = inflow(graph, task)
    if len(edges) == 1 and edges[0].source.startswith("arrival:"):
        return edges[0].source.split(":", 1)[1]
    return None

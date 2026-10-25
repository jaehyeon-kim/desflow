"""Which tasks the queueing formulas cover: one pass over the graph's flow.

The formulas hold in a plain Jackson network: every task holds at most one resource,
each queue serves in arrival order (`fifo` or `lifo`, which give the same mean
wait), and nothing blocks, removes, delays or reorders work by priority. Anything else marks the node where it happens and every task after
it in the flow. A marked task's rate is unknown, so each resource it uses has no
supported utilisation, status or wait, and cannot be named the bottleneck. These
mark:

- a task feature (`features.py`), such as `put`, `patience` or `fail`: the task and
  every task after it;
- a resource feature, such as `queue_limit` or breakdowns: the tasks that use it and
  every task after them;
- a priority queue whose tasks have different priorities: every task after its
  tasks (the resource keeps its utilisation, without a wait);
- an unstable resource: every task after its tasks (the resource keeps its status);
- a scenario step, live ingress or an `add_process` function: every arrival and
  every task.

The priority queue and the unstable resource leave out the resource's own tasks,
which a rework loop also reaches.

A `next` function or a hand-written arrival loop already makes every rate unknown
(`rates.py`).
"""

from typing import Dict, Iterable, List

from desflow.explain.loads import mixed_priorities, users
from desflow.explain.models import VERIFY_HINT, Graph


def static_marks(graph: Graph) -> Dict[str, str]:
    """The tasks the model's own declarations take out of the formulas' reach.

    Args:
        graph: The graph.

    Returns:
        Dict[str, str]: The first reason found, by task node id; with a feature
            of the whole model, every arrival too, by its node id.
    """
    nodes = {node.id: node for node in graph.nodes}
    tasks = [node for node in graph.nodes if node.kind == "task"]
    whole = list(graph.features) + [
        n.label for n in graph.nodes if n.id.startswith("opaque:process.")
    ]
    if whole:
        reason = f"the formulas leave out {', '.join(whole)}, which can change any work"
        arrivals = [n for n in graph.nodes if n.kind == "arrival"]
        return {node.id: reason + VERIFY_HINT for node in tasks + arrivals}
    marks: Dict[str, str] = {}
    for task in tasks:
        if task.features:
            what = ", ".join(task.features)
            mark(
                graph, marks, [task.id], f"it has {what}, which the formulas leave out"
            )
            mark(
                graph,
                marks,
                after(graph, [task.id]),
                f"its work comes through {task.label}, whose {what} the formulas "
                "leave out",
            )
    by_resource = users(graph)
    for resource in [n for n in graph.nodes if n.kind == "resource"]:
        using = by_resource[resource.id]
        if resource.features:
            what = ", ".join(resource.features)
            text = f"it uses {resource.label}, whose {what} the formulas leave out"
            mark(graph, marks, using, text)
            mark(
                graph,
                marks,
                after(graph, using),
                text.replace("it uses", "its work comes through", 1),
            )
        if mixed_priorities(resource, [nodes[t] for t in using]):
            mark(
                graph,
                marks,
                downstream(graph, using),
                f"its work comes through the priority queue at {resource.label}",
            )
    return marks


def mark(
    graph: Graph, marks: Dict[str, str], tasks: Iterable[str], reason: str
) -> None:
    """Gives each task its first reason; a later reason does not replace it.

    Args:
        graph: The graph.
        marks: The reasons so far, by task node id; changed in place.
        tasks: The task node ids to mark.
        reason: Why, without the "run --verify" ending.
    """
    for task in tasks:
        marks.setdefault(task, reason + VERIFY_HINT)


def after(graph: Graph, starts: List[str]) -> List[str]:
    """The tasks reached from `starts` along spawn and next edges, `starts` left out
    unless a route leads back to them.

    Args:
        graph: The graph.
        starts: Task node ids.

    Returns:
        List[str]: Task node ids.
    """
    reached: List[str] = []
    frontier = list(starts)
    while frontier:
        source = frontier.pop()
        for edge in graph.edges:
            if (
                edge.source == source
                and edge.kind in ("spawn", "next")
                and edge.target.startswith("task:")
                and edge.target not in reached
            ):
                reached.append(edge.target)
                frontier.append(edge.target)
    return reached


def downstream(graph: Graph, using: List[str]) -> List[str]:
    """The tasks after a resource's tasks, leaving out those tasks themselves.

    Args:
        graph: The graph.
        using: The task node ids that use the resource.

    Returns:
        List[str]: Task node ids.
    """
    return [task for task in after(graph, using) if task not in using]

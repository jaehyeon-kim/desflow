"""Each resource's load: λ, τ, μ, c, ρ and its status; whether its queue keeps
arrival order; and the bottleneck."""

import math
from typing import Dict, List, Optional, Tuple

from desflow.explain.capacity import capacity_rows
from desflow.explain.formulas import status_of
from desflow.explain.models import Graph, Node, ResourceResult, TaskResult


def resources(graph: Graph) -> List[Node]:
    """The resource nodes, in graph order.

    Args:
        graph: The graph.

    Returns:
        List[Node]: The resource nodes.
    """
    return [node for node in graph.nodes if node.kind == "resource"]


def users(graph: Graph) -> Dict[str, List[str]]:
    """The tasks that use each resource.

    Args:
        graph: The graph.

    Returns:
        Dict[str, List[str]]: Task node ids, by resource node id.
    """
    using: Dict[str, List[str]] = {node.id: [] for node in resources(graph)}
    for edge in graph.edges:
        if edge.kind == "uses":
            using[edge.target].append(edge.source)
    return using


def processes(graph: Graph) -> List[str]:
    """The `add_process` functions, which can use any resource.

    Args:
        graph: The graph.

    Returns:
        List[str]: Their labels.
    """
    return [n.label for n in graph.nodes if n.id.startswith("opaque:process.")]


def load(node: Node, tasks: List[TaskResult], functions: List[str]) -> ResourceResult:
    """λ, τ, μ, c, ρ, the status and the capacity rows of one resource.

    Args:
        node: The resource's node.
        tasks: The results of the tasks that use it.
        functions: The `add_process` functions, which can use it too.

    Returns:
        ResourceResult: The resource's load, without its wait.
    """
    result = ResourceResult(name=node.label, servers=node.capacity)
    unknown = [t for t in tasks if t.rate is None]
    if unknown:
        result.reason = f"task {unknown[0].name} has no known rate: {unknown[0].reason}"
        return result
    rate = sum(t.rate or 0.0 for t in tasks)
    work = sum((t.rate or 0.0) * t.service_mean for t in tasks)
    result.rate = rate
    if rate > 0:
        result.service_mean = work / rate
        result.service_rate = rate / work if work > 0 else None
    if functions and node.capacity is not None:
        result.reason = (
            f"a process outside the task graph can use it ({', '.join(functions)}); "
            "run --verify"
        )
        return result
    if node.capacity is None or work == 0:
        result.utilization, result.status = 0.0, "OK"
    elif node.capacity == 0:
        result.status = "UNSTABLE"
        result.reason = "no units (current_cap 0) while work arrives"
    else:
        result.utilization = work / node.capacity
        result.status = status_of(result.utilization)
    if node.max_capacity is not None and work > 0:
        result.capacities = capacity_rows(work, node.capacity, node.max_capacity)
    return result


def bottleneck(results: List[ResourceResult]) -> Tuple[Optional[str], Optional[str]]:
    """The resource with the highest supported ρ, one with no units and work first;
    or, when no resource has one, why none can be named.

    Args:
        results: Every resource's result.

    Returns:
        Tuple[Optional[str], Optional[str]]: The bottleneck's name, or None and why.
    """
    ranked = [
        (math.inf if r.utilization is None else r.utilization, r.name)
        for r in results
        if r.utilization is not None or (r.status == "UNSTABLE" and r.rate)
    ]
    if ranked:
        return max(ranked, key=lambda pair: pair[0])[1], None
    if not results:
        return None, None
    first = results[0]
    return (
        None,
        f"no resource has a utilisation the formulas cover; {first.name}: {first.reason}",
    )


def mixed_priorities(node: Node, using: List[Node]) -> bool:
    """True for a priority queue whose tasks do not all have one fixed priority.

    Args:
        node: The resource's node.
        using: The nodes of the tasks that use it.

    Returns:
        bool: Whether the queue can serve them in a different order from first
            come, first served.
    """
    if node.queue != "priority" or len(using) < 2:
        return False
    values = set()
    for task in using:
        try:
            values.add(float(task.priority or 0))
        except ValueError:
            return True  # an expression or a function, which can differ by entity
    return len(values) > 1

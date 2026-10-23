"""Task rates by flow balance over the graph's routes.

Each task's rate solves λ = λ₀ + Pᵀλ, where λ₀ is what the arrivals spawn into it
and P holds the share of each task's instances that go on to each other task. A
task whose inflow the graph cannot fix, or that sits in a routing loop with no
exit, is unknown, with the reason. Code the graph cannot read, a hand-written
arrival loop or a `next` function, can start any task, so with one of them every
task is unknown.
"""

from typing import Dict, List, Tuple

import numpy as np

from desflow.explain.arrivals import moments
from desflow.explain.models import ArrivalResult, Graph, Node, TaskResult
from desflow.explain.poisson import inflow

# A loop whose entities never leave has a routing matrix with a spectral radius of 1
_CLOSED = 1.0 - 1e-9
_LOOP = "routing loop with no exit"


def task_results(graph: Graph, arrivals: List[ArrivalResult]) -> List[TaskResult]:
    """Each task's rate by flow balance, and its service's mean and cs².

    Args:
        graph: The graph.
        arrivals: Each arrival's result.

    Returns:
        List[TaskResult]: One per task, with the reason where its rate is unknown.
    """
    tasks = [node for node in graph.nodes if node.kind == "task"]
    reasons = _unknown(graph, tasks, {a.name: a for a in arrivals})
    known = [node.id for node in tasks if node.id not in reasons]
    rates: Dict[str, float] = {}
    while known:
        matrix, entering = _system(graph, known, {a.name: a for a in arrivals})
        closed = _closed_loops(known, matrix)
        if not closed:
            solved = np.linalg.solve(np.eye(len(known)) - matrix.T, entering)
            rates = dict(zip(known, (float(rate) for rate in solved)))
            break
        for task in _downstream(graph, closed):
            reasons.setdefault(task, _LOOP)
        known = [task for task in known if task not in reasons]
    results = []
    for node in tasks:
        mean, variance = moments(node.distribution) if node.distribution else (0.0, 0.0)
        results.append(
            TaskResult(
                name=node.label,
                rate=rates.get(node.id),
                service_mean=mean,
                cs2=variance / mean**2 if mean > 0 else 0.0,
                reason=reasons.get(node.id),
            )
        )
    return results


def _unknown(
    graph: Graph, tasks: List[Node], arrivals: Dict[str, ArrivalResult]
) -> Dict[str, str]:
    """The tasks whose rate the graph cannot fix, with the reason for each."""
    nodes = {node.id: node for node in graph.nodes}
    starters = sorted(
        nodes[e.target].label
        for e in graph.edges
        if e.kind in ("spawn", "next") and nodes[e.target].kind == "opaque"
    )
    if starters:
        reason = f"{starters[0]} is code that can start any task; run --verify"
        return {node.id: reason for node in tasks}
    reasons: Dict[str, str] = {}
    for node in tasks:
        edges = inflow(graph, node.id)
        if not edges:
            reasons[node.id] = "no declared arrival or task starts it; run --verify"
        for edge in edges:
            source = nodes[edge.source]
            if source.kind == "arrival" and arrivals[source.label].rate is None:
                reasons[node.id] = f"arrival {source.label} has no known rate"
            elif (
                source.kind == "task"
                and edge.kind == "next"
                and edge.probability is None
            ):
                reasons[node.id] = (
                    f"the split after {source.label} depends on the run; run --verify"
                )
            elif source.kind == "task" and edge.probability is None:
                reasons[node.id] = (
                    f"{source.label} spawns it only when it does not fail, and the "
                    "share that fails is not known before a run; run --verify"
                )
    for task in _downstream(graph, set(reasons)):
        if task not in reasons:
            reasons[task] = "reached from a task whose rate is unknown"
    return reasons


def _downstream(graph: Graph, starts: set) -> set:
    """The tasks reached from `starts` along spawn and next edges, `starts` included."""
    reached, frontier = set(starts), list(starts)
    while frontier:
        source = frontier.pop()
        for edge in graph.edges:
            if (
                edge.source == source
                and edge.target.startswith("task:")
                and edge.target not in reached
            ):
                reached.add(edge.target)
                frontier.append(edge.target)
    return reached


def _system(
    graph: Graph, known: List[str], arrivals: Dict[str, ArrivalResult]
) -> Tuple[np.ndarray, np.ndarray]:
    """P, the share of each known task's instances that start each other one, and λ₀."""
    index = {task: i for i, task in enumerate(known)}
    matrix = np.zeros((len(known), len(known)))
    entering = np.zeros(len(known))
    for edge in graph.edges:
        if edge.target not in index or edge.kind not in ("spawn", "next"):
            continue
        if edge.source.startswith("arrival:"):
            rate = arrivals[edge.source.split(":", 1)[1]].rate
            entering[index[edge.target]] += rate or 0.0
        elif edge.source in index:
            matrix[index[edge.source], index[edge.target]] += edge.probability or 0.0
    return matrix, entering


def _closed_loops(known: List[str], matrix: np.ndarray) -> set:
    """The tasks of each loop whose entities never all leave it."""
    reach = matrix > 0
    for k in range(len(known)):
        reach = reach | (reach[:, [k]] & reach[[k], :])
    closed = set()
    for i, task in enumerate(known):
        if not reach[i, i]:
            continue
        members = [j for j in range(len(known)) if reach[i, j] and reach[j, i]]
        block = matrix[np.ix_(members, members)]
        if max(abs(np.linalg.eigvals(block))) >= _CLOSED:
            closed.add(task)
    return closed

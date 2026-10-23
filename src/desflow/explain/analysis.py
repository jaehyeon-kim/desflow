"""The queueing analysis of a graph: rates, then each resource's load, status and
wait, the bottleneck, the stores and containers, and the hints.

`loads.py` gives each resource's load and the bottleneck, `formulas.py` holds the
formulas and their sources, `waits.py` decides which one applies to a resource, and
`capacity.py` gives the capacity rows and the hints.
"""

from typing import Dict, Union

from desflow.explain import loads, support
from desflow.explain.arrivals import arrival_results
from desflow.explain.capacity import hints
from desflow.explain.holders import holder_results
from desflow.explain.models import (
    VERIFY_HINT,
    ArrivalResult,
    Explanation,
    Graph,
    TaskResult,
)
from desflow.explain.poisson import poisson_tasks
from desflow.explain.rates import task_results
from desflow.explain.waits import fill_wait, stations

_PRIORITY = (
    "a priority queue serves its tasks in a different order from first come, "
    "first served" + VERIFY_HINT
)


def analyze(graph: Graph, target: float = 0.85) -> Explanation:
    """Analyses a graph: rates by flow balance, then each resource's load and wait.

    A value is given only where the formulas cover it (`support.py`); everywhere
    else it is None, with the reason.

    Args:
        graph: The graph `build_graph` made.
        target: The utilisation the hints aim for, above 0 and at most 10.

    Returns:
        Explanation: The graph and its analysis.

    Raises:
        ValueError: If `target` is out of range.
    """
    if not 0 < target <= 10:
        raise ValueError(f"target must be above 0 and at most 10, got {target:g}")
    arrivals = arrival_results(graph)
    tasks = task_results(graph, arrivals)
    by_id = {f"task:{t.name}": t for t in tasks}
    users = loads.users(graph)
    functions = loads.processes(graph)
    nodes = {node.id: node for node in graph.nodes}
    marks = support.static_marks(graph)
    while True:
        _unmark({**by_id, **{f"arrival:{a.name}": a for a in arrivals}}, marks)
        resources = [
            loads.load(node, [by_id[t] for t in users[node.id]], functions)
            for node in loads.resources(graph)
        ]
        before = len(marks)
        for node, r in zip(loads.resources(graph), resources):
            if r.status == "UNSTABLE":
                support.mark(
                    graph,
                    marks,
                    support.downstream(graph, users[node.id]),
                    f"its work comes through unstable {r.name}",
                )
        if len(marks) == before:
            break
    passes = stations(graph, by_id, users, {f"resource:{r.name}": r for r in resources})
    poisson = poisson_tasks(graph, by_id, passes)
    for node, resource in zip(loads.resources(graph), resources):
        if loads.mixed_priorities(node, [nodes[t] for t in users[node.id]]):
            resource.wait_reason = resource.reason or _PRIORITY
            continue
        fill_wait(graph, node, resource, users[node.id], by_id, poisson, arrivals)
    bottleneck, why = loads.bottleneck(resources)
    return Explanation(
        sim_id=graph.sim_id,
        target=target,
        graph=graph,
        arrivals=arrivals,
        tasks=tasks,
        resources=resources,
        holders=holder_results(graph, by_id),
        bottleneck=bottleneck,
        bottleneck_reason=why,
        hints=[] if functions else [h for r in resources for h in hints(r, target)],
    )


def _unmark(
    results: Dict[str, Union[TaskResult, ArrivalResult]], marks: Dict[str, str]
) -> None:
    """Takes the rate from each marked task or arrival, with its reason."""
    for node, reason in marks.items():
        result = results[node]
        if result.rate is not None or result.reason is None:
            result.rate, result.reason = None, reason

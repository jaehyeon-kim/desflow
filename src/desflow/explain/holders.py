"""Stores and containers: their put and get rates, without a queueing analysis."""

from typing import Dict, List

from desflow.explain.models import Graph, HolderResult, TaskResult


def holder_results(graph: Graph, tasks: Dict[str, TaskResult]) -> List[HolderResult]:
    """The put and get rates of each store and container, where known.

    Args:
        graph: The graph.
        tasks: Each task's result, by node id.

    Returns:
        List[HolderResult]: One per store or container, with no queueing analysis.
    """
    results = []
    for node in graph.nodes:
        if node.kind not in ("store", "container"):
            continue
        totals = {}
        for kind in ("gives", "takes"):
            linked = [
                tasks[e.source]
                for e in graph.edges
                if e.target == node.id and e.kind == kind
            ]
            known = all(t.rate is not None for t in linked)
            totals[kind] = sum(t.rate or 0.0 for t in linked) if known else None
        results.append(
            HolderResult(
                name=node.label,
                kind=node.kind,  # type: ignore[arg-type]
                put_rate=totals["gives"],
                get_rate=totals["takes"],
                reason=f"no queueing analysis for a {node.kind}; run --verify",
            )
        )
    return results

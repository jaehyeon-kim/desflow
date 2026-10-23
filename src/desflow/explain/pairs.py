"""What `--verify` compares: each measure with its analytical value, or the reason
it is not compared.

The analysis gives a value only where the formulas cover it (`support.py`), so a
measure is compared where the analysis has a value, except a wait from Kingman's
formula with gaps that are not exponential, which is an approximation. Everywhere
else the row says "not compared", with the analysis's own reason, and an unstable
resource, which has no steady state, is not compared either.
"""

from typing import Dict, List, NamedTuple, Optional

from desflow.explain.models import Explanation, Graph
from desflow.explain.support import after

_UNSTABLE = "unstable: it has no steady state to compare with"
_APPROX = "Kingman's formula approximates the wait when the gaps are not exponential"


class Pair(NamedTuple):
    """One measure to compare.

    Attributes:
        node: The resource, task or arrival.
        measure: `utilization`, `queue_length`, `wait` or `throughput`.
        statistic: The statistic's name after `<sim_id>.stats.`.
        analytical: The analysis's value; None when it is not compared.
        scale: The factor that puts the statistic in the analysis's units.
        reason: Why it is not compared, where `analytical` is None; or, where it
            has a value, why a run has no reading for it.
    """

    node: str
    measure: str
    statistic: str
    analytical: Optional[float]
    scale: float
    reason: Optional[str]


def pairs(explanation: Explanation, window: float) -> List[Pair]:
    """Each measure to compare, per resource, per task and per arrival.

    Args:
        explanation: The analysis.
        window: The seconds measured: the end less the start and the warm-up.

    Returns:
        List[Pair]: The measures.
    """
    graph = explanation.graph
    held = _held(graph)
    found: List[Pair] = []
    for r in explanation.resources:
        base = f"resources.{r.name}"
        unstable = _UNSTABLE if r.status == "UNSTABLE" else None
        approx = _APPROX if r.method == "kingman_approx" else None
        found.append(
            _pair(
                r.name,
                "utilization",
                f"{base}.utilization_avg",
                r.utilization,
                0.01,
                unstable or r.reason,
            )
        )
        found.append(
            _pair(
                r.name,
                "queue_length",
                f"{base}.queue_length_avg",
                r.queue_length,
                1.0,
                unstable or approx or r.wait_reason,
            )
        )
    resources = {r.name: r for r in explanation.resources}
    for task, using in held.items():
        r = resources[using[0]]
        reason = "it holds several resources" if len(using) > 1 else r.wait_reason
        if r.method == "kingman_approx":
            reason = reason or _APPROX
        found.append(
            _pair(task, "wait", f"tasks.{task}.wait_mean", r.wait, 1.0, reason)
        )
    tasks = {t.name: t for t in explanation.tasks}
    for a in explanation.arrivals:
        reason = a.reason
        flow = after(graph, [f"arrival:{a.name}"])
        if not flow:
            reason = reason or "it starts no task the graph shows"
        for node in sorted(flow):
            task = node.split(":", 1)[1]
            if tasks[task].reason:
                reason = reason or f"task {task}: {tasks[task].reason}"
            for name in held.get(task, []):
                if resources[name].status == "UNSTABLE":
                    reason = reason or f"its flow passes unstable {name}"
        found.append(
            _pair(
                a.name,
                "throughput",
                f"flows.{a.name}.finished",
                a.rate,
                1.0 / window,
                reason,
            )
        )
    return found


def _pair(
    node: str,
    measure: str,
    statistic: str,
    value: Optional[float],
    scale: float,
    reason: Optional[str],
) -> Pair:
    if value is None and reason is None:
        reason = "the analysis has no value for it"
    analytical = None if reason else value
    plain = reason.removesuffix("; run --verify") if reason else None
    return Pair(node, measure, statistic, analytical, scale, plain)


def _held(graph: Graph) -> Dict[str, List[str]]:
    """The resources each task holds, by task name."""
    held: Dict[str, List[str]] = {}
    for edge in graph.edges:
        if edge.kind == "uses":
            name = edge.source.split(":", 1)[1]
            held.setdefault(name, []).append(edge.target.split(":", 1)[1])
    return held

"""The wait at a resource, where Kingman's formula or Erlang C applies.

Sources, read 24 Oct 2026: Kingman's formula (Wikipedia, Kingman's formula) for one
server with a renewal input, an approximation that is exact for a Poisson input; Erlang C (Wikipedia, M/M/c queue) for c servers with a
Poisson input and one exponential service rate; Little's law (Wikipedia, Little's
law) for Lq and L.
"""

import json
from typing import Dict, List, Optional, Tuple

from desflow.explain.arrivals import exponential
from desflow.explain.formulas import UNSTABLE_AT, erlang_wait, kingman_wait
from desflow.explain.models import Graph, Node, ResourceResult, TaskResult
from desflow.explain.poisson import direct_arrival

D3 = "no closed formula for c > 1 with non-exponential times; run --verify"
_RENEWAL = (
    "its input is not one arrival or a Poisson flow, which Kingman's formula "
    "needs; run --verify"
)


def stations(
    graph: Graph,
    tasks: Dict[str, TaskResult],
    users: Dict[str, List[str]],
    resources: Dict[str, ResourceResult],
) -> Dict[str, bool]:
    """Whether each task passes a Poisson input on unchanged in kind: one with no
    service, an exponential service with no resource (M/M/∞), or an M/M/c station.

    Args:
        graph: The graph.
        tasks: Each task's result, by node id.
        users: The tasks that use each resource, by resource node id.
        resources: Each resource's result, by node id.

    Returns:
        Dict[str, bool]: By task node id.
    """
    nodes = {node.id: node for node in graph.nodes}
    held: Dict[str, List[str]] = {task: [] for task in tasks}
    for holder, using in users.items():
        for task in using:
            held[task].append(holder)
    passes = {}
    for task, result in tasks.items():
        node = nodes[task]
        if not held[task]:
            passes[task] = not node.features and (
                node.distribution is None or exponential(node)
            )
        elif len(held[task]) == 1:
            resource = resources[held[task][0]]
            passes[task] = _markov(nodes[held[task][0]], resource, users, nodes)
        else:
            passes[task] = False
    return passes


def _markov(
    node: Node,
    resource: ResourceResult,
    users: Dict[str, List[str]],
    nodes: Dict[str, Node],
) -> bool:
    """True for a stable resource whose tasks share one exponential service rate."""
    using = [nodes[t] for t in users[node.id]]
    # One rate: every task's service has the same parameters
    services = {json.dumps(t.distribution, sort_keys=True) for t in using}
    return (
        all(exponential(t) for t in using)
        and len(services) == 1
        and resource.utilization is not None
        and resource.utilization < UNSTABLE_AT
    )


def fill_wait(
    graph: Graph,
    node: Node,
    result: ResourceResult,
    using: List[str],
    tasks: Dict[str, TaskResult],
    poisson: Dict[str, bool],
    arrivals: List,
) -> None:
    """Fills the wait, Lq and L of one resource, or the reason it has none.

    Args:
        graph: The graph.
        node: The resource's node.
        result: The resource's result, with its load; filled in place.
        using: The node ids of the tasks that use it.
        tasks: Each task's result, by node id.
        poisson: Whether each task's input is Poisson, by node id.
        arrivals: Each arrival's result.
    """
    if result.utilization is None or result.rate is None:
        result.wait_reason = result.reason
        return
    # support.py has already taken the load from a resource with a feature
    nodes = {n.id: n for n in graph.nodes}
    if not using:
        result.wait_reason = "no task uses it"
    elif result.servers is None:
        result.wait, result.method = 0.0, "no_limit"
    elif result.utilization >= UNSTABLE_AT:
        result.wait_reason = "unstable: the queue grows for as long as the run lasts"
    elif result.servers == 1:
        ca2 = _renewal_ca2(graph, using, poisson, arrivals)
        if ca2 is None:
            result.wait_reason = _RENEWAL
        else:
            mean, cs2 = _mixture([tasks[t] for t in using])
            result.wait = kingman_wait(result.utilization, ca2, cs2, mean)
            # With a Poisson input Kingman's formula is exact (Pollaczek-Khinchine)
            exact = all(poisson[t] for t in using)
            result.method = "kingman" if exact else "kingman_approx"
    elif all(poisson[t] for t in using) and _markov(
        node, result, {node.id: using}, nodes
    ):
        assert result.service_mean is not None
        result.wait = erlang_wait(result.servers, result.rate, result.service_mean)
        result.method = "erlang_c"
    else:
        result.wait_reason = D3
    if result.wait is not None and result.service_mean is not None:
        result.queue_length = result.rate * result.wait
        result.in_system = result.rate * (result.wait + result.service_mean)


def _renewal_ca2(
    graph: Graph, using: List[str], poisson: Dict[str, bool], arrivals: List
) -> Optional[float]:
    """The ca² of a renewal input: 1 for a Poisson input, the arrival's own when one
    arrival alone starts the resource's one task, else None."""
    if all(poisson[t] for t in using):
        return 1.0
    if len(using) == 1:
        arrival = direct_arrival(graph, using[0])
        found = [a for a in arrivals if a.name == arrival]
        if found and found[0].ca2 is not None:
            return found[0].ca2
    return None


def _mixture(tasks: List[TaskResult]) -> Tuple[float, float]:
    """The mean and cs² of the service time of a request, mixed by task rate."""
    rate = sum(t.rate or 0.0 for t in tasks)
    weights = [(t.rate or 0.0) / rate for t in tasks]
    mean = sum(w * t.service_mean for w, t in zip(weights, tasks))
    second = sum(w * t.service_mean**2 * (1 + t.cs2) for w, t in zip(weights, tasks))
    return mean, (second - mean**2) / mean**2 if mean > 0 else 0.0

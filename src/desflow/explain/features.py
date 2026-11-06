"""What a model does that the queueing formulas leave out, read from the builder.

Each feature is a short name, such as `abandonment (patience)`, that the analysis
gives as the reason a wait is not estimated. It reads the model through
`parts_of(context)` (`tests/unit/test_explain.py` covers each read): `tasks` (its
`specs`), `holders` (its `resource_preempt`, `resource_queues`, `resource_queue_limits`,
`resource_types` and `breakdowns_config`), `arrivals` (its `plans`), `scenario_steps`,
`signals` (its `on`) and `sinks` (its `ingress`).
"""

from typing import TYPE_CHECKING, Any, List, Mapping, Optional, Set

from desflow.explain.edges import used_resources
from desflow.values.models import Sequence, Uuid

from desflow.core.context import parts_of

if TYPE_CHECKING:
    from desflow.core.context import SimulationContext
    from desflow.core.flow import Wait
    from desflow.core.task import TaskSpec

KEEP = "keep (a unit held across tasks)"
# A request that a timeout of the same wait can end before the unit is granted
WAIT_TIMEOUT = "wait group with a timeout (a request can be abandoned)"
# Two or more request leaves: the task can hold more than one unit through its service
SEVERAL_REQUESTS = "wait group with several requests"
# A `service_time` is read for each instance, so no distribution gives its mean
COMPUTED_SERVICE_TIME = "computed service time"
# An arrival's `gap` is read at the start of each gap
COMPUTED_GAPS = "computed gaps"
# `count`, `keys`, a `key` that can repeat or `max_events` change how many entities an
# arrival starts, because a start whose key is still running is skipped
LIMITED_ARRIVALS = "limited or repeated arrivals"
# `initial` with no gap: the entities start when the run is built, and none follow
INITIAL_ONLY = "entities at the start only"
# An `interrupt` of a task, an `on:` entry or a scenario step names the task, so some
# of its instances end their service early
INTERRUPTED = "interruptions (an interrupt names it)"


def arrival_features(context: "SimulationContext", name: str) -> List[str]:
    """What an arrival does that its distribution does not say.

    Args:
        context: The context that declares the arrival.
        name: The arrival's name.

    Returns:
        List[str]: The features, in a fixed order.
    """
    plan = parts_of(context).arrivals.plans.get(name)
    if plan is None:
        return []
    found = []
    if plan.gap is not None:
        found.append(COMPUTED_GAPS)
    if plan.sourceless:
        found.append(INITIAL_ONLY)
    repeating_key = plan.key is not None and not isinstance(plan.key, (Sequence, Uuid))
    if plan.has_entities or plan.max_events is not None or repeating_key:
        found.append(LIMITED_ARRIVALS)
    return found


def task_features(context: "SimulationContext", spec: "TaskSpec") -> List[str]:
    """What the task does that the queueing formulas leave out.

    Args:
        context: The context that declares the task.
        spec: The task.

    Returns:
        List[str]: The features, in a fixed order.
    """
    parts = parts_of(context)
    features = []
    held = used_resources(spec)
    if spec.patience is not None:
        features.append("abandonment (patience)")
    if spec.balk is not None:
        features.append("abandonment (balk)")
    if spec.service_time is not None:
        features.append(COMPUTED_SERVICE_TIME)
    if spec.preempt and any(parts.holders.resource_preempt.get(r) for r in held):
        features.append("preemption")
    if _holds_several(spec):
        features.append("several resources")
    if spec.keep or _receives_kept(context, spec):
        features.append(KEEP)
    only_requests = spec.wait is not None and _only_requests(spec.wait)
    for name in (
        "wait",
        "get",
        "put",
        "signal",
        "subscribe",
        "interrupt",
        "resume",
        "batch",
        "seize",
        "open",
        "listen",
        "release",
        "cancel",
    ):
        if getattr(spec, name) is not None and not (name == "wait" and only_requests):
            features.append(name)
    if spec.name in _interrupted(context):
        features.append(INTERRUPTED)
    if spec.wait is not None and _request_leaves(spec.wait) > 1:
        features.append(SEVERAL_REQUESTS)
    if _request_with_timeout(spec):
        features.append(WAIT_TIMEOUT)
    if spec.on:
        features.append("on actions")
    if spec.fail is not None:
        features.append("fail")
    return features


def _interrupted(context: "SimulationContext") -> Set[str]:
    """The tasks an `interrupt` names: a task's own, one in the `on:` or `listen`
    list of a task or a signal, or a scenario step's."""
    parts = parts_of(context)
    actions: List[Mapping[str, Any]] = [
        action for found in parts.signals.on.values() for action in found
    ]
    for spec in parts.tasks.specs.values():
        if spec.interrupt is not None:
            actions.append({"interrupt": spec.interrupt})
        actions += [*spec.on, *(spec.listen.on if spec.listen else [])]
    actions += parts.scenario_steps
    return {
        action["interrupt"]["task"]
        for action in actions
        if action.get("interrupt") is not None
        and action["interrupt"].get("task") is not None
    }


def _request_leaves(wait: "Wait") -> int:
    """The number of `request` leaves of the wait, two to one resource counted twice."""
    return sum(1 for n in wait.branches.values() if n.kind == "request")


def _only_requests(wait: "Wait") -> bool:
    """True when every leaf of the wait is a `request`. With one such leaf, the wait
    queues for a unit as `resource:` does and the formulas cover it; with more, the
    task can hold several units, which `SEVERAL_REQUESTS` names."""
    leaves = [n for n in wait.branches.values() if not n.is_group]
    return all(n.kind == "request" for n in leaves)


def _holds_several(spec: "TaskSpec") -> bool:
    """True when the task can hold units of more than one resource at once: two of
    its own, its own and one its wait requests, or two its wait holds together."""
    own = set(spec.resources)
    if len(own) > 1:
        return True
    if spec.wait is None:
        return False
    if spec.wait.requests_together():
        return True
    return bool(own) and bool(set(spec.wait.requests()) - own)


def _request_with_timeout(spec: "TaskSpec") -> bool:
    """True when the task's wait requests a unit and also has a timeout, so the
    request can end, and the order leave the queue, before a unit is granted."""
    if spec.wait is None or not spec.wait.requests():
        return False
    return any(node.kind == "timeout" for node in spec.wait.branches.values())


def _receives_kept(context: "SimulationContext", spec: "TaskSpec") -> bool:
    """True when a task before this one keeps a unit of a resource it uses."""
    for other in parts_of(context).tasks.specs.values():
        following = other.router.tasks or set()
        if spec.name in following and set(other.keep) & set(used_resources(spec)):
            return True
    return False


def priority(spec: "TaskSpec") -> Optional[str]:
    """The task's priority as text: a number, an expression, or `function`.

    Args:
        spec: The task.

    Returns:
        Optional[str]: The text, or None for no priority.
    """
    value: Any = spec.priority
    if value is None or isinstance(value, (int, float, str)):
        return None if value is None else str(value)
    return "function"


def resource_features(context: "SimulationContext", name: str) -> List[str]:
    """What the resource does that the queueing formulas leave out.

    Args:
        context: The context that declares the resource.
        name: The resource's name.

    Returns:
        List[str]: The features, in a fixed order.
    """
    parts = parts_of(context)
    features = []
    if parts.holders.resource_queue_limits.get(name) is not None:
        features.append("abandonment (queue_limit)")
    if parts.holders.resource_types.get(name) is not None:
        features.append("custom resource type")
    if name in parts.holders.breakdowns_config:
        features.append("breakdowns")
    return features


def queue_order(context: "SimulationContext", name: str) -> Optional[str]:
    """The resource's queue order, such as `fifo` or `priority`.

    Args:
        context: The context that declares the resource.
        name: The resource's name.

    Returns:
        Optional[str]: The order, or None when it was not given.
    """
    return parts_of(context).holders.resource_queues.get(name)


def model_features(context: "SimulationContext") -> List[str]:
    """What can change demand or capacity anywhere in the model during the run.

    Args:
        context: The context.

    Returns:
        List[str]: The features, in a fixed order.
    """
    parts = parts_of(context)
    features = []
    if any(set(step) - {"at", "record"} for step in parts.scenario_steps):
        features.append("scenario steps")
    if parts.sinks.ingress:
        features.append("live ingress")
    return features

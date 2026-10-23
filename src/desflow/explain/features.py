"""What a model does that the queueing formulas leave out, read from the builder.

Each feature is a short name, such as `abandonment (patience)`, that the analysis
gives as the reason a wait is not estimated. It reads these private attributes of
`SimulationContext`, so a rename of any of them must be made here too
(`tests/unit/test_explain.py` covers each): `_task_specs`, `_resource_preempt`,
`_resource_queues`, `_resource_queue_limits`, `_resource_types`,
`_breakdowns_config`, `_scenario_steps`, `_ingress_providers`.
"""

from typing import TYPE_CHECKING, Any, List, Optional

if TYPE_CHECKING:
    from desflow.core.context import SimulationContext
    from desflow.core.task import TaskSpec

KEEP = "keep (a unit held across tasks)"


def task_features(context: "SimulationContext", spec: "TaskSpec") -> List[str]:
    """What the task does that the queueing formulas leave out.

    Args:
        context: The context that declares the task.
        spec: The task.

    Returns:
        List[str]: The features, in a fixed order.
    """
    features = []
    if spec.patience is not None:
        features.append("abandonment (patience)")
    if spec.balk is not None:
        features.append("abandonment (balk)")
    if spec.preempt and any(context._resource_preempt.get(r) for r in spec.resources):
        features.append("preemption")
    if len(spec.resources) > 1:
        features.append("several resources")
    if spec.keep or _receives_kept(context, spec):
        features.append(KEEP)
    for name in ("wait", "get", "put", "signal", "subscribe", "interrupt"):
        if getattr(spec, name) is not None:
            features.append(name)
    if spec.on:
        features.append("on actions")
    if spec.fail is not None:
        features.append("fail")
    return features


def _receives_kept(context: "SimulationContext", spec: "TaskSpec") -> bool:
    """True when a task before this one keeps a unit of a resource it uses."""
    for other in context._task_specs.values():
        following = other.router.tasks or set()
        if spec.name in following and set(other.keep) & set(spec.resources):
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
    features = []
    if context._resource_queue_limits.get(name) is not None:
        features.append("abandonment (queue_limit)")
    if context._resource_types.get(name) is not None:
        features.append("custom resource type")
    if name in context._breakdowns_config:
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
    return context._resource_queues.get(name)


def model_features(context: "SimulationContext") -> List[str]:
    """What can change demand or capacity anywhere in the model during the run.

    Args:
        context: The context.

    Returns:
        List[str]: The features, in a fixed order.
    """
    features = []
    if any(set(step) - {"at", "record"} for step in context._scenario_steps):
        features.append("scenario steps")
    if context._ingress_providers:
        features.append("live ingress")
    return features

"""A task's edges: the resources it uses, what it takes and gives, and where its
entities go next. It reads a task's `router._default`, which is private.
"""

from typing import TYPE_CHECKING, List

from desflow.explain.models import Edge, Node

if TYPE_CHECKING:
    from desflow.core.task import TaskSpec

EXIT = "exit"


def node_id(kind: str, name: str) -> str:
    """A node's id: its kind and its name, such as `task:pack_order`.

    Args:
        kind: The node's kind.
        name: The name in the model.

    Returns:
        str: The id.
    """
    return f"{kind}:{name}"


def used_resources(spec: "TaskSpec") -> List[str]:
    """The resources a task holds: its own, then those its wait requests, then the one
    it seizes and those it opens a request for.

    A unit a `request` branch of the wait wins is held through the service, as a
    unit of the task's own resource is, so it counts toward the resource's load. A
    seized or opened unit is asked for by the task, so the task uses that resource.

    Args:
        spec: The task.

    Returns:
        List[str]: The resource names, each once, in that order.
    """
    names = list(spec.resources)
    if spec.wait is not None:
        names += [r for r in spec.wait.requests() if r not in names]
    if spec.seize is not None and spec.seize.resource not in names:
        names.append(spec.seize.resource)
    if spec.open is not None:
        names += [r for r in spec.open.requests() if r not in names]
    return names


def diverts(spec: "TaskSpec") -> bool:
    """Whether some instances of the task leave the route its `next` gives: a `fail`
    condition, or a task for `on_fail`, `on_renege` or `on_balk`, so the share of
    each route is not known before a run.

    Args:
        spec: The task.

    Returns:
        bool: True when the task can divert some of its instances.
    """
    return (
        spec.fail is not None
        or spec.on_fail is not None
        or spec.on_renege is not None
        or spec.on_balk is not None
    )


def task_edges(spec: "TaskSpec", nodes: List[Node], edges: List[Edge]) -> bool:
    """Adds a task's resource, get, put, spawn and next edges.

    Args:
        spec: The task.
        nodes: The graph's nodes, which gain an opaque node for a `next` function.
        edges: The graph's edges, which gain the task's edges.

    Returns:
        bool: True when a route ends the flow, which needs the exit node.
    """
    task = node_id("task", spec.name)
    for resource in used_resources(spec):
        edges.append(
            Edge(source=task, target=node_id("resource", resource), kind="uses")
        )
    for transfer, kind in ((spec.get, "takes"), (spec.put, "gives")):
        if transfer is not None:
            holder = (
                ("store", transfer.store)
                if transfer.store
                else ("container", transfer.container)
            )
            edges.append(Edge(source=task, target=node_id(*holder), kind=kind))  # type: ignore[arg-type]
    if spec.spawn is not None:
        # The spawn starts only when the task succeeds, so with `fail` its share
        # is not known before a run
        edges.append(
            Edge(
                source=task,
                target=node_id("task", spec.spawn["task"]),
                kind="spawn",
                probability=None if diverts(spec) else 1.0,
            )
        )
    router = spec.router
    if router.tasks is None:
        # A function decides the next task, so the graph cannot name it
        node = node_id("opaque", f"{spec.name}.next")
        nodes.append(
            Node(id=node, kind="opaque", label=f"next of {spec.name}", opaque=True)
        )
        edges.append(Edge(source=task, target=node, kind="next"))
        return False
    shares = router.probabilities
    if diverts(spec):
        shares = None  # a failure, renege or balk leaves the route the rules give
    if shares is not None:
        for target, share in shares.items():
            edges.append(
                Edge(
                    source=task,
                    target=node_id("task", target),
                    kind="next",
                    probability=share,
                )
            )
        if not shares:
            edges.append(Edge(source=task, target=EXIT, kind="next", probability=1.0))
            return True
        return False
    targets = sorted(router.tasks) + [
        target
        for target in (spec.on_fail, spec.on_renege, spec.on_balk)
        if target is not None
    ]
    for target in targets:
        edges.append(Edge(source=task, target=node_id("task", target), kind="next"))
    # when rules with no default, a failure without on_fail, or no rule at all end
    # some flows
    # Weights read when the task finishes name every route, and none ends the flow
    ends = (router._default is None and not router.computed) or (
        spec.fail is not None and spec.on_fail is None
    )
    if ends:
        edges.append(Edge(source=task, target=EXIT, kind="next"))
    return ends

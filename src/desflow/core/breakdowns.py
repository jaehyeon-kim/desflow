"""Breakdowns: a resource that fails at random and is down until it is repaired.

`run_breakdowns` is a SimPy process any API can start. It draws a time to failure,
puts the resource down, publishes `down`, draws a repair time, repairs it and
publishes `repaired`, and repeats. The failure clock runs on calendar time. The
builder's `add_breakdown` and a YAML resource's `breakdown` start it for you.
"""

from typing import Any

from desflow.core.sampler import Sampler
from desflow.resources.resource import ResourceBase

# What a breakdown does to the tasks being served
BREAKDOWN_WORK = ("interrupt", "finish")


def run_breakdowns(
    env: Any, resource: ResourceBase, sampler: Sampler, work: str = "interrupt"
):
    """
    Breaks a resource down and repairs it, for as long as the run lasts.

    The time to failure and the repair time are read from the registry, at
    `<sim_id>.breakdown.<resource>.time_to_failure` and `.repair`, which a
    `SimParameter`'s `breakdowns` registers, so they can change during the run. Each
    failure publishes an event with the resource's name as its key and
    `{"status": "down", "reason": "failure"}`, and each repair one with
    `{"status": "repaired", "reason": "failure"}`; neither has a task.

    Args:
        env: The environment.
        resource: The resource that breaks down.
        sampler: The sampler the times are drawn with.
        work: `interrupt` interrupts the tasks being served, which follow their
            `on_interrupt`; `finish` lets them finish.

    Raises:
        ValueError: If `work` is neither `interrupt` nor `finish`.
    """
    if work not in BREAKDOWN_WORK:
        raise ValueError(f"work must be interrupt or finish, got '{work}'")
    path = f"{resource.sim_id}.breakdown.{resource.obj_id}"
    resource.has_breakdowns = True
    while True:
        failure = env.registry.get_config(f"{path}.time_to_failure")
        yield env.timeout(sampler.sample(failure))
        resource.fail(interrupt_work=work == "interrupt")
        _publish(env, resource, "down")
        repair = env.registry.get_config(f"{path}.repair")
        yield env.timeout(sampler.sample(repair))
        resource.repair()
        _publish(env, resource, "repaired")


def _publish(env: Any, resource: ResourceBase, status: str) -> None:
    publish = getattr(env, "publish_event", None)
    if publish is not None:
        publish(resource.obj_id, {"status": status, "reason": "failure"})

"""Breakdowns: a resource that fails at random and is down until it is repaired.

`run_breakdowns` is a SimPy process any API can start. It draws a time to failure,
puts the resource down, publishes `down`, draws a repair time, repairs it and
publishes `repaired`, and repeats. The failure clock runs on calendar time, or on the
time the resource holds a unit. The builder's `add_breakdown` and a YAML resource's
`breakdown` start it for you.
"""

from typing import Any, Generator

from desflow.internal import InternalTimeout
from desflow.core.sampler import Sampler
from desflow.resources.resource import ResourceBase

# What a breakdown does to the tasks being served
BREAKDOWN_WORK = ("interrupt", "finish")
# Which time the time to failure counts
BREAKDOWN_CLOCKS = ("calendar", "busy")
# The time a busy clock may differ from its drawn time by float error and still fail
_BUSY_EPSILON = 1e-9


def run_breakdowns(
    env: Any,
    resource: ResourceBase,
    sampler: Sampler,
    work: str = "interrupt",
    clock: str = "calendar",
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
        clock: `calendar` counts the time to failure from the start or the last
            repair, busy or idle. `busy` counts it only while the resource holds at
            least one unit, so an idle resource does not wear out. The time to
            failure is drawn once for each cycle in both. On `busy`, a time to failure
            of 0 fails at the grant of the first unit.

    Raises:
        ValueError: If `work` is neither `interrupt` nor `finish`, or `clock` is
            neither `calendar` nor `busy`.
    """
    if work not in BREAKDOWN_WORK:
        raise ValueError(f"work must be interrupt or finish, got '{work}'")
    if clock not in BREAKDOWN_CLOCKS:
        raise ValueError(f"clock must be calendar or busy, got '{clock}'")
    path = f"{resource.sim_id}.breakdown.{resource.obj_id}"
    resource.has_breakdowns = True
    while True:
        failure = env.registry.get_config(f"{path}.time_to_failure")
        if clock == "busy":
            yield from _busy_time(env, resource, sampler.sample(failure))
        else:
            yield env.timeout(sampler.sample(failure))
        resource.fail(interrupt_work=work == "interrupt")
        _publish(env, resource, "down")
        repair = env.registry.get_config(f"{path}.repair")
        yield env.timeout(sampler.sample(repair))
        resource.repair()
        _publish(env, resource, "repaired")


def _busy_time(
    env: Any, resource: ResourceBase, left: float
) -> Generator[Any, Any, None]:
    """Waits until the resource has held a unit for `left` seconds in all.

    A timer for the time left starts when the resource holds a unit and is
    abandoned when it holds none. The timer is made after the grant event, so a job
    whose end falls at the instant the clock reaches its value ends first, and the
    failure follows in the same instant.

    Args:
        env: The environment.
        resource: The resource whose busy time counts.
        left: The busy time to wait for, in seconds.
    """
    while True:
        if not resource.users:
            yield resource.when_busy()
            continue  # the unit may be given back again before this step
        if left <= 0:
            return  # a time to failure of 0 fails at the grant
        began = env.now
        # An internal timer, so one left unused when the resource goes idle does not
        # keep a run with no end time going
        idle = resource.when_idle()
        yield InternalTimeout(env, left) | idle
        if not idle.triggered:
            # The timer won: drop the event, so a resource that never goes idle does
            # not collect one for each failure
            resource._forget_waiter(idle)
        left -= env.now - began
        if left <= _BUSY_EPSILON:
            return


def _publish(env: Any, resource: ResourceBase, status: str) -> None:
    publish = getattr(env, "publish_event", None)
    if publish is not None:
        publish(resource.obj_id, {"status": status, "reason": "failure"})

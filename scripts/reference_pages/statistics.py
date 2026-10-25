"""The Statistic Names page."""

from typing import Dict, List

# What each statistic measures. The code knows the names; these say what they mean.
MEANINGS: Dict[str, str] = {
    "resources.capacity": "units now",
    "resources.in_use": "units held by tasks; above `capacity` after a drop, until they finish",
    "resources.queue_length": "requests waiting now",
    "resources.utilization": "`in_use` as a percentage of `capacity`, now",
    "resources.utilization_avg": "utilisation averaged over time, in percent",
    "resources.queue_length_avg": "requests waiting, averaged over time",
    "resources.down": "1 while a resource with a `breakdown` is down, 0 otherwise",
    "resources.downtime": "seconds a resource with a `breakdown` has been down so far",
    "resources.failures": "failures of a resource with a `breakdown` so far",
    "stores.capacity": "items it can hold now",
    "stores.level": "items held now",
    "stores.getters_waiting": "tasks waiting to get an item",
    "stores.putters_waiting": "tasks waiting to put an item",
    "stores.level_avg": "items held, averaged over time",
    "containers.capacity": "largest amount, now",
    "containers.level": "amount held now",
    "containers.getters_waiting": "tasks waiting to take an amount",
    "containers.putters_waiting": "tasks waiting to add an amount",
    "containers.level_avg": "amount held, averaged over time",
    "tasks.finished": "instances finished so far",
    "tasks.in_progress": "instances queued or started and not yet ended, now",
    "tasks.balked": "instances that refused to join the queue; published once one does",
    "tasks.reneged": "instances that left the queue before they were served; published once one does",
    "tasks.wait_mean": "mean time from `queued` to `started`",
    "tasks.wait_max": "longest time from `queued` to `started`",
    "tasks.service_mean": "mean time from `started` to finished",
    "tasks.service_max": "longest time from `started` to finished",
    "tasks.wait_p50": "median wait",
    "tasks.wait_p95": "the wait 95% of instances stay under",
    "tasks.wait_p99": "the wait 99% of instances stay under",
    "tasks.service_p50": "median service time",
    "tasks.service_p95": "the service time 95% of instances stay under",
    "tasks.service_p99": "the service time 99% of instances stay under",
    "flows.finished": "entities that left the model, by the arrival that started them",
    "flows.started": "entities whose first task has started, by the arrival that started them",
    "flows.in_system": "entities in the model now: started, and not yet finished, failed or gone",
    "flows.failed": "entities whose flow ended in a failure no `on_fail` handled; published once one does",
    "tasks.interrupted": "interrupts delivered to instances, preemptions not included; published once one is",
    "tasks.preempted": "times a higher-priority request took an instance's unit; published once one does",
    "tasks.resumed": "times an interrupted instance got its units back and was served again; published once one does",
    "tasks.failed": "instances that failed: their `fail` held, their function raised or their wait failed; published once one does",
}


def statistic_names() -> Dict[str, List[str]]:
    """Every name each level publishes, by level, found by running the statistics."""
    from desflow import (
        CapacityConfig,
        ContainerConfig,
        DynamicContainer,
        DynamicRealtimeEnvironment,
        DynamicResource,
        DynamicStore,
        SimParameter,
    )
    from desflow.config import STATS_LEVELS

    names: Dict[str, List[str]] = {}
    for level in STATS_LEVELS:
        env = DynamicRealtimeEnvironment(factor=0)
        env.registry.register_sim_parameter(
            SimParameter(
                sim_id="<sim_id>",
                resources={"<name>": CapacityConfig(current_cap=1, max_cap=1)},
                stores={"<name>": CapacityConfig(current_cap=1, max_cap=1)},
                containers={"<name>": ContainerConfig(current_cap=1, max_cap=1)},
            )
        )
        env.setup_stats("<sim_id>", level)
        # A resource with a breakdown, as run_breakdowns marks it, adds its readings
        DynamicResource(env, "<sim_id>", "<name>").has_breakdowns = True
        DynamicStore(env, "<sim_id>", "<name>")
        DynamicContainer(env, "<sim_id>", "<name>")
        env.stats.task_queued("<task>", 0)
        env.stats.task_started("<task>", 0)
        env.stats.task_finished("<task>", 0)
        env.stats.flow_started("<arrival>")
        env.stats.task_balked("<task>", 1)
        env.stats.task_queued("<task>", 2)
        env.stats.task_reneged("<task>", 2)
        env.stats.task_queued("<task>", 3)
        env.stats.task_failed("<task>", 3)
        env.stats.flow_failed("<arrival>")
        env.stats.task_queued("<task>", 4)
        env.stats.task_started("<task>", 4)
        env.stats.task_interrupted("<task>", 4)
        env.stats.task_interrupted("<task>", 4, preempted=True)
        env.stats.task_resumed("<task>", 4)
        env.stats.task_queued("<task>", 5)
        env.stats.flow_finished("<arrival>")
        names[level] = list(env.stats.readings())
        env.teardown()
    return names


def _meaning(name: str) -> str:
    parts = name.split(".")
    return MEANINGS[f"{parts[2]}.{parts[-1]}"]


def statistics_page() -> str:
    """The Statistic Names reference page, as Markdown."""
    names = statistic_names()
    lines: List[str] = [
        "# Statistic Names",
        "",
        "Every name the built-in statistics publish, at each level, generated by running "
        "the statistics in the code. [Statistics](../user-guide/statistics.md) explains "
        "the levels and how each value is computed. Each level publishes the names of the "
        "levels before it, and the new ones listed under it. `<sim_id>` is the "
        "simulation's id, `<name>` a resource, store or container, `<task>` a task, and "
        "`<arrival>` the arrival that started a flow's entities.",
    ]
    seen: List[str] = []
    for level, published in names.items():
        new = [name for name in published if name not in seen]
        seen += new
        lines += ["", f"## {level}", ""]
        if not new:
            lines.append("Publishes nothing: the statistics are off.")
            continue
        lines += ["| Name | Measures |", "|---|---|"]
        lines += [f"| `{name}` | {_meaning(name)} |" for name in new]
    return "\n".join(lines) + "\n"

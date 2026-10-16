"""Generate the Reference pages that the code already knows: Distributions and Statistics.

mkdocs-gen-files runs this file during every docs build (the `gen-files` plugin in
mkdocs.yml), so the pages are built from the code and cannot disagree with it. The
functions return Markdown, so tests/docs/test_gen_pages.py checks them without a
build.

- Distributions: every family in `desflow.core.distributions.DISTRIBUTIONS`, with its
  parameters, rules and registry paths, and the mean, variance and value with no
  random generator of one example, computed by the family's own functions.
- Statistics: every name each level publishes, found by running the statistics on a
  model with one resource, store, container, task and flow.
"""

from typing import Dict, List

# One example per family, in the units a blueprint uses. The code does not know a
# typical value, so these are written here; every number below them is computed.
EXAMPLES: Dict[str, Dict[str, object]] = {
    "exponential": {"rate": 0.5},
    "normal": {"mean": 3.0, "std": 0.5},
    "lognormal": {"mean": 4.0, "std": 3.0},
    "truncated_normal": {"mean": 1.0, "std": 1.0, "low": 0.0},
    "constant": {"value": 1.5},
    "uniform": {"low": 1.0, "high": 2.0},
    "triangular": {"low": 1.0, "mode": 1.25, "high": 2.25},
    "gamma": {"shape": 9.0, "scale": 0.1667},
    "weibull": {"shape": 1.5, "scale": 3600.0},
    "empirical": {"values": [1.2, 1.5, 1.5, 2.1, 3.0]},
}

# What each statistic measures. The code knows the names; these say what they mean.
MEANINGS: Dict[str, str] = {
    "resources.capacity": "units now",
    "resources.in_use": "units held by tasks; above `capacity` after a drop, until they finish",
    "resources.queue_length": "requests waiting now",
    "resources.utilization": "`in_use` as a percentage of `capacity`, now",
    "resources.utilization_avg": "utilisation averaged over time, in percent",
    "resources.queue_length_avg": "requests waiting, averaged over time",
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
}


def _number(value: float) -> str:
    return f"{value:.4g}"


def distributions_page() -> str:
    """The Distributions reference page, as Markdown."""
    from desflow.core.distributions import (
        DISTRIBUTIONS,
        TIME_LIST_PARAMETERS,
        TIME_PARAMETERS,
    )
    from desflow.models.params import DistributionConfig

    lines: List[str] = [
        "# Distributions",
        "",
        "Every distribution family desflow can draw from, generated from the registry of "
        "distributions in the code. [Arrivals and Services](../user-guide/arrivals-and-services.md) "
        "explains how a draw works, and how each family is seeded and changed live.",
        "",
        "Each family lists its parameters, the rules they must follow, and the registry "
        "path of each parameter that can change during a run. The mean, variance and "
        "value with no random generator are computed by the family's own functions for "
        "the example given. In a blueprint, a time-valued parameter "
        f"({', '.join(f'`{p}`' for p in TIME_PARAMETERS + TIME_LIST_PARAMETERS)}) also takes "
        "a duration such as `5 min`.",
        "",
        "| Family | Parameters | Live paths |",
        "|---|---|---|",
    ]
    for name, family in DISTRIBUTIONS.items():
        live = ", ".join(f"`.{p}`" for p in family.live) or "none"
        params = ", ".join(f"`{p}`" for p in family.parameters)
        lines.append(f"| [`{name}`](#{name}) | {params} | {live} |")
    for name, family in DISTRIBUTIONS.items():
        example = EXAMPLES[name]
        config = DistributionConfig(dist=name, **example)  # type: ignore[arg-type]
        yaml_line = ", ".join(
            f"{key}: {value}" for key, value in {"dist": name, **example}.items()
        ).replace("'", "")
        defaults = ", ".join(f"`{k}` {v:g}" for k, v in family.defaults.items())
        lines += [
            "",
            f"## {name}",
            "",
            family.summary,
            "",
            "| | |",
            "|---|---|",
            f"| `dist` | `{name}` |",
            f"| Parameters | {', '.join(f'`{p}`' for p in family.parameters)} |",
            f"| Defaults | {defaults or 'none'} |",
            f"| Rules | {family.rules} |",
            "| Registry paths | "
            + (
                ", ".join(f"`<sim_id>.service.<name>.{p}`" for p in family.live)
                + " (and `.arrival.` for an arrival)"
                if family.live
                else "none: the lists are taken at load"
            )
            + " |",
            f"| Uses a random number | {'yes' if family.random else 'no'} |",
            "",
            f"For the example `{{{yaml_line}}}`: mean {_number(family.mean(config))}, "
            f"variance {_number(family.variance(config))}, and with no random "
            f"generator every draw is {_number(family.fixed(config))}.",
        ]
    return "\n".join(lines) + "\n"


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
        DynamicResource(env, "<sim_id>", "<name>")
        DynamicStore(env, "<sim_id>", "<name>")
        DynamicContainer(env, "<sim_id>", "<name>")
        env.stats.task_queued("<task>", 0)
        env.stats.task_started("<task>", 0)
        env.stats.task_finished("<task>", 0)
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


def write() -> None:
    """Writes both pages into the docs build."""
    import mkdocs_gen_files

    for path, page in (
        ("reference/distributions.md", distributions_page()),
        ("reference/statistics.md", statistics_page()),
    ):
        with mkdocs_gen_files.open(path, "w") as file:
            file.write(page)
        mkdocs_gen_files.set_edit_path(path, "scripts/gen_pages.py")


# mkdocs-gen-files runs this file with runpy, under this name
if __name__ == "<run_path>":
    write()

"""The blocks of the User Guide pages on the parts of a model: Arrivals and
Services, Tasks and Processes, Resources, Stores and Containers, Task Flow and
Statistics."""

from typing import Callable, Dict, List

from .capture import (
    event_lines,
    fields,
    load_script,
    logged_lines,
    printed_lines,
    script_output,
    stat_lines,
    yaml_records,
)
from .commands import command


def invalid_parameters() -> List[str]:
    """Arrivals and Services: the errors of three refused distributions."""
    return printed_lines(
        lambda: load_script("docs/snippets/pages/arrivals_services.py")
    )


def refused_update() -> List[str]:
    """Arrivals and Services: the log line of a refused live update."""
    return logged_lines(
        "desflow.core.registry",
        lambda: load_script("docs/snippets/pages/arrivals_services.py"),
    )


def task_events() -> List[str]:
    """Tasks and Processes: the events of orders-1, packed and then labelled."""
    lines = event_lines(yaml_records("docs/snippets/yaml/tasks.yaml"))
    return [line for line in lines if fields(line)["key"] == "orders-1"]


def resources_printed() -> List[str]:
    """Resources: the order each queue order serves, and a capacity drop."""
    return script_output("docs/snippets/pages/resources.py")


def holder_readings() -> List[str]:
    """Stores and Containers: the readings before and after the stock count."""
    lines = yaml_records("docs/snippets/yaml/stores_containers.yaml")
    keep = (
        "containers.stock.level'",
        "containers.stock.getters_waiting'",
        "stores.parcels.level'",
        "stores.parcels.putters_waiting'",
    )
    return [
        line
        for line in lines
        if any(k in line for k in keep) and fields(line)["sim_ts"] in (30.0, 40.0)
    ]


def gift_flow() -> List[str]:
    """Task Flow: the finished events of gifts-5, backordered once, then wrapped."""
    lines = event_lines(yaml_records("docs/snippets/yaml/task_flow.yaml"))
    return [
        line
        for line in lines
        if fields(line)["key"] == "gifts-5" and "status" not in fields(line)["value"]
    ]


def refused_expressions() -> List[str]:
    """Task Flow: the errors of three refused expressions."""
    return printed_lines(lambda: load_script("docs/snippets/pages/task_flow.py"))


def statistics_final() -> List[str]:
    """Statistics: the statistics published at the end of the run."""
    lines = stat_lines(yaml_records("docs/snippets/yaml/statistics.yaml"))
    return [line for line in lines if fields(line)["sim_ts"] == 120]


def arrivals_dist() -> List[str]:
    """Arrivals and Services: the gamma of a mean and a std, from `desflow dist`."""
    lines = command("dist", "gamma", "--mean", "1.5", "--std", "0.5")
    return [lines[0], *lines[-2:]]


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "arrivals-dist": arrivals_dist,
    "statistics-final": statistics_final,
    "gift-flow": gift_flow,
    "refused-expressions": refused_expressions,
    "holder-readings": holder_readings,
    "resources-printed": resources_printed,
    "task-events": task_events,
    "invalid-parameters": invalid_parameters,
    "refused-update": refused_update,
}

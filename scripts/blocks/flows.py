"""The blocks of Stock, Priorities and A Flow of Tasks."""

from typing import Callable, Dict, List

from .capture import capture, event_lines, fields, load_script, yaml_records
from .commands import resources_and_reasons


def stock_out() -> List[str]:
    """Stock: the stock's readings around the second stock-out of 40-box deliveries."""
    module = load_script("docs/snippets/tutorials/stock.py")
    lines = capture(lambda: module.shop(40).run(until=210))
    keep = (".stock.level'", ".stock.getters_waiting'", ".packers.queue_length'")
    return [
        line
        for line in lines
        if any(k in line for k in keep) and 170 <= fields(line)["sim_ts"] <= 200
    ]


def stock_table() -> List[str]:
    """Stock: the mean wait for 60-box and 40-box deliveries."""
    module = load_script("docs/snippets/tutorials/stock.py")
    lines: List[str] = []

    def run() -> None:
        for delivery in (60, 40):
            wait = module.mean_wait(delivery)
            lines.append(f"deliveries of {delivery}: mean wait {wait:6.2f} s")

    capture(run)
    return lines


def priorities_table() -> List[str]:
    """Priorities: the mean wait of each kind of order under each queue order."""
    module = load_script("docs/snippets/tutorials/priorities.py")
    lines: List[str] = []

    def run() -> None:
        for queue in ("fifo", "priority"):
            for task, wait in module.mean_waits(queue).items():
                lines.append(f"{queue:<8} {task:<12} mean wait {wait:5.2f} s")

    capture(run)
    return lines


def one_order_flow() -> List[str]:
    """A Flow of Tasks: every event of orders-172, backordered and packed twice."""
    lines = event_lines(yaml_records("docs/snippets/yaml/flow_of_tasks.yaml"))
    return [line for line in lines if fields(line)["key"] == "orders-172"]



def priorities_explain() -> List[str]:
    """Priorities: `desflow explain` on the hour at its default rates."""
    return resources_and_reasons("docs/snippets/yaml/priorities.yaml")


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "one-order-flow": one_order_flow,
    "priorities-table": priorities_table,
    "priorities-explain": priorities_explain,
    "stock-table": stock_table,
    "stock-out": stock_out,
}

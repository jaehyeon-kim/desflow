"""The blocks of First Simulation, Queues and Capacity, Checking a Model and
Complete Model, and of Queueing Basics."""

import functools
from pathlib import Path
from typing import Callable, Dict, List

from .capture import (
    ROOT,
    event_lines,
    load_script,
    records_until,
    script_output,
    yaml_records,
)
from .commands import command, explain, lines_between, lines_from, sample


def first_trace() -> List[str]:
    """First Simulation: the events of the first three orders."""
    return records_until(
        event_lines(yaml_records("docs/snippets/yaml/first_shop.yaml")), 6
    )


def first_sample() -> List[str]:
    """First Simulation: the first five events, from `desflow run --sample 5`."""
    return sample("docs/snippets/yaml/first_shop.yaml", 5)


def queues_table() -> List[str]:
    """Queues and Capacity: the mean wait of the busy hour for 1 to 4 packers."""
    module = load_script("docs/snippets/tutorials/queues_and_capacity.py")
    return [f"packers {n}: mean wait {module.mean_wait(n):8.2f} s" for n in range(1, 5)]


def queues_explain() -> List[str]:
    """Queues and Capacity: `desflow explain` on the busy hour."""
    return explain("docs/snippets/yaml/queues_and_capacity.yaml")


def queueing_explain() -> List[str]:
    """Queueing Basics: the resource table of `desflow explain` with three packers."""
    lines = explain("docs/snippets/yaml/queues_and_capacity.yaml", PACKERS="3")
    start = lines.index("Resources")
    return lines[start : start + 6]


def queueing_verify() -> List[str]:
    """Queueing Basics: `desflow explain --verify` on the busy hour with exponential packing."""
    lines = explain(
        "docs/snippets/yaml/choosing_inputs.yaml",
        "--verify",
        "--replications",
        "10",
        PACKING="exponential",
    )
    start = next(i for i, line in enumerate(lines) if line.startswith("Verify:"))
    return lines[start : start + 9]


def queues_network() -> List[str]:
    """Queues and Capacity: the network of packing and checking, with why neither
    wait is estimated, up to its hints."""
    lines = explain("docs/snippets/yaml/queues_network.yaml")
    return lines[: next(i for i, line in enumerate(lines) if line.startswith("Hints"))]


def queues_overload() -> List[str]:
    """Queues and Capacity: the hints for deliberate overload."""
    lines = explain("docs/snippets/yaml/queues_and_capacity.yaml", "--target", "1.2")
    return lines_from(lines, "Hints for")


def checking_explain() -> List[str]:
    """Checking a Model: the busy hour of Trusting Results against the formulas."""
    lines = explain(
        "docs/snippets/yaml/trusting_results.yaml", "--verify", "--replications", "10"
    )
    return lines_from(lines, "Not estimated:")


def complete_fit() -> List[str]:
    """Complete Model: the candidates for the observed packing times."""
    return command(
        "fit",
        "docs/snippets/tutorials/complete_model_observed.csv",
        "--column",
        "packing_seconds",
        "--lower",
        "0",
    )


def complete_explain() -> List[str]:
    """Complete Model: the resources, what is not estimated, and the verify table."""
    lines = explain(
        "docs/snippets/yaml/complete_model.yaml",
        "--verify",
        "--replications",
        "10",
    )
    return (
        lines_between(lines, "Resources", "pickers by units")
        + lines_between(lines, "Bottleneck:", "Hints for")
        + lines_from(lines, "Verify:")
    )


def complete_experiment() -> List[str]:
    """Complete Model: the comparisons, checks and observed values of its report."""
    import tempfile

    from desflow import run_experiment

    with tempfile.TemporaryDirectory() as folder:
        report = run_experiment(
            ROOT / "docs/snippets/yaml/complete_model_experiment.yaml",
            output=Path(folder),
        ).report.split("\n")
    return report[report.index("## Comparisons") : report.index("## Limits") - 1]


@functools.lru_cache(maxsize=None)
def _queueing() -> List[str]:
    """What the Queueing Basics snippet prints, run once for all of its blocks."""
    return script_output("docs/snippets/pages/queueing_basics.py")


def _queueing_lines(start: int, end: int) -> Callable[[], List[str]]:
    """Queueing Basics: the printed lines from `start` up to `end`, for one block."""
    return lambda: _queueing()[start:end]


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "queueing-utilisation": _queueing_lines(0, 1),
    "queueing-busy": _queueing_lines(1, 2),
    "queueing-little": _queueing_lines(2, 5),
    "queueing-formulas": _queueing_lines(5, 9),
    "queueing-packers": _queueing_lines(9, 11),
    "queueing-checks": _queueing_lines(11, 15),
    "queueing-kendall": _queueing_lines(15, 16),
    "queueing-dm1": _queueing_lines(16, 17),
    "queueing-flow": _queueing_lines(17, 20),
    "queueing-explain": queueing_explain,
    "queueing-verify": queueing_verify,
    "first-trace": first_trace,
    "first-sample": first_sample,
    "queues-table": queues_table,
    "queues-explain": queues_explain,
    "queues-network": queues_network,
    "queues-overload": queues_overload,
    "checking-explain": checking_explain,
    "complete-fit": complete_fit,
    "complete-explain": complete_explain,
    "complete-experiment": complete_experiment,
}

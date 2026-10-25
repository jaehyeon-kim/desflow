"""The blocks of Randomness and Seeds, Choosing Inputs, Measuring a Run, Trusting
Results and Fitting the Inputs."""

import contextlib
import logging
from pathlib import Path
from typing import Callable, Dict, List
from unittest import mock

from .capture import DOCS, capture, cwd, fields, load_script, stat_lines, yaml_records
from .commands import command

# A run without a seed draws one at random. The seed lines show this one, so the
# page does not change on every run; the line is otherwise the code's own.
_DRAWN_SEED = 3166812249


def _seed_line(run: Callable[[], object]) -> List[str]:
    """The line an unseeded run logs about its seed."""
    lines: List[str] = []

    class _Keep(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if "random_seed" in record.getMessage():
                lines.append(record.getMessage())

    handler = _Keep(logging.INFO)
    logger = logging.getLogger("desflow.core.context")
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        with mock.patch("secrets.randbelow", return_value=_DRAWN_SEED):
            capture(run)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
    return lines


def seed_line_yaml() -> List[str]:
    """Randomness and Seeds: what `desflow run` logs without a seed."""
    from desflow import SimulationContext

    def run() -> None:
        with cwd(DOCS / "snippets" / "yaml"):
            app = SimulationContext.from_yaml(Path("randomness_and_seeds.yaml"))
        app.random_seed = None
        app.run(until=1)

    return _seed_line(run)


def seed_line_builder() -> List[str]:
    """Randomness and Seeds: what a builder run logs without a seed."""

    def run() -> None:
        app = load_script("docs/snippets/tutorials/randomness_and_seeds.py").app
        app.random_seed = None
        app.run(until=1)

    return _seed_line(run)


def twelfth_order() -> List[str]:
    """Randomness and Seeds: the events of orders-12 with the seed 42."""
    lines = yaml_records("docs/snippets/yaml/randomness_and_seeds.yaml")
    return [line for line in lines if fields(line).get("key") == "orders-12"]


def choosing_table() -> List[str]:
    """Choosing Inputs: the mean wait of the busy hour for each packing candidate."""
    module = load_script("docs/snippets/tutorials/choosing_inputs.py")
    return [
        f"{name:<12} mean wait {module.mean_wait(name):5.2f} s"
        for name in module.CANDIDATES
    ]


def choosing_dist() -> List[str]:
    """Choosing Inputs: the gamma with the measured times' mean and std."""
    return command("dist", "gamma", "--mean", "1.5", "--std", "0.4832")


def measuring_final() -> List[str]:
    """Measuring a Run: the statistics published at the end of the busy hour."""
    lines = stat_lines(yaml_records("docs/snippets/yaml/measuring_a_run.yaml"))
    return [line for line in lines if fields(line)["sim_ts"] == 3600]


def trusting_table() -> List[str]:
    """Trusting Results: the mean wait without and with the warm-up."""
    module = load_script("docs/snippets/tutorials/trusting_results.py")
    lines: List[str] = []

    def run() -> None:
        lines.append(f"without a warm-up: mean wait {module.mean_wait(None):.3f} s")
        lines.append(
            f"with a 2 min warm-up: mean wait {module.mean_wait('2 min'):.3f} s"
        )

    capture(run)
    return lines


def fit_table() -> List[str]:
    """Fitting the Inputs: the table of candidates for the 40 packing times."""
    import io

    module = load_script("docs/snippets/tutorials/fit_the_inputs.py")
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        module.fitted()
    return printed.getvalue().rstrip("\n").split("\n")


def trusting_level() -> List[str]:
    """Trusting Results: the mean wait of 40 replications with a 90% interval."""
    lines = command(
        "run",
        "docs/snippets/yaml/trusting_results.yaml",
        "--replications",
        "40",
        "--measure",
        "tasks.pack_order.wait_mean",
        "--level",
        "0.9",
    )
    return lines[: lines.index("")]


def choosing_rate() -> List[str]:
    """Choosing Inputs: the mean and spread of the busy hour's arrival gaps."""
    return command("dist", "exponential", "--rate", "1.8")[:3]


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "trusting-table": trusting_table,
    "fit-table": fit_table,
    "measuring-final": measuring_final,
    "choosing-table": choosing_table,
    "choosing-dist": choosing_dist,
    "choosing-rate": choosing_rate,
    "trusting-level": trusting_level,
    "seed-line-yaml": seed_line_yaml,
    "seed-line-builder": seed_line_builder,
    "twelfth-order": twelfth_order,
}

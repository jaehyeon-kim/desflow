"""The blocks of Command Line, each a command run in a folder of downloaded
examples."""

import functools
import re
from pathlib import Path
from typing import Callable, Dict, List

from .commands import in_folder


@functools.lru_cache(maxsize=None)
def _workflow() -> Path:
    """The folder of Command Line: the experiment and fitting examples, downloaded,
    and the experiment run once, so the report can be built from its files."""
    import atexit
    import shutil
    import tempfile

    folder = Path(tempfile.mkdtemp(prefix="desflow-cli-")).resolve()
    atexit.register(shutil.rmtree, folder, True)
    for name in ("experiment", "fitting"):
        in_folder(folder, "examples", "get", name, "--api", "yaml")
    in_folder(
        folder, "experiment", "experiment/yaml/experiment.yaml",
        "--output", "results/packers",
    )  # fmt: skip
    return folder


_CLI_SHOP = "experiment/yaml/experiment_shop.yaml"


def cli_get() -> List[str]:
    """Command Line: `desflow examples get experiment`, in an empty folder."""
    import tempfile

    with tempfile.TemporaryDirectory() as folder:
        return in_folder(
            Path(folder).resolve(), "examples", "get", "experiment", "--api", "yaml"
        )


def cli_get_dest() -> List[str]:
    """Command Line: `desflow examples get` into another folder, in an empty folder."""
    import tempfile

    with tempfile.TemporaryDirectory() as folder:
        return in_folder(
            Path(folder).resolve(), "examples", "get", "experiment", "--api",
            "builder", "--dest", "shop", "--ref", "main",
        )  # fmt: skip


def cli_explain() -> List[str]:
    """Command Line: `desflow explain` on the experiment's shop."""
    return in_folder(_workflow(), "explain", _CLI_SHOP)


def cli_sample() -> List[str]:
    """Command Line: the first five events of the experiment's shop."""
    lines = in_folder(_workflow(), "run", _CLI_SHOP, "--sample", "5")
    return [re.sub(r'"timestamp":"[^"]*"', '"timestamp":"..."', line) for line in lines]


def cli_replications() -> List[str]:
    """Command Line: ten replications of one hour of the experiment's shop."""
    return in_folder(
        _workflow(), "run", _CLI_SHOP, "--replications", "10", "--seed", "42",
        "--until", "1h",
    )  # fmt: skip


def cli_verify() -> List[str]:
    """Command Line: `desflow explain --verify` with ten replications of one hour."""
    return in_folder(
        _workflow(), "explain", _CLI_SHOP, "--verify", "--replications", "10",
        "--seed", "42", "--until", "1h", "--target", "0.9",
    )  # fmt: skip


def cli_measure() -> List[str]:
    """Command Line: two measures of the replications, at a 99% level."""
    return in_folder(
        _workflow(), "run", _CLI_SHOP, "--replications", "10", "--seed", "42",
        "--until", "1h", "--measure", "tasks.pack.wait_mean",
        "--measure", "resources.packers.utilization_avg", "--level", "0.99",
        "--workers", "2",
    )  # fmt: skip


def cli_experiment() -> List[str]:
    """Command Line: `desflow experiment` on the experiment example."""
    return in_folder(
        _workflow(), "experiment", "experiment/yaml/experiment.yaml",
        "--output", "results/packers",
    )  # fmt: skip


def cli_report() -> List[str]:
    """Command Line: `desflow report` on the experiment's files."""
    return in_folder(_workflow(), "report", "results/packers")


def cli_fit() -> List[str]:
    """Command Line: `desflow fit` on the 40 packing times of the fitting example."""
    return in_folder(
        _workflow(), "fit", "fitting/packing_times.csv", "--column", "packing_seconds",
        "--lower", "0",
    )  # fmt: skip


def cli_fit_options() -> List[str]:
    """Command Line: `desflow fit` on three families, writing the best and a plot."""
    return in_folder(
        _workflow(), "fit", "fitting/packing_times.csv", "--column", "packing_seconds",
        "--unit", "s", "--lower", "0", "--upper", "10",
        "--candidates", "triangular,gamma,empirical", "--bins", "8",
        "--min-count", "30", "--tolerance", "0.2", "--drop-invalid",
        "--out", "fitting/yaml/packing_fit.yaml", "--plot", "packing_fit.png",
    )  # fmt: skip


def cli_dist_triangular() -> List[str]:
    """Command Line: `desflow dist triangular` from the fitted low, mode and high."""
    return in_folder(
        _workflow(), "dist", "triangular", "--low", "0.55", "--mode", "1.2",
        "--high", "2.84",
    )  # fmt: skip


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "cli-get": cli_get,
    "cli-get-dest": cli_get_dest,
    "cli-explain": cli_explain,
    "cli-verify": cli_verify,
    "cli-sample": cli_sample,
    "cli-replications": cli_replications,
    "cli-measure": cli_measure,
    "cli-experiment": cli_experiment,
    "cli-report": cli_report,
    "cli-fit": cli_fit,
    "cli-fit-options": cli_fit_options,
    "cli-dist-triangular": cli_dist_triangular,
}

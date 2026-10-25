"""The blocks of the User Guide pages on writing and running a model: Blueprints,
Live Parameters, Ingress, Builder API, Replications and Experiment Results."""

import contextlib
import os
import re
from pathlib import Path
from typing import Callable, Dict, List

from .capture import (
    ROOT,
    capture,
    cwd,
    event_lines,
    fields,
    load_script,
    printed_lines,
    yaml_records,
)


def replications_telemetry() -> List[str]:
    """Replications: the telemetry a sink receives from three replications."""
    module = load_script("docs/snippets/pages/replications.py")
    return capture(module.builder)


def replications_summary() -> List[str]:
    """Replications: the summary of the same three replications."""
    module = load_script("docs/snippets/pages/replications.py")
    result: List[object] = []
    capture(lambda: result.append(module.builder()))
    return result[0].summary_text().split("\n")  # type: ignore[attr-defined]


def experiment_report() -> List[str]:
    """Experiments: the results, comparison, check and observed part of the
    experiment example's report."""
    import tempfile

    module = load_script("examples/builder/experiment_example.py")
    with tempfile.TemporaryDirectory() as folder:
        with contextlib.redirect_stdout(open(os.devnull, "w")):
            report = module.run(output=Path(folder)).report.split("\n")
    return report[report.index("## Results") : report.index("## Limits") - 1]


def blueprint_error() -> List[str]:
    """Blueprints: what a blueprint with a mistake prints."""
    with cwd(ROOT):
        return printed_lines(
            lambda: load_script("docs/snippets/pages/blueprint_errors.py")
        )


def label_records() -> List[str]:
    """Live Parameters: the ground-truth records the labels sink wrote."""
    import tempfile

    with tempfile.TemporaryDirectory() as folder, cwd(Path(folder)):
        yaml_records("docs/snippets/yaml/ground_truth.yaml")
        lines: List[str] = []
        for file in sorted(Path("labels").glob("*.jsonl")):
            lines += file.read_text(encoding="utf-8").splitlines()
    return [re.sub(r'"timestamp":"[^"]*"', '"timestamp":"..."', line) for line in lines]


def change_lands() -> List[str]:
    """Live Parameters: the packers' capacity before and after a scenario step."""
    lines = yaml_records("docs/snippets/yaml/scenario.yaml")
    return [
        line
        for line in lines
        if "packers.capacity'" in line and fields(line)["sim_ts"] in (20.0, 30.0, 40.0)
    ]


def ingress_old_keys() -> List[str]:
    """Ingress: the line a message with the old keys is skipped with."""
    return printed_lines(lambda: load_script("docs/snippets/pages/ingress_senders.py"))


def builder_first() -> List[str]:
    """Builder API: the first events of the guide's simulation."""
    import runpy

    path = str(ROOT / "docs/snippets/pages/builder.py")
    lines = capture(lambda: runpy.run_path(path, run_name="__main__"))
    return event_lines(lines)[:8]


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "builder-first": builder_first,
    "ingress-old-keys": ingress_old_keys,
    "label-records": label_records,
    "change-lands": change_lands,
    "blueprint-error": blueprint_error,
    "replications-telemetry": replications_telemetry,
    "replications-summary": replications_summary,
    "experiment-report": experiment_report,
}

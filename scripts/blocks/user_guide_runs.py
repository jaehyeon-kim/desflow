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
    # The last digits of a mean differ between macOS and Linux, so the page shows 10
    return [
        re.sub(
            r"('value': )(-?\d+\.\d+(?:e[+-]?\d+)?)",
            lambda m: m.group(1) + f"{float(m.group(2)):.10g}",
            line,
        )
        for line in capture(module.builder)
    ]


def replications_summary() -> List[str]:
    """Replications: the summary of the same three replications."""
    module = load_script("docs/snippets/pages/replications.py")
    result: List[object] = []
    capture(lambda: result.append(module.builder()))
    return result[0].summary_text().split("\n")  # type: ignore[attr-defined]


def replications_file() -> List[str]:
    """Replications: the file `desflow run --summary` writes for three replications of
    the example, run as the page gives the command in the example's folder."""
    import json
    import shutil
    import tempfile

    from typer.testing import CliRunner

    from desflow.cli import app

    with tempfile.TemporaryDirectory() as folder:
        shutil.copy(ROOT / "examples/yaml/replications.yaml", folder)
        with cwd(Path(folder)):
            result = CliRunner().invoke(
                app,
                ["run", "replications.yaml", "--replications", "3", "--measure",
                 "tasks.pack.wait_mean", "--summary", "result.json"],
            )  # fmt: skip
            assert result.exit_code == 0, result.output
            document = json.loads(Path("result.json").read_text(encoding="utf-8"))
    # The versions depend on the machine, and the last digits of a mean differ between
    # macOS and Linux, so the page shows 10
    document["versions"] = {name: "..." for name in document["versions"]}
    text = json.dumps(document, indent=2)
    return re.sub(
        r"(?<=: )(-?\d+\.\d+(?:e[+-]?\d+)?)",
        lambda m: repr(float(f"{float(m.group(1)):.10g}")),
        text,
    ).split("\n")


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


def run_segments() -> List[str]:
    """Run Length: what the first two segments of the shop read and publish."""
    lines = yaml_records("docs/snippets/yaml/run_segments.yaml")
    return [
        line
        for line in lines
        if "queue_read" in line or "system.simulation.segment" in line
    ][:4]


def run_steps() -> List[str]:
    """Run Length: the first readings that `run.steps` publishes."""
    return yaml_records("docs/snippets/yaml/run_steps.yaml")[:7]


def generated_values() -> List[str]:
    """Blueprints: the first records of the generated-values blueprint."""
    return yaml_records("docs/snippets/yaml/generated_values.yaml")[:2]


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "builder-first": builder_first,
    "generated-values": generated_values,
    "run-segments": run_segments,
    "run-steps": run_steps,
    "ingress-old-keys": ingress_old_keys,
    "label-records": label_records,
    "change-lands": change_lands,
    "blueprint-error": blueprint_error,
    "replications-telemetry": replications_telemetry,
    "replications-summary": replications_summary,
    "replications-file": replications_file,
    "experiment-report": experiment_report,
}

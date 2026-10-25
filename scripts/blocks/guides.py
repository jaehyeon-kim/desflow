"""The blocks of the How-To pages and of Records and Telemetry."""

import contextlib
from typing import Callable, Dict, List

from .capture import (
    DOCS,
    cwd,
    event_lines,
    fields,
    load_script,
    printed_lines,
    yaml_records,
)
from .commands import explain, lines_from

_THEORY = "docs/snippets/yaml/check_against_theory.yaml"


def theory_one() -> List[str]:
    """Check Against Theory: one packer, verified with 10 runs."""
    return explain(_THEORY, "--verify", "--replications", "10")


def theory_two() -> List[str]:
    """Check Against Theory: two packers, the comparison."""
    lines = explain(
        _THEORY, "--verify", "--replications", "10", PACKERS="2", RATE="1.5"
    )
    return lines_from(lines, "Verify:")


def theory_python() -> List[str]:
    """Check Against Theory: the comparison from Python."""
    module = load_script("docs/snippets/guides/check_against_theory.py")
    return printed_lines(module.verify)


def theory_unsupported() -> List[str]:
    """Check Against Theory: two packers with constant packing."""
    module = load_script("docs/snippets/guides/check_against_theory.py")
    return printed_lines(module.unsupported)


def fit_gaps() -> List[str]:
    """Fit a Distribution: the table of candidates for the gaps between orders."""
    import io

    module = load_script("docs/snippets/guides/fit_a_distribution.py")
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        module.fit()
    return printed.getvalue().rstrip("\n").split("\n")


def record_examples() -> List[str]:
    """Records and Telemetry: one telemetry record and the events of one task."""
    readings = yaml_records("docs/snippets/yaml/scenario.yaml")
    telemetry = [line for line in readings if "packers.in_use'" in line][:1]
    lines = yaml_records("docs/snippets/yaml/tasks.yaml")
    events = [
        line
        for line in event_lines(lines)
        if fields(line)["key"] == "orders-1" and fields(line)["task"] == "pack_order"
    ]
    return telemetry + events


def hybrid_events() -> List[str]:
    """Call Python from YAML: the first packed order and the first break."""
    lines = event_lines(yaml_records("docs/snippets/yaml/hybrid.yaml"))
    packed = [line for line in lines if "'delivery'" in line][:1]
    breaks = [line for line in lines if "packers-break" in line][:2]
    return packed + breaks


def python_error() -> List[str]:
    """Call Python from YAML: what `desflow run` prints for a reference that does not
    resolve, run from the blueprint's folder."""
    from typer.testing import CliRunner

    from desflow.cli import app

    with cwd(DOCS / "snippets" / "yaml"):
        result = CliRunner().invoke(app, ["run", "python_error.yaml"])
    assert result.exit_code == 1, result.output
    return result.output.rstrip("\n").split("\n")


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "hybrid-events": hybrid_events,
    "python-error": python_error,
    "record-examples": record_examples,
    "fit-gaps": fit_gaps,
    "theory-one": theory_one,
    "theory-two": theory_two,
    "theory-python": theory_python,
    "theory-unsupported": theory_unsupported,
}

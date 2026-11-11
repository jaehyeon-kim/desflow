"""A refusal points at the line of the key it is about."""

from pathlib import Path

import pytest

from desflow.blueprint.loader import BlueprintError
from desflow.builder.context import SimulationContext

KEY_WITH_VARIABLE = """\
simulation: {sim_id: V, factor: 0, random_seed: 1}
services:
  work: {dist: exponential, rate: 1.0}
arrivals:
  orders: {dist: exponential, rate: 0.5, spawn: serve}
tasks:
  other:
    service: work
    payload: {}
  ${TASK:-serve}:
    service: work
    payload: {}
    bogus_key: 3
run: {until: 20}
"""


def test_a_key_written_with_a_variable_is_reported_at_its_own_line(tmp_path: Path):
    path = tmp_path / "m.yaml"
    path.write_text(KEY_WITH_VARIABLE)
    with pytest.raises(BlueprintError, match=r"m\.yaml:13: tasks\.serve\.bogus_key"):
        SimulationContext.from_yaml(path)


UNION_BASE = """\
simulation: {{sim_id: S, factor: 0, random_seed: 1}}
resources:
  desk:
    current_cap: 1{resource}
stores: {{shelf: {{}}}}
services:
  work: {{dist: exponential, rate: 1.0}}
arrivals:
  orders: {{dist: exponential, rate: 0.5, spawn: serve}}
signals: {{go: {{}}}}
tasks:
  serve:
    service: work{task_resource}
    payload: {{}}{task}
run:
  until: 20{run}
{stats}"""

UNION_CASES = {
    "preempt": (
        {"resource": "\n    preempt:\n      victim: x\n      bogus: 1"},
        "resources.desk.preempt.bogus",
    ),
    "drain": ({"run": "\n  drain:\n    bogus: 2"}, "run.drain.bogus"),
    "seize": (
        {"task": "\n    seize: {resource: desk, patiense: 1}"},
        "tasks.serve.seize.patiense",
    ),
    "release": (
        {"task": "\n    release: {resource: desk, tsk: serve}"},
        "tasks.serve.release.tsk",
    ),
    "cancel": ({"task": "\n    cancel: {handle: h, idx: 1}"}, "tasks.serve.cancel.idx"),
    "signal": ({"task": "\n    signal: {of: go, kee: 1}"}, "tasks.serve.signal.kee"),
    "resource": (
        {"task_resource": "\n    resource: {of: desk, kee: 1}"},
        "tasks.serve.resource.kee",
    ),
    "breakdown": (
        {"resource": "\n    breakdown: {clok: 1}"},
        "resources.desk.breakdown.clok",
    ),
    "stats": ({"stats": "stats: {intervl: 5}\n"}, "stats.intervl"),
    "listen": ({"task": "\n    listen: {onn: x}"}, "tasks.serve.listen.onn"),
    "next": ({"task": "\n    next: {wen: x}"}, "tasks.serve.next.wen"),
    "queue": ({"resource": "\n    quue: fifo"}, "resources.desk.quue"),
}


@pytest.mark.parametrize("case", sorted(UNION_CASES))
def test_an_unknown_key_under_a_union_names_the_key_at_its_own_line(tmp_path, case):
    given, key = UNION_CASES[case]
    path = tmp_path / "m.yaml"
    text = UNION_BASE.format(
        **{
            "resource": "",
            "task_resource": "\n    resource: desk",
            "task": "",
            "run": "",
            "stats": "",
            **given,
        }
    )
    path.write_text(text)
    leaf = key.rsplit(".", 1)[1]
    line = next(n for n, row in enumerate(text.splitlines(), 1) if leaf in row)
    with pytest.raises(BlueprintError) as refused:
        SimulationContext.from_yaml(path)
    lines = str(refused.value).splitlines()
    unknown = [row for row in lines if "unknown key" in row]
    assert len(unknown) == 1, lines
    assert unknown[0].startswith(f"{path}:{line}: {key}: unknown key"), unknown[0]
    for row in lines:
        assert "[" not in row.split(": ", 2)[1], row
        for member in ("str:", "bool:", "Drain.", "PreemptRule.", "SegmentEvery"):
            assert member not in row, row
    assert "did you mean" in unknown[0] or case in ("release", "preempt", "drain")


@pytest.mark.parametrize(
    "run",
    [
        "{until: 20, segments: {every: true}}",
        "{until: 20, drain: {limit: .nan}}",
    ],
)
def test_a_wrong_value_under_a_union_reports_only_the_matching_member(tmp_path, run):
    path = tmp_path / "m.yaml"
    path.write_text(
        UNION_BASE.format(
            resource="", task_resource="\n    resource: desk", task="", run="", stats=""
        ).replace("run:\n  until: 20", f"run: {run}")
    )
    with pytest.raises(BlueprintError) as refused:
        SimulationContext.from_yaml(path)
    lines = str(refused.value).splitlines()
    assert len(lines) == 1, lines
    assert "Input should be a valid" not in lines[0], lines[0]

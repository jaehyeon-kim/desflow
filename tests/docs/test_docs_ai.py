"""The Agent Skill pages say what the skill and the commands do today.

Build a Model quotes the checklist of SKILL.md, two messages of `desflow check` and the
Console warning; Port from SimPy copies the table of `references/simpy.md`; Evaluation
counts the tasks of `scripts/skill/evals/`.
"""

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs" / "ai"
SKILL = ROOT / "skills" / "desflow"


def _page(name):
    return (DOCS / name).read_text(encoding="utf-8")


def _desflow(tmp_path, *args):
    ran = subprocess.run(
        [str(Path(sys.executable).parent / "desflow"), *args],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    return ran.returncode, ran.stdout + ran.stderr


def test_the_checklist_is_the_one_in_skill_md():
    page = _page("build-a-model.md")
    skill = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    checklist = re.search(r"```text\n(- \[ \].*?)\n```", skill, re.S).group(1)
    assert len(checklist.splitlines()) == 5
    assert f"```text\n{checklist}\n```" in page
    assert (SKILL / "scripts" / "check_model.py").is_file()


def test_the_check_messages_are_the_ones_desflow_gives(tmp_path):
    page = _page("build-a-model.md")
    (tmp_path / "model.yaml").write_text(
        "simulation: {sim_id: Shop, factor: 0, random_seed: 42}\n"
        "arrivals:\n"
        "  orders: {dist: exponential, rate: 1, spawn: place_order}\n"
        "tasks:\n"
        "  place_order:\n"
        "    payload: {order_id: {gen: sequence}}\n",
        encoding="utf-8",
    )
    code, text = _desflow(tmp_path, "check", "model.yaml")
    assert code == 0 and "ok model.yaml (sim_id Shop)" in text
    warning = next(ln for ln in text.splitlines() if "the run has no end" in ln)
    assert (
        f"```text\n{warning.split('] desflow.blueprint.build: ', 1)[1].replace(str(tmp_path) + '/', '')}\n```"
        in page
    )
    (tmp_path / "seedless.yaml").write_text(
        "simulation: {sim_id: Shop, factor: 0}\n"
        "arrivals:\n"
        "  orders: {dist: exponential, rate: 1, spawn: place_order, max_events: 5}\n"
        "tasks:\n"
        "  place_order:\n"
        "    payload: {order_id: {gen: sequence}}\n"
        "tests:\n"
        "  - name: five_orders\n"
        "    until: 100\n"
        "    expect:\n"
        "      tasks.place_order.finished: 5\n",
        encoding="utf-8",
    )
    code, text = _desflow(tmp_path, "check", "seedless.yaml")
    assert code == 1
    refusal = text.strip().replace("seedless.yaml", "model.yaml")
    assert f"```text\n{refusal}\n```" in page


def test_a_builder_file_is_checked_and_sampled_and_the_console_warns(tmp_path):
    source = (ROOT / "examples/builder/local_example.py").read_text(encoding="utf-8")
    no_logging = re.sub(r"logging\.basicConfig\(.*?\n\)\n", "", source, flags=re.S)
    (tmp_path / "builder.py").write_text(no_logging, encoding="utf-8")
    code, text = _desflow(tmp_path, "check", "builder.py")
    assert code == 0 and "ok builder.py (sim_id Shop)" in text
    code, text = _desflow(tmp_path, "run", "builder.py", "--sample", "2")
    assert code == 0 and text.count('"stream_type":"event"') == 2
    ran = subprocess.run(
        [sys.executable, "builder.py"], cwd=tmp_path, capture_output=True, text=True
    )
    warning = next(ln for ln in ran.stderr.splitlines() if "UserWarning" in ln)
    shown = re.search(
        r"```text\n(nolog\.py:\d+: UserWarning: .*?)\n```", _page("build-a-model.md")
    )
    assert (
        warning.split("UserWarning: ", 1)[1]
        == shown.group(1).split("UserWarning: ", 1)[1]
    )


def test_port_from_simpy_holds_every_row_of_the_skill_table():
    page = _page("from-simpy.md")
    rows = [
        ln
        for ln in (SKILL / "references" / "simpy.md")
        .read_text(encoding="utf-8")
        .splitlines()
        if ln.startswith("|")
    ]
    assert len(rows) == 13
    assert all(row in page for row in rows)


def test_evaluation_counts_what_the_evals_folder_holds():
    page = _page("evaluation.md")
    tasks = [p for p in (ROOT / "scripts/skill/evals/tasks").iterdir() if p.is_dir()]
    assert len(tasks) == 20 and "The evaluation has 20 tasks" in page
    runs = [int(n) for n in re.findall(r"\| (?:Sonnet|Haiku|Opus) \| (\d+) \|", page)]
    assert runs[-3:] == [60, 20, 20] and sum(runs[-3:]) == 100

"""The How-To pages of Analysis and Tools show what `desflow test` and the blueprint
tests print, the lines of the Check Against Theory list hold, and the recommender of
Call Python from YAML ranks the same in every form."""

import re
import shutil
import sys
from pathlib import Path

import pytest
from how_to_support import GUIDES, YAML, copy_into
from how_to_support import load_from as _load_from
from typer.testing import CliRunner

from desflow.cli import app

ROOT = Path(__file__).resolve().parents[2]
PAGE = (ROOT / "docs/how-to/analysis/test-a-model.md").read_text("utf-8")


def _desflow(*words):
    result = CliRunner().invoke(app, ["test", *words], catch_exceptions=False)
    return result.exit_code, result.output


@pytest.mark.parametrize("form", ["yaml", "builder"])
def test_desflow_test_passes_the_tests_of_each_form(form, tmp_path, monkeypatch):
    """The two tests of the page pass in the blueprint and in the builder file, and
    the page shows the lines they print."""
    monkeypatch.chdir(tmp_path)
    if form == "yaml":
        copy_into(tmp_path, YAML / "test_a_model.yaml")
        name = "test_a_model.yaml"
        holder = name
    else:
        copy_into(tmp_path, GUIDES / "test_a_model.py")
        shutil.copy(YAML / "test_a_model_tests.yaml", tmp_path / "tests.yaml")
        name = "test_a_model.py"
        holder = "tests.yaml"
    code, output = _desflow(name)
    assert code == 0
    assert output == f"ok {holder}: two packers\nok {holder}: another seed\n"
    if form == "yaml":
        assert f"```text\n{output}```" in PAGE


def test_a_test_that_differs_prints_the_page_lines_and_fix_repairs_it(
    tmp_path, monkeypatch
):
    """`finished: 300` in place of 319 fails with the lines the page shows, and
    `--fix` writes the measured value."""
    monkeypatch.chdir(tmp_path)
    blueprint = tmp_path / "test_a_model.yaml"
    blueprint.write_text(
        (YAML / "test_a_model.yaml")
        .read_text("utf-8")
        .replace("finished: 319", "finished: 300")
    )
    code, output = _desflow("test_a_model.yaml")
    assert code == 1
    assert f"```text\n{output}```" in PAGE
    code, output = _desflow("--fix", "test_a_model.yaml")
    assert code == 0 and output.startswith("fixed test_a_model.yaml: two packers")
    assert "finished: 319" in blueprint.read_text("utf-8")
    assert _desflow("test_a_model.yaml")[0] == 0


THEORY = (ROOT / "docs/how-to/analysis/check-against-theory.md").read_text("utf-8")


def _explain(tmp_path, text):
    from desflow import SimulationContext

    path = tmp_path / "model.yaml"
    path.write_text(text)
    return SimulationContext.from_yaml(path).analyze()


def test_the_formulas_leave_out_a_task_an_interrupt_names(tmp_path):
    """Check Against Theory lists a task that an `interrupt` names, and a task another
    task starts, among the tasks the formulas leave out; the analysis does the same."""
    explanation = _explain(
        tmp_path,
        "simulation: {sim_id: S, factor: 0}\n"
        "resources: {m: {current_cap: 1}}\n"
        "signals: {stop: {on: [{interrupt: {task: work}}]}}\n"
        "arrivals:\n"
        "  a: {dist: exponential, rate: 0.5, spawn: work}\n"
        "  b: {dist: exponential, rate: 0.1, spawn: boss}\n"
        "tasks:\n"
        "  work: {resource: m, service_time: 1, payload: {}, on_interrupt: abandon,"
        " on: [{spawn: {task: side}}]}\n"
        "  boss: {payload: {}, signal: stop}\n"
        "  side: {payload: {}}\n",
    )
    by_name = {task.name: task for task in explanation.tasks}
    assert by_name["work"].rate is None
    assert "interruptions (an interrupt names it)" in by_name["work"].reason
    assert by_name["side"].rate is None and "work" in by_name["side"].reason
    assert "a task that an `interrupt` names (in the `interrupt` of a task" in THEORY
    assert "a task started by a `spawn` entry of a task's `on:`" in THEORY


def test_the_list_of_what_the_formulas_leave_out_names_each_feature_with_its_effect():
    """Each feature that explain/features.py reports is on the page, in the item whose
    effect is the one `explain` has: a task feature marks the task and every task after
    it, an arrival feature the arrival and every task after it, a resource feature the
    tasks that use the resource and every task after them."""
    from desflow.explain import features

    start = THEORY.index("takes the formulas out of use")
    block = THEORY[start:].split("\n\n")[1]
    items = block.splitlines()
    assert all(item.startswith("- ") for item in items)
    effects = {
        features.INTERRUPTED: ("task that an `interrupt` names", "that task and every"),
        features.SPAWN_COUNT: ("spawn `count` other than 1", "that task and every"),
        features.PICKED_RESOURCE: ("`key` or `lowest`", "that task and every"),
        features.GROUPED_ARRIVALS: ("`group` other than 1", "that arrival and every"),
        features.INACTIVE_ARRIVAL: ("`active: false`", "that arrival and every"),
        features.RESOURCE_FAMILY: (
            "resource family",
            "the tasks that use the resource",
        ),
        features.CHANGED_CAPACITY: (
            "`on:` entry that sets a resource's capacity",
            "the tasks that use the resource",
        ),
    }
    for feature, (words, effect) in effects.items():
        found = [item for item in items if words in item]
        assert len(found) == 1, f"{feature}: {len(found)} items name {words!r}"
        assert found[0].split(": ")[-1].startswith(effect), feature
    assert all(item.endswith(";") for item in items[:-1]) and items[-1].endswith(".")


def test_a_grouped_or_inactive_arrival_takes_its_tasks_out_of_the_formulas(tmp_path):
    """An arrival with a `group` other than 1, or `active: false`, has no rate, and
    neither has the task after it."""
    for key in ("group: 2", "active: false"):
        explanation = _explain(
            tmp_path,
            "simulation: {sim_id: S, factor: 0}\n"
            "resources: {m: {current_cap: 1}}\n"
            "services: {s: {dist: exponential, rate: 1}}\n"
            f"arrivals: {{a: {{dist: exponential, rate: 0.5, spawn: work, {key}}}}}\n"
            "tasks: {work: {resource: m, service: s, payload: {}}}\n",
        )
        assert explanation.arrivals[0].rate is None, key
        assert explanation.tasks[0].rate is None, key


# --- Call Python from YAML ---------------------------------------------------------


class _Valkey(dict):
    """A stand-in for the Valkey client: the two calls the page makes."""

    def mget(self, keys):
        return [self.get(key) for key in keys]

    def set(self, key, value):
        self[key] = value


def _visit_lines(form, tmp_path, monkeypatch, capsys):
    """Runs one form of the recommender shop against the stand-in and returns the JSON
    records it prints, without their clock timestamps."""
    import json

    from desflow import SimulationContext

    tmp_path.mkdir()
    copy_into(
        tmp_path,
        GUIDES / "call_python_ranking.py",
        GUIDES / "call_python_seed.py",
        GUIDES / "call_python.py",
        GUIDES / "call_python_low_level.py",
        YAML / "call_python.yaml",
    )
    monkeypatch.chdir(tmp_path)
    for name in ("call_python_ranking", "call_python_seed", "call_python"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    ranking = _load_from(tmp_path, "call_python_ranking")
    monkeypatch.setitem(sys.modules, "call_python_ranking", ranking)
    seed = _load_from(tmp_path, "call_python_seed")
    client = _Valkey()
    seed.seed(client)
    monkeypatch.setattr(ranking, "CLIENT", client)
    if form == "yaml":
        SimulationContext.from_yaml(tmp_path / "call_python.yaml").run()
    elif form == "builder":
        _load_from(tmp_path, "call_python").build_shop().run(until=1)
    else:
        low_level = _load_from(tmp_path, "call_python_low_level")
        env = low_level.shop()
        env.run(until=1)
        env.teardown()
    records = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("{")
    ]
    for record in records:
        record.pop("timestamp", None)
    return records


def test_the_recommender_forms_rank_alike_and_match_a_reference_score(
    tmp_path, monkeypatch, capsys
):
    """Ten visits in each form, with the same features and the same three products,
    and the three products are the ones a separate computation of the LinUCB score
    ranks first."""
    import json

    import numpy as np

    runs = {
        form: _visit_lines(form, tmp_path / form, monkeypatch, capsys)
        for form in ("yaml", "builder", "low_level")
    }
    for form in runs:
        assert len(runs[form]) == 10
    assert runs["yaml"] == runs["builder"] == runs["low_level"]
    seed_models = _load_from(tmp_path / "yaml", "call_python_seed").MODELS
    identity = {"A_inv": np.eye(3), "b": np.zeros(3)}
    for record in runs["yaml"]:
        x = np.array(record["value"]["features"])
        scores = {}
        for pid in ("p1", "p2", "p3", "p4", "p5"):
            model = seed_models.get(pid, identity)
            a_inv, b = np.array(model["A_inv"]), np.array(model["b"])
            scores[pid] = x @ a_inv @ b + 1.0 * np.sqrt(x @ a_inv @ x)
        best = sorted(scores, key=scores.get, reverse=True)[:3]
        assert record["value"]["shown"] == best
    page = (ROOT / "docs/how-to/tools/yaml-advanced.md").read_text("utf-8")
    shown = [
        re.sub(r'"timestamp":"[^"]*",', "", line)
        for line in re.search(r"```text\n(\{.*?)```", page, re.S)[1].splitlines()
    ]
    assert [json.loads(line) for line in shown] == runs["yaml"][:3]


# --- Test a Pipeline ---------------------------------------------------------------


@pytest.mark.parametrize("form", ["yaml", "builder", "low_level"])
def test_the_pipeline_tests_pass_in_each_form(form, tmp_path):
    """The saved files of a form run under pytest, 4 tests pass, and every form flags the
    windows at 330, 340 and 350 seconds that the page names."""
    import subprocess

    copy_into(
        tmp_path,
        GUIDES / "pipeline_under_test.py",
        GUIDES / "test_a_pipeline.py",
        YAML / "test_a_pipeline.yaml",
    )
    copy_into(
        tmp_path,
        GUIDES / "pipeline_shop_yaml.py",
        GUIDES / "pipeline_shop_builder.py",
        GUIDES / "pipeline_shop_low_level.py",
    )
    shutil.copy(GUIDES / f"pipeline_shop_{form}.py", tmp_path / "pipeline_shop.py")
    ran = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "test_a_pipeline.py",
            "-p",
            "no:cacheprovider",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert " 4 passed in " in ran.stdout, ran.stdout
    shops = {
        "yaml": _load_from(tmp_path, "pipeline_shop_yaml"),
        "builder": _load_from(tmp_path, "pipeline_shop_builder"),
        "low_level": _load_from(tmp_path, "pipeline_shop_low_level"),
    }
    module = shops[form]
    pipeline = _load_from(tmp_path, "pipeline_under_test")
    module.run_shop(tmp_path / "again")
    events = pipeline.read_lines(f"{tmp_path}/again/data")
    labels = pipeline.read_lines(f"{tmp_path}/again/labels")
    flagged = pipeline.find_surges(events)
    assert flagged == [330, 340, 350]
    assert pipeline.score(flagged, labels) == {"caught": 1, "missed": 0, "false": 0}
    # The page states the windows the run flags and the score it gives
    page = (ROOT / "docs/how-to/analysis/test-a-pipeline.md").read_text("utf-8")
    windows = ", ".join(map(str, flagged[:-1])) + f" and {flagged[-1]}"
    assert f"flags the windows that start at {windows} seconds" in page
    assert "The score is one fault caught, none missed and no false alarm." in page
    assert f"starts at {flagged[0]} seconds, {flagged[0] - 300} seconds after" in page


def test_run_before_runs_its_function_when_the_run_starts(tmp_path, monkeypatch):
    """The Run Length page's `run.before` calls the function once, when the run starts
    and not when the file is loaded."""
    from desflow import SimulationContext

    copy_into(tmp_path, YAML / "run_before.yaml", YAML / "run_before_logic.py")
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "run_before_logic", raising=False)
    app = SimulationContext.from_yaml(tmp_path / "run_before.yaml")
    assert not (tmp_path / "events").exists()
    app.run(until=10)
    assert (tmp_path / "events").is_dir()
    page = (ROOT / "docs/user-guide/time-and-pacing/run-length.md").read_text("utf-8")
    assert "run_before.yaml:before" in page and "once, at the start of `run()`" in page


def test_the_editor_modeline_is_one_line_that_the_yaml_extension_reads():
    """yaml-language-server reads a modeline from one line and takes the schema from
    it: `#`, white space, `yaml-language-server:` and `$schema=<url>`. Set Up Your Editor
    shows that line, and the blueprint snippet holds it."""
    text = (YAML / "editor_setup.yaml").read_text("utf-8")
    section = text.split("# --8<-- [start:modeline]\n")[1].split("# --8<-- [end:")[0]
    lines = section.splitlines()
    assert len(lines) == 1
    assert re.match(r"^#\s+(?:yaml-language-server\s*:|\$schema:)", lines[0])
    schema = re.search(r"\$schema(?:=|:[^\S\r\n]*)(\S+)", lines[0])
    assert schema and schema.group(1).endswith("/schema/blueprint.schema.json")
    page = (ROOT / "docs/how-to/tools/set-up-your-editor.md").read_text("utf-8")
    assert page.count(lines[0]) == 2

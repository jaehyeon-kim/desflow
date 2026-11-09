"""`desflow.blueprint.check_model` reads a model file as `desflow check` does and as the
skill's `check_model.py` needs it."""

from pathlib import Path

from desflow.blueprint import ModelCheck, check_model

ROOT = Path(__file__).resolve().parents[2]

BAD = "simulation: {sim_id: Shop, ranom_seed: 7}\n"


def test_an_accepted_blueprint_gives_its_run_section_and_start_time(tmp_path):
    file = tmp_path / "start.yaml"
    file.write_text(
        (ROOT / "examples/yaml/local.yaml")
        .read_text(encoding="utf-8")
        .replace(
            "simulation: {sim_id: Shop,",
            "simulation: {logical_start_time: 2026-01-01T09:00:00, sim_id: Shop,",
        ),
        encoding="utf-8",
    )
    checked = check_model(file)
    assert isinstance(checked, ModelCheck)
    assert checked.problem is None
    assert checked.context is not None and checked.context.random_seed == 42
    assert checked.run is not None and checked.run.until is not None
    # YAML reads the unquoted time as a datetime; the skill compares it as text
    assert checked.start == "2026-01-01 09:00:00"


def test_a_refused_blueprint_gives_the_problem_and_no_run_section(tmp_path):
    file = tmp_path / "bad.yaml"
    file.write_text(BAD, encoding="utf-8")
    checked = check_model(file)
    assert checked.context is None and checked.run is None
    assert checked.problem and str(file) in checked.problem


def test_a_python_file_gives_no_run_section_and_a_file_without_a_context_says_so(
    tmp_path,
):
    low_level = tmp_path / "low.py"
    low_level.write_text("x = 1\n", encoding="utf-8")
    checked = check_model(low_level)
    assert checked.no_context and checked.context is None and checked.run is None
    builder = ROOT / "examples/builder/local_example.py"
    built = check_model(builder)
    assert built.context is not None and built.run is None and built.start is None


def test_variables_reach_the_check_the_run_section_and_the_start_time(tmp_path):
    file = tmp_path / "variables.yaml"
    file.write_text(
        (ROOT / "examples/yaml/local.yaml")
        .read_text(encoding="utf-8")
        .replace(
            "simulation: {sim_id: Shop, factor: 1.0, random_seed: 42}",
            "simulation:\n  sim_id: Shop\n  random_seed: ${SEED}\n"
            "  logical_start_time: '${START}'",
        )
        .replace("run: {until: 30}", "run:\n  until: ${UNTIL}"),
        encoding="utf-8",
    )
    checked = check_model(
        file, {"SEED": "9", "START": "2026-02-03T04:05:06", "UNTIL": "45"}
    )
    assert checked.problem is None
    assert checked.context is not None and checked.context.random_seed == 9
    assert checked.run is not None and checked.run.until == 45
    assert checked.start == "2026-02-03T04:05:06"

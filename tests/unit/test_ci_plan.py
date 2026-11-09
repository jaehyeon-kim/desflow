"""scripts/ci_plan.py picks the jobs of a run of checks.yml.

A pull request tests Python 3.11 and 3.15, a push to main every Python from 3.11 to
3.15, and the schedule or a run by hand everything. A pull request or a push skips the
code jobs when it changes only docs, and runs the docs and service jobs only when it
changes what they cover. Each job's parts cover every part number once, so no test is
left out.
"""

import importlib.util
import json
from collections import defaultdict
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "ci_plan.py"
_spec = importlib.util.spec_from_file_location("ci_plan", SCRIPT)
assert _spec is not None and _spec.loader is not None
ci_plan = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ci_plan)

SRC = ["src/desflow/core/task.py"]


def _plan(event, changed):
    return {k: json.loads(v) for k, v in ci_plan.plan(event, changed).items()}


def _versions(plan, os="ubuntu-latest"):
    return {leg["python"] for leg in plan["unit"]["include"] if leg["os"] == os}


def test_a_pull_request_tests_the_lowest_and_highest_python():
    plan = _plan("pull_request", SRC)
    assert plan["code"] and not plan["services"]
    assert {leg["os"] for leg in plan["unit"]["include"]} == {"ubuntu-latest"}
    assert _versions(plan) == {"3.11", "3.15"}
    assert plan["simpy"] == ["lowest"]


def test_a_push_to_main_tests_every_python_on_ubuntu():
    plan = _plan("push", SRC)
    assert plan["code"] and not plan["services"]
    assert {leg["os"] for leg in plan["unit"]["include"]} == {"ubuntu-latest"}
    assert _versions(plan) == {"3.11", "3.12", "3.13", "3.14", "3.15"}
    assert plan["simpy"] == ["lowest"]


def test_the_short_sets_leave_out_the_every_model_tests_and_known_cases():
    for event in ("pull_request", "push"):
        for leg in _plan(event, SRC)["unit"]["include"]:
            assert "tests/known_cases" not in leg["paths"]
            assert "not every_model" in leg["marks"]


def test_the_schedule_and_a_run_by_hand_run_everything():
    for event in ("schedule", "workflow_dispatch"):
        plan = _plan(event, [])
        assert plan["code"] and plan["docs"] and plan["services"]
        assert plan["simpy"] == ["lowest", "newest"]
        legs = plan["unit"]["include"]
        assert {leg["os"] for leg in legs} == {
            "ubuntu-latest",
            "windows-latest",
            "macos-latest",
        }
        for os in ("ubuntu-latest", "windows-latest", "macos-latest"):
            assert any(
                leg["os"] == os
                and "tests/known_cases" in leg["paths"]
                and "every_model" not in leg["marks"]
                for leg in legs
            ), os


def test_a_docs_only_change_skips_the_code_jobs():
    for event in ("pull_request", "push"):
        plan = _plan(event, ["docs/index.md", "mkdocs.yml", "CONTRIBUTING.md"])
        assert not plan["code"] and plan["docs"] and not plan["services"]
    assert _plan("pull_request", ["docs/index.md", "src/desflow/cli/run.py"])["code"]


def test_the_docs_tests_run_only_when_what_they_read_changes():
    assert _plan("pull_request", SRC)["docs"]
    assert not _plan("pull_request", ["tests/unit/test_values.py"])["docs"]
    assert not _plan("push", ["tests/unit/test_values.py"])["docs"]


def test_a_connector_change_runs_the_service_jobs():
    for event in ("pull_request", "push"):
        for path in (
            "src/desflow/connectors/egress/kafka.py",
            "uv.lock",
            "examples/yaml/kafka.yaml",
        ):
            assert _plan(event, [path])["services"], (event, path)


def test_unknown_changes_run_every_job():
    for event in ("pull_request", "push"):
        plan = _plan(event, [])
        assert plan["code"] and plan["docs"] and plan["services"]


def test_every_job_runs_each_of_its_parts_once():
    for event in ("pull_request", "push", "schedule"):
        parts = defaultdict(list)
        for leg in _plan(event, SRC)["unit"]["include"]:
            parts[(leg["os"], leg["python"], leg["paths"], leg["marks"])].append(
                leg["shard"]
            )
        for key, got in parts.items():
            count = len(got)
            assert sorted(got) == sorted(f"{n}/{count}" for n in range(1, count + 1)), (
                key
            )


def test_every_extra_has_an_install_check():
    import tomllib

    pyproject = tomllib.loads((SCRIPT.parents[1] / "pyproject.toml").read_text())
    extras = set(pyproject["project"]["optional-dependencies"])
    checks = {
        p.stem.removeprefix("check_")
        for p in (SCRIPT.parent / "install_checks").glob("check_*.py")
    }
    assert checks == extras | {"core"}

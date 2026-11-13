"""scripts/ci_plan.py picks the test jobs of the workflows.

A pull request tests Python 3.11, a push to main Python 3.12 to 3.15, and manual.yml
every test on Python 3.11, 3.12 and 3.15 (or the versions it is given) on Ubuntu and
on Python 3.12 on Windows. A pull request or a push skips the code jobs when it
changes only files no code job reads. A pull request runs the docs and service jobs
only when it changes what they cover, and a push to main runs neither. Each job's
parts cover every part number once, so no test is left out.
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

SRC = ["src/desflow/builder/task.py"]


def _plan(event, changed):
    return {k: json.loads(v) for k, v in ci_plan.plan(event, changed).items()}


def _versions(plan, os="ubuntu-latest"):
    return {leg["python"] for leg in plan["unit"]["include"] if leg["os"] == os}


def test_a_pull_request_tests_the_lowest_python():
    plan = _plan("pull_request", SRC)
    assert plan["code"] and not plan["services"]
    assert {leg["os"] for leg in plan["unit"]["include"]} == {"ubuntu-latest"}
    assert _versions(plan) == {"3.11"}


def test_a_push_to_main_tests_the_versions_the_pull_request_did_not():
    plan = _plan("push", SRC)
    assert plan["code"] and not plan["docs"] and not plan["services"]
    assert {leg["os"] for leg in plan["unit"]["include"]} == {"ubuntu-latest"}
    assert _versions(plan) == {"3.12", "3.13", "3.14", "3.15"}


def test_the_short_sets_leave_out_the_every_model_tests_and_known_cases():
    for event in ("pull_request", "push"):
        for leg in _plan(event, SRC)["unit"]["include"]:
            assert "tests/known_cases" not in leg["paths"]
            assert "not every_model" in leg["marks"]


def test_the_manual_set_runs_every_test_on_the_lowest_and_newest_python():
    plan = _plan("manual", [])
    assert _versions(plan) == {"3.11", "3.12", "3.15"}
    assert _versions(plan, "windows-latest") == {"3.12"}
    for leg in plan["unit"]["include"]:
        assert "tests/known_cases" in leg["paths"]
        assert "every_model" not in leg["marks"]


def test_a_manual_run_by_hand_tests_the_versions_it_is_given():
    plan = {
        k: json.loads(v)
        for k, v in ci_plan.plan("workflow_dispatch", [], ["3.13"]).items()
    }
    assert _versions(plan) == {"3.13"}
    assert _versions(plan, "windows-latest") == {"3.12"}


def test_a_docs_only_change_skips_the_code_jobs():
    for event in ("pull_request", "push"):
        plan = _plan(
            event, ["docs/css/code-lines.css", "mkdocs.yml", "CONTRIBUTING.md"]
        )
        assert not plan["code"] and not plan["services"]
        assert plan["docs"] == (event == "pull_request")
    assert _plan("pull_request", ["CONTRIBUTING.md", "src/desflow/cli/run.py"])["code"]


def test_a_page_or_snippet_change_runs_the_code_jobs_that_read_it():
    # The unit tests check each page's imports and run the snippet blueprints, and
    # the regression captures the blueprints under docs/
    for path in (
        "docs/index.md",
        "docs/snippets/yaml/dynamic_topology.yaml",
        "docs/snippets/yaml/simpy_forms.py",
    ):
        for event in ("pull_request", "push"):
            assert _plan(event, [path])["code"], (event, path)


def test_the_docs_tests_run_when_a_test_file_they_read_changes():
    for path in (
        "tests/conformance/test_yaml_only.py",
        "tests/conformance/register.yaml",
        "tests/known_cases/bt_aq1/original.py",
        "tests/regression/left_out.yaml",
    ):
        assert _plan("pull_request", [path])["docs"], path


def test_the_docs_tests_run_only_when_what_they_read_changes():
    assert _plan("pull_request", SRC)["docs"]
    assert not _plan("pull_request", ["tests/unit/test_values.py"])["docs"]
    assert not _plan("push", ["tests/unit/test_values.py"])["docs"]


def test_a_connector_change_runs_the_service_jobs_on_a_pull_request():
    for path in (
        "src/desflow/connectors/egress/kafka.py",
        "uv.lock",
        "examples/yaml/kafka.yaml",
        # The modules that build, start and stop the connectors, and what the
        # service examples read
        "src/desflow/blueprint/build.py",
        "src/desflow/builder/context.py",
        "src/desflow/cli/run.py",
        "src/desflow/core/environment.py",
        "tests/conftest.py",
        "docs/gallery/simulation-models/kafka.md",
    ):
        assert _plan("pull_request", [path])["services"], path
        assert not _plan("push", [path])["services"], path


def test_unknown_changes_run_every_job_of_the_set():
    plan = _plan("pull_request", [])
    assert plan["code"] and plan["docs"] and plan["services"]
    plan = _plan("push", [])
    assert plan["code"] and not plan["docs"] and not plan["services"]


def test_a_run_has_at_most_20_jobs_at_once():
    """The plan's limit. A pull request runs Lint, the install checks, its unit parts,
    3 regression parts, 3 service jobs and 5 docs parts together; a push to main its
    unit parts and the strict docs build."""
    assert 1 + 1 + len(_plan("pull_request", SRC)["unit"]["include"]) + 3 + 3 + 5 <= 20
    assert len(_plan("push", SRC)["unit"]["include"]) + 1 <= 20


def test_every_job_runs_each_of_its_parts_once():
    for event in ("pull_request", "push", "manual"):
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


def test_the_all_extra_names_every_other_extra():
    import tomllib

    extras = tomllib.loads((SCRIPT.parents[1] / "pyproject.toml").read_text())[
        "project"
    ]["optional-dependencies"]
    named = extras["all"][0].removeprefix("desflow[").removesuffix("]").split(",")
    assert sorted(named) == sorted(set(extras) - {"all"})

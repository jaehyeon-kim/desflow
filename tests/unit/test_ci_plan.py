"""scripts/ci_plan.py picks the jobs of a pipeline run.

A push runs every test leg; a pull request runs the short set, skips the code jobs
when it changes only docs, and runs the service jobs only when it changes what they
cover. Each leg's shares cover every share number once, so no test is left out.
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


def _plan(event, changed):
    return {k: json.loads(v) for k, v in ci_plan.plan(event, changed).items()}


def test_a_push_runs_everything():
    plan = _plan("push", [])
    assert plan["full"] and plan["code"] and plan["services"]
    legs = plan["unit"]["include"]
    assert {leg["python"] for leg in legs} == {"3.11", "3.12", "3.13", "3.14", "3.15"}
    assert {leg["os"] for leg in legs} == {"ubuntu-latest", "windows-latest"}
    assert plan["simpy"] == ["lowest", "newest"]
    assert len(plan["paced"]["include"]) == 2


def test_a_pull_request_runs_the_short_set():
    plan = _plan("pull_request", ["src/desflow/core/task.py"])
    assert not plan["full"] and plan["code"] and not plan["services"]
    legs = plan["unit"]["include"]
    assert {leg["os"] for leg in legs} == {"ubuntu-latest"}
    assert {leg["python"] for leg in legs} == {"3.11", "3.12", "3.15"}
    assert plan["simpy"] == ["lowest"]


def test_a_docs_only_pull_request_skips_the_code_jobs():
    plan = _plan("pull_request", ["docs/index.md", "mkdocs.yml", "CONTRIBUTING.md"])
    assert not plan["code"] and not plan["services"]
    assert _plan("pull_request", ["docs/index.md", "src/desflow/cli/run.py"])["code"]
    assert _plan("pull_request", [])["code"]


def test_a_connector_change_runs_the_service_jobs():
    for path in (
        "src/desflow/connectors/egress/kafka.py",
        "uv.lock",
        "examples/yaml/kafka.yaml",
    ):
        assert _plan("pull_request", [path])["services"], path


def test_every_leg_runs_each_of_its_shares_once():
    for event in ("push", "pull_request"):
        shares = defaultdict(list)
        for leg in _plan(event, ["src/x.py"])["unit"]["include"]:
            key = (leg["os"], leg["python"], leg["paths"], leg["marks"])
            shares[key].append(leg["shard"])
        for key, got in shares.items():
            if got == [""]:
                continue
            count = len(got)
            assert sorted(got) == sorted(f"{n}/{count}" for n in range(1, count + 1)), (
                key
            )


def test_the_every_model_tests_and_known_cases_run_on_each_system():
    legs = _plan("push", [])["unit"]["include"]
    for os in ("ubuntu-latest", "windows-latest"):
        assert any(
            leg["os"] == os
            and "tests/known_cases" in leg["paths"]
            and "every_model" not in leg["marks"]
            for leg in legs
        ), os


def test_every_extra_has_an_install_check():
    import tomllib

    pyproject = tomllib.loads((SCRIPT.parents[1] / "pyproject.toml").read_text())
    extras = set(pyproject["project"]["optional-dependencies"])
    checks = {
        p.stem.removeprefix("check_")
        for p in (SCRIPT.parent / "install_checks").glob("check_*.py")
    }
    assert checks == extras | {"core"}

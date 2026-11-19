"""The Integrations pages run the shop from Airflow, Dagster, Prefect and Kestra.

Each orchestrator's file in docs/snippets/orchestrators/ runs with the commands its
page shows, in a folder of its own, under `uv run --isolated --no-dev --group <name>`,
so the orchestrator is installed only for its test and the run uses this checkout's
desflow. Each prints the figures it read from result.json, which must be the figures
of the same command run directly and the output block on its page. Kestra needs a
server, so its flow is read: the task type, the blueprint it writes and the command,
which runs here.
"""

import ast
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
PAGES = ROOT / "docs" / "integrations"
SNIPPETS = ROOT / "docs" / "snippets" / "orchestrators"
DESFLOW = shutil.which("desflow", path=str(Path(sys.executable).parent)) or "desflow"
COMMAND = (
    "desflow run shop.yaml --var PACKERS=3 --until 3600 --seed 7 --summary result.json"
)
FIGURES = (
    "tasks.pack.finished",
    "tasks.pack.wait_mean",
    "resources.packers.utilization_avg",
)


def _figures(folder: Path) -> dict:
    summary = json.loads((folder / "result.json").read_text(encoding="utf-8"))
    return {
        row["statistic"]: row["value"]
        for row in summary["statistics"]
        if row["statistic"] in FIGURES
    }


@pytest.fixture(scope="module")
def direct(tmp_path_factory) -> dict:
    """The figures of the command run straight from a shell."""
    folder = tmp_path_factory.mktemp("direct")
    shutil.copy(SNIPPETS / "shop.yaml", folder)
    done = subprocess.run(
        [DESFLOW, *COMMAND.split()[1:]],
        cwd=folder,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return _figures(folder)


def _uv(group: str, args, cwd: Path, env=None) -> str:
    """Runs `args` in an environment with this checkout and the group, as a reader
    with the orchestrator installed runs them."""
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is not on PATH")
    environment = {**os.environ, **(env or {})}
    # The docs environment's own venv must not stand in for the isolated one
    environment.pop("VIRTUAL_ENV", None)
    done = subprocess.run(
        [uv, "run", "--project", str(ROOT), "--isolated", "--no-dev", "--group", group]
        + args,
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=900,
    )
    assert done.returncode == 0, done.stdout[-3000:] + done.stderr[-3000:]
    return done.stdout


def _printed(stdout: str) -> dict:
    """The figures the script prints as its last dictionary line."""
    lines = [line for line in stdout.splitlines() if line.startswith("{'")]
    assert lines, stdout[-2000:]
    return ast.literal_eval(lines[-1])


def _shown(page: str) -> dict:
    """The figures in the output block of a page's Check the Result section."""
    text = (PAGES / page).read_text(encoding="utf-8")
    section = text.split("## Check the Result")[1]
    found = re.search(r"```text\n(\{.*?\})\n```", section, re.S)
    assert found, f"{page} shows no figures"
    return ast.literal_eval(found[1])


def _folder(tmp_path: Path, script: str) -> Path:
    for name in (script, "shop.yaml"):
        shutil.copy(SNIPPETS / name, tmp_path)
    return tmp_path


def test_airflow_runs_the_dag_once_and_returns_the_figures(tmp_path, direct):
    """`airflow db migrate`, then `python airflow_dag.py` from the DAGs folder, which
    is `$AIRFLOW_HOME/dags` by default."""
    home = tmp_path / "airflow"
    dags = home / "dags"
    dags.mkdir(parents=True)
    _folder(dags, "airflow_dag.py")
    env = {"AIRFLOW_HOME": str(home)}
    _uv("airflow", ["airflow", "db", "migrate"], dags, env)
    printed = _printed(_uv("airflow", ["python", "airflow_dag.py"], dags, env))
    assert printed == direct == _shown("airflow.md")


def test_dagster_materializes_the_asset_with_the_figures(tmp_path, direct):
    folder = _folder(tmp_path, "dagster_asset.py")
    printed = _printed(_uv("dagster", ["python", "dagster_asset.py"], folder))
    assert printed == direct == _shown("dagster.md")


def test_prefect_runs_the_flow_and_returns_the_figures(tmp_path, direct):
    """With no server set, Prefect starts a temporary one; PREFECT_HOME keeps its
    database in the test's folder."""
    folder = _folder(tmp_path, "prefect_flow.py")
    env = {"PREFECT_HOME": str(tmp_path / "prefect")}
    printed = _printed(_uv("prefect", ["python", "prefect_flow.py"], folder, env))
    assert printed == direct == _shown("prefect.md")


def test_the_kestra_flow_runs_the_shop_with_the_same_command(tmp_path, direct):
    flow = yaml.safe_load((SNIPPETS / "kestra_flow.yaml").read_text(encoding="utf-8"))
    run_shop = flow["tasks"][0]
    assert run_shop["type"] == "io.kestra.plugin.scripts.python.Commands"
    assert run_shop["outputFiles"] == ["result.json"]
    assert yaml.safe_load(run_shop["inputFiles"]["shop.yaml"]) == yaml.safe_load(
        (SNIPPETS / "shop.yaml").read_text(encoding="utf-8")
    )
    [command] = run_shop["commands"]
    assert command == COMMAND
    (tmp_path / "shop.yaml").write_text(run_shop["inputFiles"]["shop.yaml"], "utf-8")
    done = subprocess.run(
        [DESFLOW, *command.split()[1:]],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert _figures(tmp_path) == direct

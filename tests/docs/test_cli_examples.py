"""`desflow examples` lists every example and downloads it at the installed release.

Downloads are served from the repository's own examples/ folder, so the tests need
no network and check that what a reader gets is what the repository holds.
"""

import urllib.error
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from desflow.blueprint import examples
from desflow.blueprint.cli import app

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES_DIR = ROOT / "examples"
runner = CliRunner()


def _local_fetch(url):
    """Serves a raw.githubusercontent.com URL from the local examples/ folder."""
    path = url.split("/examples/", 1)[1]
    return (EXAMPLES_DIR / path).read_bytes()


def test_every_example_file_is_listed():
    """A new example file that the command cannot download, or a listed file that is
    missing, fails here."""
    listed = {
        p
        for e in examples.EXAMPLES.values()
        for p in [x for paths in e.files.values() for x in paths] + e.shared
    }
    on_disk = {
        str(p.relative_to(EXAMPLES_DIR))
        for p in EXAMPLES_DIR.rglob("*")
        if p.suffix in (".py", ".yaml", ".csv") and "__pycache__" not in p.parts
    }
    assert on_disk == listed


@pytest.mark.parametrize(
    "version,ref",
    [("0.17.0", "v0.17.0"), ("1.2.3", "v1.2.3"), ("0.0.0-dev", "main"), ("", "main")],
)
def test_release_ref(version, ref):
    assert examples.release_ref(version) == ref


def test_list_names_every_example():
    result = runner.invoke(app, ["examples"])
    assert result.exit_code == 0
    for name in examples.EXAMPLES:
        assert name in result.output


def test_get_writes_each_version_in_its_folder(tmp_path):
    with mock.patch.object(examples, "_fetch", side_effect=_local_fetch):
        result = runner.invoke(
            app, ["examples", "get", "related-records", "--dest", str(tmp_path)]
        )
    assert result.exit_code == 0, result.output
    yaml_dir = tmp_path / "related-records" / "yaml"
    assert (yaml_dir / "postgres_orders.yaml").read_bytes() == (
        EXAMPLES_DIR / "yaml/advanced/postgres_orders.yaml"
    ).read_bytes()
    assert (yaml_dir / "postgres_orders_logic.py").is_file()
    assert (tmp_path / "related-records/builder/postgres_orders_example.py").is_file()
    assert (tmp_path / "related-records/low-level/postgres_orders_example.py").is_file()
    assert "odctl up postgres" in result.output


def test_get_one_version_and_shared_files(tmp_path):
    with mock.patch.object(examples, "_fetch", side_effect=_local_fetch):
        result = runner.invoke(
            app,
            ["examples", "get", "kafka", "--api", "low-level", "--dest", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    written = sorted(
        str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*") if p.is_file()
    )
    assert written == ["kafka/kafka_dashboard.py", "kafka/low-level/kafka_example.py"]


def test_get_refuses_to_overwrite_without_force(tmp_path):
    with mock.patch.object(examples, "_fetch", side_effect=_local_fetch):
        runner.invoke(app, ["examples", "get", "local", "--dest", str(tmp_path)])
        again = runner.invoke(
            app, ["examples", "get", "local", "--dest", str(tmp_path)]
        )
        forced = runner.invoke(
            app, ["examples", "get", "local", "--dest", str(tmp_path), "--force"]
        )
    assert again.exit_code == 1 and "--force" in again.output
    assert forced.exit_code == 0


def test_a_missing_file_writes_nothing(tmp_path):
    error = urllib.error.HTTPError("u", 404, "Not Found", None, None)
    get = ["examples", "get", "local", "--dest", str(tmp_path)]
    with mock.patch.object(examples, "_fetch", side_effect=error):
        result = runner.invoke(app, get)
        on_main = runner.invoke(app, [*get, "--ref", "main"])
    assert result.exit_code == 1 and "--ref main" in result.output
    # main has nothing more to offer, so a failure there does not suggest it
    assert on_main.exit_code == 1 and "--ref main" not in on_main.output
    assert not any(tmp_path.rglob("*.py"))


def test_unknown_example_is_refused():
    result = runner.invoke(app, ["examples", "get", "nope"])
    assert result.exit_code != 0

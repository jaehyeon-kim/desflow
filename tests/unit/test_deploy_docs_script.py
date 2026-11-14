"""`scripts/deploy_docs.sh` deploys only docs that passed the strict build."""

import json
import os
import subprocess
import textwrap
from pathlib import Path
from typing import List

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "deploy_docs.sh"

pytestmark = pytest.mark.skipif(
    os.name == "nt", reason="The stub PATH needs a POSIX Bash environment."
)


def _deploy(tmp_path: Path, head: str, main: str, deployed: str = "[]") -> List[str]:
    """Runs the script with stand-ins for git, uv, sudo and cp, and returns the `uv`
    commands it ran, in order."""
    stubs = tmp_path / "stubs"
    stubs.mkdir()
    log = tmp_path / "uv.log"
    (stubs / "git").write_text(
        textwrap.dedent(
            """\
            #!/bin/sh
            case "$*" in
              "rev-parse FETCH_HEAD") echo "$MAIN" ;;
              "rev-parse HEAD") echo "$HEAD_COMMIT" ;;
              "rev-parse --short"*) echo short ;;
            esac
            """
        )
    )
    # `uv run mike list --json` gives the versions on gh-pages, and `uv run python`
    # runs the script's filter with the real Python
    (stubs / "uv").write_text(
        textwrap.dedent(
            """\
            #!/bin/sh
            echo "$*" >> "$UV_LOG"
            case "$*" in
              "run mike list --json") echo "$DEPLOYED" ;;
              "run python -c"*) shift 2; exec python3 "$@" ;;
            esac
            """
        )
    )
    for name in ("sudo", "cp"):
        (stubs / name).write_text("#!/bin/sh\n")
    for stub in stubs.iterdir():
        stub.chmod(0o755)
    (tmp_path / "pyproject.toml").write_text('version = "1.2.3"\n')
    env = {
        **os.environ,
        "PATH": f"{stubs}:{os.environ['PATH']}",
        "RUNNER_TEMP": str(tmp_path),
        "UV_LOG": str(log),
        "HEAD_COMMIT": head,
        "MAIN": main,
        "DEPLOYED": deployed,
    }
    subprocess.run(["bash", str(SCRIPT)], cwd=tmp_path, env=env, check=True)
    return log.read_text().splitlines()


def test_a_head_that_replaced_the_commit_is_built_in_strict_mode_before_it_is_deployed(
    tmp_path: Path,
):
    commands = _deploy(tmp_path, head="aaa", main="bbb")
    strict = next(i for i, c in enumerate(commands) if "mkdocs build --strict" in c)
    deploy = next(i for i, c in enumerate(commands) if "mike deploy" in c)
    assert strict < deploy


def test_the_commit_that_is_the_head_is_not_built_again(tmp_path: Path):
    commands = _deploy(tmp_path, head="aaa", main="aaa")
    assert not any("mkdocs build" in c for c in commands)
    assert any("mike deploy" in c for c in commands)


def test_a_release_is_deployed_as_its_minor_version_with_the_full_version_as_an_alias(
    tmp_path: Path,
):
    commands = _deploy(tmp_path, head="aaa", main="aaa")
    assert "run mike deploy --update-aliases 1.2 1.2.3 latest" in commands


def test_a_patch_release_deployed_under_its_full_version_is_replaced_by_its_minor(
    tmp_path: Path,
):
    deployed = json.dumps(
        [
            {"version": "1.2.0", "aliases": ["latest"]},
            {"version": "1.1.0", "aliases": []},
            {"version": "1.2", "aliases": []},
        ]
    )
    commands = _deploy(tmp_path, head="aaa", main="aaa", deployed=deployed)
    deletes = [c for c in commands if c.startswith("run mike delete")]
    assert deletes == ["run mike delete 1.2.0"]
    assert commands.index(deletes[0]) < next(
        i for i, c in enumerate(commands) if "mike deploy" in c
    )

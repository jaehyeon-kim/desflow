"""`scripts/deploy_docs.sh` deploys only docs that passed the strict build."""

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


def _deploy(tmp_path: Path, head: str, main: str) -> List[str]:
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
    (stubs / "uv").write_text('#!/bin/sh\necho "$*" >> "$UV_LOG"\n')
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

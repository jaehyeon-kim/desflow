"""The MCP Server page runs its two scripts in docs/snippets/integrations/.

Each runs with the command its page shows, in a folder of its own, against this
checkout's `desflow mcp`, and what it prints must be the output block under the
command on the page. The client entry the page gives for other assistants must be the
plugin's.
"""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "docs" / "integrations" / "mcp-server.md"
SNIPPETS = ROOT / "docs" / "snippets" / "integrations"
COMMANDS = (SNIPPETS / "commands.sh").read_text(encoding="utf-8")


def _command(section: str) -> str:
    """The one command line of a section of commands.sh."""
    text = COMMANDS.split(f"# --8<-- [start:{section}]\n")[1]
    [line] = [
        line
        for line in text.split(f"# --8<-- [end:{section}]")[0].splitlines()
        if not line.startswith("#")
    ]
    return line


def _shown(section: str) -> str:
    """The text block that follows the page's block of this section."""
    text = PAGE.read_text(encoding="utf-8")
    after = text.split(f'title="docs/snippets/integrations/commands.sh:{section}"')[1]
    found = re.search(r"```text\n(.*?)\n```", after, re.S)
    assert found, f"no output block after {section}"
    return found[1]


def _run(section: str, tmp_path: Path) -> str:
    """Runs the section's command in a folder with the scripts and the shop, with this
    checkout's Python and `desflow` first on PATH."""
    for name in ("mcp_client.py", "mcp_example.py"):
        shutil.copy(SNIPPETS / name, tmp_path)
    shutil.copy(ROOT / "docs" / "snippets" / "orchestrators" / "shop.yaml", tmp_path)
    bin_folder = str(Path(sys.executable).parent)
    env = {**os.environ, "PATH": bin_folder + os.pathsep + os.environ["PATH"]}
    done = subprocess.run(
        _command(section).split(),
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    assert done.returncode == 0, done.stderr[-3000:]
    return done.stdout.rstrip("\n")


def test_the_server_lists_the_tools_that_write_nothing(tmp_path):
    assert _run("list-tools", tmp_path) == _shown("list-tools")


def test_allow_changes_adds_the_tools_that_write(tmp_path):
    assert _run("list-tools-changes", tmp_path) == _shown("list-tools-changes")


def test_the_example_checks_explains_and_runs_the_shop(tmp_path):
    assert _run("run-example", tmp_path) == _shown("run-example")


def test_the_client_entry_is_the_plugins():
    entry = json.loads((SNIPPETS / "mcp.json").read_text(encoding="utf-8"))
    plugin = json.loads(
        (ROOT / "plugin" / "mcp_config.json").read_text(encoding="utf-8")
    )
    assert entry["mcpServers"] == plugin["mcpServers"]

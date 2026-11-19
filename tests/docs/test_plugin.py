"""The plugin for Claude Code, Codex and Antigravity: its manifests, marketplace files,
MCP entry and logo, which `scripts/gen_skill.py` writes.

Codex has no command that validates a plugin, so these tests are its check. The CI job
`plugin` validates the Claude Code and Antigravity files with each tool's own command.
"""

import importlib.util
import json
import re
import struct
from pathlib import Path

import pytest
from typer.main import get_command

from desflow.cli import app

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "gen_skill", _ROOT / "scripts" / "gen_skill.py"
)
assert _spec is not None and _spec.loader is not None
gen_skill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_skill)

PLUGIN = _ROOT / "plugin"
MANIFESTS = [
    "plugin/.claude-plugin/plugin.json",
    "plugin/.codex-plugin/plugin.json",
    "plugin/.antigravity-plugin/plugin.json",
]
MARKETPLACES = [".claude-plugin/marketplace.json", ".agents/plugins/marketplace.json"]
# Where each tool reads the MCP entry: Claude Code and Codex inline in their
# plugin.json, Antigravity from its own file at the plugin root
MCP_ENTRIES = [
    "plugin/.claude-plugin/plugin.json",
    "plugin/.codex-plugin/plugin.json",
    "plugin/mcp_config.json",
]


def _json(path: str):
    return json.loads((_ROOT / path).read_text(encoding="utf-8"))


def test_committed_plugin_files_are_generated():
    """No plugin file is edited by hand, so every manifest carries the version of
    pyproject.toml."""
    for path, content in gen_skill.plugin_files().items():
        assert (_ROOT / path).read_text(encoding="utf-8") == content, (
            f"{path} is out of date; run "
            "`uv run --all-extras python scripts/gen_skill.py`"
        )


def test_every_manifest_carries_the_version_it_is_given():
    files = gen_skill.plugin_files("9.8.7")
    for path in MANIFESTS:
        assert json.loads(files[path])["version"] == "9.8.7", path
    # A marketplace entry with its own version would differ from the manifest's and
    # make Claude Code's validate report a mismatch
    for path in MARKETPLACES:
        assert all(
            "version" not in entry for entry in json.loads(files[path])["plugins"]
        )


def test_every_tool_reads_the_same_mcp_entry_without_allow_changes():
    """The entry runs a registered command and not `--allow-changes`, which serves the
    tools that run a model and write files. A root `.mcp.json` would also be the
    repository's own project MCP file, which every Claude Code session in desflow
    loads, so there is none."""
    entry = {"desflow": {"command": "desflow", "args": ["mcp"]}}
    for path in MCP_ENTRIES:
        assert _json(path)["mcpServers"] == entry, path
    assert not (_ROOT / ".mcp.json").exists()
    assert "mcp" in get_command(app).commands  # type: ignore[attr-defined]


def test_every_marketplace_names_one_plugin_desflow_in_the_plugin_folder():
    """Codex needs plugin.json inside the plugin's folder, and each tool copies only
    that folder on install."""
    for path in MANIFESTS + MARKETPLACES:
        assert _json(path)["name"] == "desflow", path
    for path in MARKETPLACES:
        plugins = _json(path)["plugins"]
        assert [(p["name"], p["source"]) for p in plugins] == [("desflow", "./plugin")]


def test_the_plugin_folder_holds_only_the_plugin_and_no_symlink():
    """Antigravity copies the whole folder and fails on a symlink that points outside
    it, and Claude Code's validate --strict fails on a CLAUDE.md it does not load."""
    found = {p.relative_to(PLUGIN).parts[0] for p in PLUGIN.rglob("*")}
    assert found == {
        ".claude-plugin",
        ".codex-plugin",
        ".antigravity-plugin",
        "mcp_config.json",
        "assets",
        "skills",
    }
    assert not any(p.is_symlink() for p in PLUGIN.rglob("*"))
    skill = PLUGIN / "skills" / "desflow" / "SKILL.md"
    assert re.search(r"^name: desflow$", skill.read_text(encoding="utf-8"), re.M)


@pytest.mark.parametrize(
    ("manifest", "field"),
    [
        ("plugin/.claude-plugin/plugin.json", ("icon",)),
        ("plugin/.codex-plugin/plugin.json", ("interface", "logo")),
        ("plugin/.antigravity-plugin/plugin.json", ("logo",)),
    ],
)
def test_the_logo_is_a_square_png_of_512_px_or_more_outside_a_dot_folder(
    manifest, field
):
    """Claude Code's plugin directory asks for 512 to 2048 px, agy leaves out a path
    with a part that starts with a dot, and Codex needs `./` in front."""
    logo = _json(manifest)
    for key in field:
        logo = logo[key]
    assert logo.startswith("./") == ("interface" in field), logo
    relative = logo.removeprefix("./")
    assert not any(part.startswith(".") for part in relative.split("/"))
    header = (PLUGIN / relative).read_bytes()[:24]
    assert header[:8] == b"\x89PNG\r\n\x1a\n"
    width, height = struct.unpack(">II", header[16:24])
    assert width == height >= 512

"""`desflow mcp` serves desflow's commands as tools, and each tool gives what its
command gives.

Each tool is called through the SDK's in-memory client and compared with its command
run by `CliRunner` on the same files. The tools that write are checked to be absent
without `--allow-changes`, and the names of the tools and their parameters are pinned,
because 1.x keeps them.
"""

import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path
from unittest import mock

import anyio
import pytest
from typer.testing import CliRunner

# The jobs at the lowest Pydantic install one below the SDK's floor, so the SDK
# fails to import there and these tests are left to the other jobs
pytest.importorskip("mcp.server.mcpserver")

from mcp import Client, StdioServerParameters  # noqa: E402

from desflow import examples  # noqa: E402
from desflow.cli import app  # noqa: E402
from desflow import mcp_server  # noqa: E402
from desflow.mcp_server import build_server  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
READ_TOOLS = {"check", "explain", "test", "fit", "dist", "schema", "list_examples"}
WRITE_TOOLS = {"run", "experiment", "report", "get_example"}
_FIT = ["file", "column", "unit", "candidates", "lower", "upper", "bins", "min_count"]
_FIT += ["tolerance", "drop_invalid"]
_DIST = ["family", "rate", "mean", "std", "low", "value", "high", "mode", "shape"]
_DIST += ["scale", "p"]
# The names of the tools and of their parameters stay the same in every 1.x release
TOOL_PARAMETERS = {
    "check": ["files", "variables"],
    "explain": ["source", "verify", "until", "seed", "replications", "target"],
    "test": ["files", "variables"],
    "fit": _FIT,
    "dist": _DIST,
    "schema": [],
    "list_examples": [],
}
CHANGE_PARAMETERS = TOOL_PARAMETERS | {
    "test": ["files", "variables", "fix"],
    "fit": [*_FIT, "out", "plot"],
    "run": ["file", "until", "seed", "drain", "replications", "level", "measures"]
    + ["workers", "variables"],
    "experiment": ["file", "output"],
    "report": ["folder"],
    "get_example": ["name", "api", "dest", "ref", "force"],
}
# The parameters a call must give; a later 1.x release makes no other one required
REQUIRED = {"check": ["files"], "explain": ["source"], "test": ["files"]}
REQUIRED |= {"fit": ["column", "file"], "run": ["file"], "experiment": ["file"]}
REQUIRED |= {"report": ["folder"], "get_example": ["name"]}
# The 40 packing times of the tutorials, in seconds
PACKING = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip
EXPERIMENT = """\
experiment: packers
model: shop.yaml
until: 10 min
replications: 2
seed: 1
measures: [tasks.pack.wait_mean]
variants:
  two: {}
  three: {Shop.resources.packers.current_cap: 3}
"""


@pytest.fixture
def folder(tmp_path, monkeypatch):
    """A folder with the shop of the Command Line page, its packing times and a small
    experiment of it, as the working folder."""
    shutil.copy(
        ROOT / "docs/snippets/yaml/command_line_shop.yaml", tmp_path / "shop.yaml"
    )
    rows = "\n".join(str(value) for value in PACKING)
    (tmp_path / "packing.csv").write_text(f"seconds\n{rows}\n", encoding="utf-8")
    (tmp_path / "experiment.yaml").write_text(EXPERIMENT, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _call(name, arguments=None, allow_changes=False):
    """Calls one tool through an in-process client and returns the result."""

    async def run():
        async with Client(build_server(allow_changes)) as client:
            return await client.call_tool(name, arguments or {})

    return anyio.run(run)


def _tools(allow_changes):
    """The tools an in-process client lists, by name."""

    async def run():
        async with Client(build_server(allow_changes)) as client:
            return (await client.list_tools()).tools

    return {tool.name: tool for tool in anyio.run(run)}


def _command(*arguments):
    """Runs a command as a user does, and gives its result."""
    return CliRunner().invoke(app, list(arguments))


def _text(result):
    """The text a tool returned."""
    assert not result.is_error, result.content[0].text
    return result.structured_content["result"]


def test_the_tools_that_write_are_served_only_with_allow_changes():
    read = _tools(allow_changes=False)
    changes = _tools(allow_changes=True)
    assert set(read) == READ_TOOLS
    assert all(tool.annotations.read_only_hint for tool in read.values())
    assert set(changes) == READ_TOOLS | WRITE_TOOLS
    read_only = {
        name for name, tool in changes.items() if tool.annotations.read_only_hint
    }
    # With --allow-changes, test can fix and fit can write files
    assert read_only == READ_TOOLS - {"test", "fit"}


@pytest.mark.parametrize("allow_changes", [False, True])
def test_tool_and_parameter_names_stay_stable(allow_changes):
    tools = _tools(allow_changes)
    expected = CHANGE_PARAMETERS if allow_changes else TOOL_PARAMETERS
    assert {
        name: sorted(tool.input_schema.get("properties", {}))
        for name, tool in tools.items()
    } == {name: sorted(parameters) for name, parameters in expected.items()}
    assert {
        name: sorted(tool.input_schema.get("required", []))
        for name, tool in tools.items()
    } == {name: REQUIRED.get(name, []) for name in expected}


@pytest.mark.parametrize(
    "tool, arguments, command",
    [
        ("check", {"files": ["shop.yaml"]}, ["check", "shop.yaml"]),
        (
            "check",
            {"files": ["shop.yaml"], "variables": {"PACKERS": "3"}},
            ["check", "shop.yaml", "--var", "PACKERS=3"],
        ),
        ("test", {"files": ["shop.yaml"]}, ["test", "shop.yaml"]),
        (
            "fit",
            {"file": "packing.csv", "column": "seconds", "lower": 0},
            ["fit", "packing.csv", "--column", "seconds", "--lower", "0"],
        ),
        (
            "dist",
            {"family": "gamma", "mean": 1.5, "std": 0.4832},
            ["dist", "gamma", "--mean", "1.5", "--std", "0.4832"],
        ),
        ("dist", {}, ["dist"]),
        ("list_examples", {}, ["examples", "list"]),
    ],
    ids=["check", "check-variables", "test", "fit", "dist", "dist-list", "examples"],
)
def test_the_text_tools_return_what_the_command_prints(
    folder, tool, arguments, command
):
    if tool == "fit":
        pytest.importorskip("scipy")
    printed = _command(*command)
    assert printed.exit_code == 0, printed.output
    assert printed.stdout.strip() in _text(_call(tool, arguments))


@pytest.mark.parametrize(
    "tool, arguments, command",
    [
        ("explain", {"source": "shop.yaml"}, ["explain", "shop.yaml", "--json"]),
        (
            "explain",
            {"source": "shop.yaml", "verify": True, "seed": 3, "target": 0.9},
            ["explain", "shop.yaml", "--json", "--verify", "--seed", "3"]
            + ["--target", "0.9"],
        ),
        ("schema", {}, ["schema"]),
    ],
    ids=["explain", "explain-verify", "schema"],
)
def test_the_json_tools_return_the_document_of_the_command(
    folder, tool, arguments, command
):
    printed = _command(*command)
    assert printed.exit_code == 0, printed.output
    result = _call(tool, arguments)
    assert not result.is_error, result.content[0].text
    assert result.structured_content == json.loads(printed.stdout)


@pytest.mark.parametrize(
    "arguments, options",
    [
        (
            {"until": "1 min", "seed": 5, "variables": {"PACKERS": "3"}},
            ["--until", "1 min", "--seed", "5", "--var", "PACKERS=3"],
        ),
        (
            {"replications": 2, "seed": 5, "measures": ["tasks.pack.wait_mean"]},
            ["--replications", "2", "--seed", "5", "--measure"]
            + ["tasks.pack.wait_mean"],
        ),
    ],
    ids=["single", "replications"],
)
def test_run_returns_the_summary_the_command_writes(folder, arguments, options):
    printed = _command("run", "shop.yaml", *options, "--summary", "summary.json")
    assert printed.exit_code == 0, printed.output
    handlers = list(logging.getLogger().handlers)
    result = _call("run", {"file": "shop.yaml", **arguments}, allow_changes=True)
    assert not result.is_error, result.content[0].text
    expected = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
    assert result.structured_content == expected
    # The logging the command set up for the call is gone, and the server's is back
    assert logging.getLogger().handlers == handlers


def test_a_desflow_logger_a_call_creates_is_set_back_to_notset():
    """`desflow run --replications` sets the level of loggers that may not exist
    before the call, such as `desflow.builder`; the next call starts without it."""
    name = "desflow.test_mcp_server.created_by_the_call"
    assert name not in logging.root.manager.loggerDict
    with mcp_server._command_logging():
        logging.getLogger(name).setLevel(logging.WARNING)
    assert logging.getLogger(name).level == logging.NOTSET


def test_test_with_fix_writes_what_the_command_writes(folder):
    shutil.copy("shop.yaml", "copy.yaml")
    for name in ("shop.yaml", "copy.yaml"):
        text = Path(name).read_text(encoding="utf-8")
        Path(name).write_text(text.replace(": 224", ": 1"), encoding="utf-8")
    printed = _command("test", "copy.yaml", "--fix")
    assert printed.exit_code == 0, printed.output
    result = _call("test", {"files": ["shop.yaml"], "fix": True}, allow_changes=True)
    assert "fixed shop.yaml: two packers" in _text(result)
    assert Path("shop.yaml").read_text(encoding="utf-8") == Path("copy.yaml").read_text(
        encoding="utf-8"
    )


def test_fit_with_out_and_plot_writes_what_the_command_writes(folder):
    pytest.importorskip("scipy")
    options = {"file": "packing.csv", "column": "seconds", "lower": 0}
    printed = _command(
        *("fit", "packing.csv", "--column", "seconds", "--lower", "0"),
        *("--out", "cli.yaml", "--plot", "cli.png"),
    )
    assert printed.exit_code == 0, printed.output
    result = _call("fit", options | {"out": "tool.yaml", "plot": "tool.png"}, True)
    assert "tool.yaml" in _text(result)
    assert Path("tool.yaml").read_text() == Path("cli.yaml").read_text()
    assert Path("tool.png").stat().st_size > 0


def test_experiment_and_report_write_what_the_commands_write(folder):
    printed = _command("experiment", "experiment.yaml", "--output", "cli")
    assert printed.exit_code == 0, printed.output
    result = _call(
        "experiment", {"file": "experiment.yaml", "output": "tool"}, allow_changes=True
    )
    assert _text(result) == printed.stdout.strip().replace("cli", "tool")
    for name in ("results.csv", "summary.csv", "report.md"):
        assert (folder / "tool" / name).read_text() == (
            folder / "cli" / name
        ).read_text()
    (folder / "tool" / "report.md").unlink()
    report = _call("report", {"folder": "tool"}, allow_changes=True)
    assert "report.md" in _text(report)
    assert (folder / "tool" / "report.md").read_text() == (
        folder / "cli" / "report.md"
    ).read_text()


def test_get_example_downloads_what_the_command_downloads(folder):
    def fetch(url):
        return (ROOT / "examples" / url.split("/examples/", 1)[1]).read_bytes()

    with mock.patch.object(examples, "_fetch", side_effect=fetch):
        printed = _command("examples", "get", "local", "--api", "yaml", "--dest", "cli")
        result = _call(
            "get_example",
            {"name": "local", "api": "yaml", "dest": "tool"},
            allow_changes=True,
        )
    assert printed.exit_code == 0, printed.output
    assert _text(result) == printed.stdout.strip().replace("cli", "tool")
    assert (folder / "tool/local/yaml/local.yaml").read_bytes() == (
        ROOT / "examples/yaml/local.yaml"
    ).read_bytes()


@pytest.mark.parametrize(
    "tool, arguments, message",
    [
        ("check", {"files": ["nope.yaml"]}, "'nope.yaml' does not exist"),
        (
            "run",
            {"file": "shop.yaml", "measures": ["tasks.pack.wait_mean"]},
            "--measure can only be used with --replications",
        ),
        (
            "run",
            {"file": "shop.yaml", "workers": 2},
            "--workers can only be used with --replications",
        ),
        (
            "run",
            {"file": "shop.yaml", "level": 0.9},
            "--level can only be used with --replications",
        ),
        ("explain", {"source": "shop.yaml", "target": 20}, "--target must be above 0"),
    ],
    ids=[
        *("missing-file", "refused-option", "refused-workers", "refused-level"),
        "refused-value",
    ],
)
def test_a_refused_call_returns_the_message_of_the_command(
    folder, tool, arguments, message
):
    result = _call(tool, arguments, allow_changes=True)
    assert result.is_error
    assert message in result.content[0].text


def test_a_file_name_that_looks_like_an_option_stays_a_file_name(folder):
    """A read-only `test` given `--fix` among its files reads it as a file that is
    not there, not as the option that writes the expected values."""
    before = (folder / "shop.yaml").read_text(encoding="utf-8")
    result = _call("test", {"files": ["shop.yaml", "--fix"]})
    assert result.is_error
    assert "--fix" in result.content[0].text
    assert (folder / "shop.yaml").read_text(encoding="utf-8") == before


def test_a_single_run_with_no_end_time_is_refused(folder):
    """A run that never ends would hold the server's lock, so every later call would
    wait; the tool refuses it as replications are refused, and a given until runs."""
    shop = (folder / "shop.yaml").read_text(encoding="utf-8")
    (folder / "endless.yaml").write_text(
        shop.replace("run: {until: 10 min}", ""), encoding="utf-8"
    )
    result = _call("run", {"file": "endless.yaml"}, allow_changes=True)
    assert result.is_error
    assert "a run needs an end time" in result.content[0].text
    result = _call("run", {"file": "endless.yaml", "until": "1 min"}, True)
    assert not result.is_error, result.content[0].text


def test_the_protocol_stream_holds_only_messages(folder):
    """`desflow mcp` is started as a client starts it, and runs a model whose Console
    sink prints records. Nothing reaches stdout outside the messages: before the first
    message, and during the call."""
    console = (folder / "shop.yaml").read_text(encoding="utf-8")
    console = console.replace(
        "stats: basic", "egress:\n  - {type: Console}\n\nstats: basic"
    )
    (folder / "console.yaml").write_text(console, encoding="utf-8")
    command = [sys.executable, "-c", "from desflow.cli import app; app()", "mcp"]

    closed_at_once = subprocess.run(
        [*command, "--allow-changes"],
        cwd=folder,
        input=b"",
        capture_output=True,
        timeout=60,
    )
    assert closed_at_once.returncode == 0
    assert closed_at_once.stdout == b""

    async def run():
        server = StdioServerParameters(
            command=command[0], args=[*command[1:], "--allow-changes"], cwd=folder
        )
        async with Client(server) as client:
            return await client.call_tool(
                "run", {"file": "console.yaml", "until": "1 min", "seed": 5}
            )

    result = anyio.run(run)
    assert not result.is_error, result.content[0].text
    assert result.structured_content["end_time"] == 60

"""The MCP server of `desflow mcp`: desflow's commands as tools for an AI assistant.

Each tool runs its command through the `desflow` Typer app, with the arguments a user
would type, so a tool and its command parse, check and refuse the same way. A tool
whose command prints JSON (`explain --json`, `schema`, and the file of `run
--summary`) returns that document as its structured content; every other tool returns
what its command prints. A command that exits with a code other than 0 makes the tool
return an error with the command's message.

The tools that write nothing are always served. The tools that write, `run`,
`experiment`, `report`, `get_example`, `test` with `fix` and `fit` with `out` or
`plot`, are served only with `desflow mcp --allow-changes`, because a run sends records
to the blueprint's sinks and the others write files.
"""

import contextlib
import io
import json
import logging
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, Iterator, List, NamedTuple, Optional

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, TextContent, ToolAnnotations

# One tool runs at a time: the redirect of stdout and stderr, and the logging set up
# by the command, belong to one call
_lock = threading.Lock()

_READ_ONLY = ToolAnnotations(read_only_hint=True)
_WRITES = ToolAnnotations(read_only_hint=False, destructive_hint=True)


@contextlib.contextmanager
def _command_logging() -> Iterator[None]:
    """Gives a command the logging a new `desflow` process has, then puts back the
    server's.

    A command sets up logging with `logging.basicConfig`, which adds a handler only to
    a root logger that has none, and `desflow run --replications` quietens two of
    desflow's loggers, which may not exist before the call. So the root logger's
    handlers are set aside for the call, every desflow logger's level is put back after
    it, and a desflow logger the call created is set back to NOTSET.
    """
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    levels = {
        name: logger.level
        for name, logger in logging.root.manager.loggerDict.items()
        if isinstance(logger, logging.Logger) and name.startswith("desflow")
    }
    for handler in handlers:
        root.removeHandler(handler)
    try:
        yield
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            handler.close()
        for handler in handlers:
            root.addHandler(handler)
        root.setLevel(level)
        for name, logger in list(logging.root.manager.loggerDict.items()):
            if isinstance(logger, logging.Logger) and name.startswith("desflow"):
                logger.setLevel(levels.get(name, logging.NOTSET))


class _Printed(NamedTuple):
    """What a command wrote to stdout and to stderr."""

    stdout: str
    stderr: str

    def text(self) -> str:
        """What the command printed, stderr first, without trailing blank lines."""
        return (self.stderr + self.stdout).rstrip()


def _run(arguments: List[str]) -> _Printed:
    """
    Runs a `desflow` command in this process and captures what it prints.

    Args:
        arguments: The command line after `desflow`, such as `["check", "shop.yaml"]`.

    Returns:
        _Printed: What the command wrote to stdout and to stderr.

    Raises:
        ToolError: With the command's output as the message, when the command exits
            with a code other than 0 or raises.
    """
    from desflow.cli import app

    out, err = io.StringIO(), io.StringIO()
    code: Any = 0
    with (
        _lock,
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
        _command_logging(),
    ):
        try:
            app(args=arguments, prog_name="desflow")
        except SystemExit as exit_:
            code = exit_.code
        except Exception as exc:
            # An error inside a run keeps its traceback in the CLI; the tool gives its
            # message
            code = 1
            err.write(f"{type(exc).__name__}: {exc}\n")
    if code not in (0, None):
        text = _Printed(out.getvalue(), err.getvalue()).text()
        raise ToolError(text or f"desflow {arguments[0]} exited with code {code}.")
    return _Printed(out.getvalue(), err.getvalue())


def _document(text: str) -> CallToolResult:
    """A JSON document a command printed or wrote, as the tool's structured content."""
    document = json.loads(text)
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(document, indent=2))],
        structured_content=document,
    )


def _options(**values: Any) -> List[str]:
    """The command-line options for these values, in order: a name becomes
    `--name-with-dashes`, True a flag, a list a repeated option, a mapping repeated
    `--var NAME=VALUE`, and None or False nothing."""
    arguments: List[str] = []
    for name, value in values.items():
        option = "--" + name.replace("_", "-")
        if value is None or value is False:
            continue
        if value is True:
            arguments.append(option)
        elif isinstance(value, dict):
            for key, text in value.items():
                arguments += [option, f"{key}={text}"]
        elif isinstance(value, list):
            for item in value:
                arguments += [option, str(item)]
        else:
            arguments += [option, str(value)]
    return arguments


def build_server(allow_changes: bool) -> MCPServer:
    """
    Builds the MCP server with desflow's tools.

    Args:
        allow_changes: Also serve the tools that write: `run`, `experiment`,
            `report`, `get_example`, `test` with `fix`, and `fit` with `out` and
            `plot`.

    Returns:
        MCPServer: The server. `run()` serves it on stdio.
    """
    server = MCPServer("desflow")

    @server.tool(
        description="Check that each blueprint or builder file builds, without "
        "running it, as `desflow check` does. variables fill ${NAME} as --var does.",
        annotations=_READ_ONLY,
    )
    def check(files: List[str], variables: Optional[Dict[str, str]] = None) -> str:
        """Checks each file and returns what `desflow check` prints."""
        return _run(["check", *files, *_options(var=variables)]).text()

    @server.tool(
        description="Explain a model as `desflow explain --json` does: its graph, "
        "each resource's utilisation, status and wait where the queueing formulas "
        "apply, the bottleneck and the tuning hints. With verify, also run it "
        "unpaced and compare. The shape can change with its schema_version.",
        annotations=_READ_ONLY,
    )
    def explain(
        source: str,
        verify: bool = False,
        until: Optional[str] = None,
        seed: Optional[int] = None,
        replications: Optional[int] = None,
        target: float = 0.85,
    ) -> CallToolResult:
        """Explains the model and returns the document `desflow explain --json`
        prints."""
        arguments = ["explain", source, "--json", "--target", str(target)]
        arguments += _options(
            verify=verify, until=until, seed=seed, replications=replications
        )
        return _document(_run(arguments).stdout)

    _add_test(server, allow_changes)
    _add_fit(server, allow_changes)

    @server.tool(
        description="List the distribution families, or convert a family's "
        "parameters to its mean and standard deviation and back, as `desflow dist` "
        "does.",
        annotations=_READ_ONLY,
    )
    def dist(
        family: Optional[str] = None,
        rate: Optional[float] = None,
        mean: Optional[float] = None,
        std: Optional[float] = None,
        low: Optional[float] = None,
        value: Optional[float] = None,
        high: Optional[float] = None,
        mode: Optional[float] = None,
        shape: Optional[float] = None,
        scale: Optional[float] = None,
        p: Optional[float] = None,
    ) -> str:
        """Returns what `desflow dist` prints."""
        arguments = ["dist", *([family] if family else [])]
        arguments += _options(
            rate=rate,
            mean=mean,
            std=std,
            low=low,
            value=value,
            high=high,
            mode=mode,
            shape=shape,
            scale=scale,
            p=p,
        )
        return _run(arguments).text()

    @server.tool(
        description="Return the blueprint JSON Schema, as `desflow schema` prints it.",
        annotations=_READ_ONLY,
    )
    def schema() -> CallToolResult:
        """Returns the blueprint JSON Schema."""
        return _document(_run(["schema"]).stdout)

    @server.tool(
        description="List the examples, with what each shows and the service it "
        "needs, as `desflow examples list` does.",
        annotations=_READ_ONLY,
    )
    def list_examples() -> str:
        """Returns what `desflow examples list` prints."""
        return _run(["examples", "list"]).text()

    if allow_changes:
        _add_changes(server)
    return server


def _add_test(server: MCPServer, allow_changes: bool) -> None:
    """Adds `test`, with `fix` only when the server allows changes."""
    description = (
        "Run the tests of each blueprint, or of each builder file with a tests.yaml "
        "beside it, and report the statistics that differ, as `desflow test` does. "
        "Each test runs once, unpaced, with no ingress and no egress."
    )
    if not allow_changes:

        @server.tool(name="test", description=description, annotations=_READ_ONLY)
        def read_test(
            files: List[str], variables: Optional[Dict[str, str]] = None
        ) -> str:
            """Returns what `desflow test` prints."""
            return _run(["test", *files, *_options(var=variables)]).text()

        return

    @server.tool(
        name="test",
        description=description + " With fix, write the statistics the runs gave "
        "into the expect values that differ, as --fix does.",
        annotations=_WRITES,
    )
    def fixing_test(
        files: List[str],
        variables: Optional[Dict[str, str]] = None,
        fix: bool = False,
    ) -> str:
        """Returns what `desflow test` prints, after fixing with `fix`."""
        return _run(["test", *files, *_options(fix=fix, var=variables)]).text()


def _add_fit(server: MCPServer, allow_changes: bool) -> None:
    """Adds `fit`, with `out` and `plot` only when the server allows changes."""
    description = (
        "Fit a column of a CSV file to the distribution families and rank them, as "
        "`desflow fit` does. Needs the fit extra."
    )

    def fit_command(file: str, column: str, **options: Any) -> str:
        """Runs `desflow fit` with these options and returns what it prints."""
        return _run(["fit", file, "--column", column, *_options(**options)]).text()

    if not allow_changes:

        @server.tool(name="fit", description=description, annotations=_READ_ONLY)
        def read_fit(
            file: str,
            column: str,
            unit: str = "s",
            candidates: Optional[str] = None,
            lower: Optional[float] = None,
            upper: Optional[float] = None,
            bins: Optional[int] = None,
            min_count: int = 20,
            tolerance: float = 0.25,
            drop_invalid: bool = False,
        ) -> str:
            """Returns what `desflow fit` prints."""
            return fit_command(
                file,
                column,
                unit=unit,
                candidates=candidates,
                lower=lower,
                upper=upper,
                bins=bins,
                min_count=min_count,
                tolerance=tolerance,
                drop_invalid=drop_invalid,
            )

        return

    @server.tool(
        name="fit",
        description=description + " out writes the best fit as YAML, and plot "
        "draws the fits as an image, as --out and --plot do.",
        annotations=_WRITES,
    )
    def writing_fit(
        file: str,
        column: str,
        unit: str = "s",
        candidates: Optional[str] = None,
        lower: Optional[float] = None,
        upper: Optional[float] = None,
        bins: Optional[int] = None,
        min_count: int = 20,
        tolerance: float = 0.25,
        drop_invalid: bool = False,
        out: Optional[str] = None,
        plot: Optional[str] = None,
    ) -> str:
        """Returns what `desflow fit` prints, after writing `out` and `plot`."""
        return fit_command(
            file,
            column,
            unit=unit,
            candidates=candidates,
            lower=lower,
            upper=upper,
            bins=bins,
            min_count=min_count,
            tolerance=tolerance,
            drop_invalid=drop_invalid,
            out=out,
            plot=plot,
        )


def _add_changes(server: MCPServer) -> None:
    """Adds the tools that run models with their sinks or write files."""

    @server.tool(
        description="Run a blueprint once, or its replications, as `desflow run "
        "--summary` does, and return the summary it writes: the statistics of the "
        "run, or the seeds, the summary and every replication's values. A run "
        "sends its records to the blueprint's sinks. measures, level and workers "
        "apply only with replications. variables fill ${NAME} as --var does.",
        annotations=_WRITES,
    )
    def run(
        file: str,
        until: Optional[str] = None,
        seed: Optional[int] = None,
        drain: bool = False,
        replications: Optional[int] = None,
        level: float = 0.95,
        measures: Optional[List[str]] = None,
        workers: int = 1,
        variables: Optional[Dict[str, str]] = None,
    ) -> CallToolResult:
        """Runs the blueprint and returns the document `--summary` writes."""
        arguments = ["run", file]
        arguments += _options(
            until=until, seed=seed, drain=drain, replications=replications
        )
        if replications is not None:
            arguments += _options(level=level, workers=workers)
        # desflow run refuses --measure, --level and --workers without
        # --replications; measures is passed on, so the tool refuses as the command
        # does, and level and workers keep their defaults
        arguments += _options(measure=measures)
        arguments += _options(var=variables)
        with tempfile.TemporaryDirectory() as folder:
            summary = Path(folder) / "summary.json"
            _run([*arguments, "--summary", str(summary)])
            return _document(summary.read_text(encoding="utf-8"))

    @server.tool(
        description="Run an experiment file and write its artifacts and report.md, "
        "as `desflow experiment` does. output overrides the folder the file names.",
        annotations=_WRITES,
    )
    def experiment(file: str, output: Optional[str] = None) -> str:
        """Returns what `desflow experiment` prints."""
        return _run(["experiment", file, *_options(output=output)]).text()

    @server.tool(
        description="Build an experiment's report.md again from the artifacts in "
        "its output folder, as `desflow report` does.",
        annotations=_WRITES,
    )
    def report(folder: str) -> str:
        """Returns what `desflow report` prints."""
        return _run(["report", folder]).text()

    @server.tool(
        description="Download an example into <dest>/<name>/<api>/ at the installed "
        "release, as `desflow examples get` does. force replaces files that are "
        "already there.",
        annotations=_WRITES,
    )
    def get_example(
        name: str,
        api: Optional[str] = None,
        dest: str = ".",
        ref: Optional[str] = None,
        force: bool = False,
    ) -> str:
        """Returns what `desflow examples get` prints."""
        arguments = ["examples", "get", name]
        arguments += _options(api=api, dest=dest, ref=ref, force=force)
        return _run(arguments).text()

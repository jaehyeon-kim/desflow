"""`desflow mcp`: serves the desflow commands as MCP tools on stdio."""

from typing import Annotated

import typer

from desflow.cli._common import fail

# The examples --help prints after the options, as odctl's help does
_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# Serve the tools that write nothing, as an MCP client starts the server[/dim]\n
  $ [bold cyan]desflow mcp[/bold cyan]\n\n
  [dim]# Also serve the tools that run models and write files[/dim]\n
  $ [bold cyan]desflow mcp --allow-changes[/bold cyan]
"""


def register(app: typer.Typer) -> None:
    """Adds the `mcp` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("mcp", rich_help_panel="Assistants", epilog=_EPILOG)(_mcp)


# Each command is private: typer shows its docstring as the help text, so it
# carries no Args section, and register() gives the command its name
def _mcp(
    allow_changes: Annotated[
        bool,
        typer.Option(
            "--allow-changes",
            help="Also serve the tools that write: run, experiment, report and "
            "get_example, test with fix, and fit with out and plot. A run sends "
            "records to the blueprint's sinks, and the others write files.",
        ),
    ] = False,
) -> None:
    """Serve the desflow commands as MCP tools to an AI assistant on this machine.

    An MCP client starts the server as a process and exchanges messages with it over
    its standard input and output. Each tool runs the command of the same name. The
    tools that write are served only with --allow-changes, an option that shows in
    the client's configuration. Needs the mcp extra.
    """
    try:
        from desflow.mcp_server import build_server
    except ImportError:
        fail(
            "desflow mcp needs the mcp extra. Install it with pip install 'desflow[mcp]'"
        )
    build_server(allow_changes).run()

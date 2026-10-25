"""The `desflow` command.

Each command lives in its own module, which parses the arguments and calls a
package. A module imports that package only when its command runs.
"""

import logging
from typing import Annotated

import typer

from desflow.cli import (
    dist,
    examples,
    experiment,
    explain,
    fit,
    report,
    run,
    schema,
)

app = typer.Typer(
    help="Run, explain and analyse desflow simulations, fit their inputs, and download the examples.",
    no_args_is_help=True,
    add_completion=False,
    epilog="""
[bold underline]Examples:[/bold underline]\n
  [dim]# Run a blueprint, sending its records to its sinks[/dim]\n
  $ [bold cyan]desflow run shop.yaml[/bold cyan]\n\n
  [dim]# Print the first five events, with no services[/dim]\n
  $ [bold cyan]desflow run shop.yaml --sample 5[/bold cyan]\n\n
  [dim]# Find the bottleneck before running[/dim]\n
  $ [bold cyan]desflow explain shop.yaml[/bold cyan]\n\n
  [dim]# List the examples, then download one[/dim]\n
  $ [bold cyan]desflow examples list[/bold cyan]\n
  $ [bold cyan]desflow examples get local[/bold cyan]
""",
)

logger = logging.getLogger("desflow.cli")


def _show_version(value: bool) -> None:
    if value:
        from desflow import __version__

        typer.echo(__version__)
        raise typer.Exit()


@app.callback()
def _main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Print the installed version and exit.",
            callback=_show_version,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Run, explain and analyse desflow simulations, fit their inputs, and download the examples."""


for _module in (run, explain, experiment, report, fit, dist, schema, examples):
    _module.register(app)


def ddes() -> None:
    """Run the `desflow` command under its old name, `ddes`, after a warning.

    `ddes` was the command before the package was renamed from dynamic-des to
    desflow. It is removed in a later release, as the Changelog says.
    """
    typer.echo(
        "Warning: the ddes command is deprecated and will be removed in a later "
        "release. Use desflow instead.",
        err=True,
    )
    app(prog_name="desflow")

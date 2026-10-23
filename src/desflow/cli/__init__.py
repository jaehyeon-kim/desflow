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
    help="Run desflow simulations defined in YAML blueprint files, and download the examples.",
    no_args_is_help=True,
    add_completion=False,
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
    """Run desflow simulations defined in YAML blueprint files, and download the examples."""


for _module in (run, experiment, report, fit, schema, examples, dist, explain):
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

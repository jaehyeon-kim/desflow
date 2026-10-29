"""`desflow experiment`: runs an experiment file through `desflow.analysis`."""

import logging
from pathlib import Path
from typing import Annotated, Optional

import typer

from desflow.cli._common import refuse, setup_logging


# The examples --help prints after the options, as odctl's help does
_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# Run an experiment and write its results to a folder[/dim]\n
  $ [bold cyan]desflow experiment experiment.yaml --output results[/bold cyan]
"""


def register(app: typer.Typer) -> None:
    """Adds the `experiment` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("experiment", rich_help_panel="Studies", epilog=_EPILOG)(_experiment)


# Each command is private: typer shows its docstring as the help text, so it
# carries no Args section
def _experiment(
    file: Annotated[
        Path,
        typer.Argument(
            help="The experiment file.",
            metavar="FILE",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    output: Annotated[
        Optional[Path],
        typer.Option(
            metavar="FOLDER",
            help="Where to write the artifacts. Overrides output in the file.",
        ),
    ] = None,
) -> None:
    """Run the experiment in FILE and write its artifacts and report.

    Exits 1 if a check fails.
    """
    from desflow.analysis.runner import run_experiment
    from desflow.blueprint.loader import BlueprintError

    setup_logging(logging.WARNING)
    try:
        result = run_experiment(file, raise_on_failure=False, output=output)
    except (BlueprintError, ValueError) as exc:
        refuse(str(exc))
    typer.echo(f"Wrote the artifacts and report.md to {result.output}")
    for row in result.checks:
        state = "passed" if row["passed"] else "FAILED"
        typer.echo(
            f"{state}: {row['design']}: {row['measure']} {row['bound']} "
            f"{row['limit']:g}, checked {row['value']:.4g}"
        )
    if result.failed:
        raise typer.Exit(code=1)

"""`desflow report`: builds an experiment's report again from its artifacts."""

from pathlib import Path
from typing import Annotated

import typer


def register(app: typer.Typer) -> None:
    """Adds the `report` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("report")(_report)


def _report(
    folder: Annotated[
        Path,
        typer.Argument(
            help="An experiment's output folder, with its experiment.json.",
            metavar="FOLDER",
            exists=True,
            file_okay=False,
        ),
    ],
) -> None:
    """Build the report of an experiment again from the artifacts in FOLDER."""
    from desflow.analysis.report import write_report

    try:
        path = write_report(folder)
    except FileNotFoundError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    typer.echo(f"Wrote {path}")

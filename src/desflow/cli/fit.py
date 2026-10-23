"""`desflow fit`: fits a column of a CSV file through `desflow.analysis.fit`."""

from pathlib import Path
from typing import Annotated, Optional

import typer


def register(app: typer.Typer) -> None:
    """Adds the `fit` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("fit")(_fit)


def _fit(
    file: Annotated[
        Path,
        typer.Argument(
            help="A CSV file with a header row.",
            metavar="FILE",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    column: Annotated[str, typer.Option(metavar="NAME", help="The column to fit.")],
    unit: Annotated[
        str,
        typer.Option(help="The data's unit: ms, s, min or h. Settings are in seconds."),
    ] = "s",
    candidates: Annotated[
        Optional[str],
        typer.Option(
            metavar="NAMES",
            help="The families to try, separated by commas. Defaults to every family.",
        ),
    ] = None,
    lower: Annotated[
        Optional[float],
        typer.Option(help="The lowest plausible value, in the data's unit."),
    ] = None,
    upper: Annotated[
        Optional[float],
        typer.Option(help="The highest plausible value, in the data's unit."),
    ] = None,
    bins: Annotated[
        Optional[int],
        typer.Option(
            min=1,
            help="Offer the empirical distribution as a histogram with this many bins.",
        ),
    ] = None,
    min_count: Annotated[
        int, typer.Option(min=1, help="The fewest values a family is fitted to.")
    ] = 20,
    tolerance: Annotated[
        float,
        typer.Option(
            help="How far a fitted mean and standard deviation may be from the "
            "data's, as a share of the data's.",
        ),
    ] = 0.25,
    drop_invalid: Annotated[
        bool,
        typer.Option(help="Leave out invalid values rather than stopping."),
    ] = False,
    out: Annotated[
        Optional[Path],
        typer.Option(
            metavar="FILE",
            help="Write the best candidate's settings here, for !include in a blueprint.",
        ),
    ] = None,
    plot: Annotated[
        Optional[Path],
        typer.Option(
            metavar="FILE",
            help="Draw the data against the best candidate, as a PNG.",
        ),
    ] = None,
) -> None:
    """Fit a column of FILE to the distribution families, and exit 1 if none is usable."""
    from desflow.analysis.fit import fit_column, read_column

    try:
        result = fit_column(
            read_column(file, column),
            unit=unit,
            candidates=candidates.split(",") if candidates else None,
            lower=lower,
            upper=upper,
            bins=bins,
            min_count=min_count,
            tolerance=tolerance,
            drop_invalid=drop_invalid,
        )
    except (ValueError, ImportError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    typer.echo(result.text())
    if result.best is None:
        raise typer.Exit(code=1)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(result.yaml(source=f"{file.name}, column {column}"))
        typer.echo(f"Wrote {out}")
    if plot is not None:
        result.plot(plot)
        typer.echo(f"Wrote {plot}")

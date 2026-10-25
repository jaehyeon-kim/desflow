"""`desflow explain`: the graph of a model and the queueing estimates that apply."""

import logging
from pathlib import Path
from typing import Annotated, Optional

import typer

from desflow.cli.run import _parse_until


# The examples --help prints after the options, as odctl's help does
_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# Show the graph, the utilisation and the bottleneck[/dim]\n
  $ [bold cyan]desflow explain shop.yaml[/bold cyan]\n\n
  [dim]# Size each resource for a utilisation of 0.7[/dim]\n
  $ [bold cyan]desflow explain shop.yaml --target 0.7[/bold cyan]\n\n
  [dim]# Compare the formulas with 10 replications of the model[/dim]\n
  $ [bold cyan]desflow explain shop.yaml --verify --replications 10[/bold cyan]\n\n
  [dim]# Print the result as JSON[/dim]\n
  $ [bold cyan]desflow explain shop.yaml --json[/bold cyan]
"""


def register(app: typer.Typer) -> None:
    """Adds the `explain` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("explain", rich_help_panel="Run and Check", epilog=_EPILOG)(_explain)


def _refuse(message: str) -> None:
    """Exits 2, as for a usage error: a missing file, a file with no context,
    options that do not go together, or an end at or before the warm-up's end."""
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=2)


def _invalid(message: str) -> None:
    """Exits 1, as `desflow run` does for an invalid blueprint."""
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=1)


def _explain(
    source: Annotated[
        str,
        typer.Argument(
            help="A YAML blueprint, or a builder Python file with a module-level "
            "SimulationContext, local or a URI such as s3://bucket/sims/shop.yaml.",
            metavar="SOURCE",
        ),
    ],
    as_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Print the explanation as JSON (schema_version 0, experimental).",
        ),
    ] = False,
    verify: Annotated[
        bool,
        typer.Option(
            "--verify",
            help="Also run the model unpaced and compare its statistics with the "
            "analysis.",
        ),
    ] = False,
    until: Annotated[
        Optional[str],
        typer.Option(
            metavar="TIME",
            help="With --verify: the end of the run, in seconds or as a duration "
            "such as '10 min'. Overrides run.until in the file.",
        ),
    ] = None,
    seed: Annotated[
        Optional[int],
        typer.Option(
            metavar="N",
            help="With --verify: the run's seed. Overrides the model's own.",
        ),
    ] = None,
    replications: Annotated[
        Optional[int],
        typer.Option(
            metavar="N",
            min=2,
            help="With --verify: run N times and compare with each measure's 95% "
            "interval, instead of one run within 10 percent.",
        ),
    ] = None,
    target: Annotated[
        float,
        typer.Option(
            metavar="T",
            help="The utilisation the hints aim for, above 0 and at most 10; at 1 "
            "or above the hints are for deliberate overload.",
        ),
    ] = 0.85,
) -> None:
    """Show the model in SOURCE as a graph, with its utilisation, status, waits and tuning hints where the queueing formulas apply, and why where they do not."""
    from desflow.blueprint.loader import BlueprintError
    from desflow.explain.render import render_terminal, to_json
    from desflow.loading import load_context

    if not 0 < target <= 10:
        _refuse(f"--target must be above 0 and at most 10, got {target:g}")
    if not verify and (
        until is not None or seed is not None or replications is not None
    ):
        _refuse("--until, --seed and --replications apply to --verify; add --verify")
    stop_at = _parse_until(until)
    folder, _, entry = source.rpartition("/")
    if "://" not in source:
        path = Path(source)
        if not path.is_file():
            _refuse(f"no file {source}")
        folder, entry = str(path.parent), path.name
    # A warning while loading, such as a capacity clamped to max_cap, is shown on
    # stderr; the explanation stays alone on stdout
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        context = load_context(folder, entry)
    except BlueprintError as exc:
        _invalid(str(exc))
    except ValueError as exc:
        # A Python file with no SimulationContext, such as a low-level script
        _refuse(str(exc))
    if verify and stop_at is None and context._default_until is None:
        _refuse(
            "--verify needs an end time: pass --until, or set run.until in the file"
        )
    if verify:
        from desflow.analysis.models import seconds
        from desflow.explain.verify import window_error

        end = seconds(stop_at if stop_at is not None else context._default_until)
        start = context.initial_time + (context.warmup or 0.0)
        if end is not None and end <= start:
            _refuse(window_error(end, start, "--verify"))
    try:
        explanation = context.analyze(
            verify, until=stop_at, seed=seed, replications=replications, target=target
        )
    except ValueError as exc:
        # A value the verifying run refuses, as desflow run reports it
        _invalid(str(exc))
    typer.echo(
        to_json(explanation) if as_json else render_terminal(explanation), nl=as_json
    )

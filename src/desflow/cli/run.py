"""`desflow run`: runs a blueprint, or replications of it."""

import logging
import secrets
import shlex
from pathlib import Path
from typing import Annotated, Any, List, Optional

import typer

from desflow.utils import time_to_seconds

logger = logging.getLogger("desflow.cli")


def register(app: typer.Typer) -> None:
    """Adds the `run` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("run")(_run)


def _run_options(until: Optional[str], drain: bool) -> str:
    """The options given that change the run, as a repeat command needs them."""
    options = f" --until {shlex.quote(until)}" if until is not None else ""
    return options + (" --drain" if drain else "")


def _parse_until(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        pass
    try:
        return time_to_seconds(value)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None


# Each command is private: typer shows its docstring as the help text, so it
# carries no Args section, and register() gives the command its name
def _run(
    file: Annotated[
        Path,
        typer.Argument(
            help="The blueprint file.",
            metavar="FILE",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    until: Annotated[
        Optional[str],
        typer.Option(
            metavar="TIME",
            help="Simulation time to stop at, in seconds or as a duration such as "
            "'10 min'. Overrides run.until in the file.",
        ),
    ] = None,
    seed: Annotated[
        Optional[int],
        typer.Option(
            metavar="N",
            help="The random seed. Overrides simulation.random_seed in the file. A "
            "run without one logs the seed it drew, with the command that repeats it.",
        ),
    ] = None,
    drain: Annotated[
        bool,
        typer.Option(
            "--drain",
            help="At the end time, stop the arrivals and keep running until every "
            "entity in the model has left. Sets run.drain in the file to true.",
        ),
    ] = False,
    replications: Annotated[
        Optional[int],
        typer.Option(
            metavar="N",
            min=2,
            help="Run N replications unpaced, with seeds spawned from the seed, and "
            "print the mean and confidence interval of each statistic.",
        ),
    ] = None,
    level: Annotated[
        float,
        typer.Option(
            help="The confidence level of the replications' intervals, between 0 "
            "and 1.",
        ),
    ] = 0.95,
    measure: Annotated[
        Optional[List[str]],
        typer.Option(
            metavar="NAME",
            help="A statistic to summarise, such as tasks.pack.wait_mean; repeat it "
            "for more. Defaults to every statistic the run publishes, at least up to typical.",
        ),
    ] = None,
    workers: Annotated[
        int,
        typer.Option(
            metavar="N",
            min=1,
            help="Replications run at once, each in its own process.",
        ),
    ] = 1,
    sample: Annotated[
        Optional[int],
        typer.Option(
            metavar="N",
            min=1,
            help="Print the first N events as JSON lines, unpaced, with no ingress "
            "and no real sinks.",
        ),
    ] = None,
    stdout: Annotated[
        bool,
        typer.Option(
            "--stdout",
            help="Print every record except the system.simulation.lag_seconds "
            "readings as a JSON line, instead of writing to the file's sinks. The "
            "lag readings describe the wall clock, not the model.",
        ),
    ] = False,
) -> None:
    """Build the simulation in FILE and run it, or run replications of it."""
    from desflow.blueprint.build import Preview
    from desflow.blueprint.loader import BlueprintError
    from desflow.loading import load_context

    stop_at = _parse_until(until)
    preview = Preview(sample) if sample is not None or stdout else None
    if preview is not None and replications is not None:
        typer.echo(
            "Error: --sample and --stdout print one run; leave out --replications",
            err=True,
        )
        raise typer.Exit(code=2)
    if file.suffix == ".py":
        typer.echo(
            "Error: desflow run takes a YAML blueprint; run a Python file with "
            "python FILE",
            err=True,
        )
        raise typer.Exit(code=2)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    if replications is not None:
        # Each replication would log its build and teardown; the sinks still log
        logging.getLogger("desflow.core").setLevel(logging.WARNING)

    try:
        context = load_context(file.parent, file.name, preview)
    except BlueprintError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    if seed is not None:
        context.random_seed = seed
    if context._repeat_hint is not None:
        # The run's seed is filled in later, so braces in the until are doubled
        options = _run_options(until, drain)
        context._repeat_hint += options.replace("{", "{{").replace("}", "}}")
    if replications is not None:
        _replicate(
            file,
            replications,
            context.random_seed,
            stop_at,
            level,
            measure,
            workers,
            drain,
            _run_options(until, drain),
        )
        return
    try:
        context.start(until=stop_at, drain=True if drain else None)
    except ValueError as exc:
        # A run value refused before the run starts, such as --drain without an
        # until, is reported as a blueprint error is; an error inside the run keeps
        # its traceback
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None
    try:
        context.finish()
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user.")
    if sample is not None and context._sample_seen < sample:
        _report_short_sample(context, sample)


def _report_short_sample(context: Any, sample: int) -> None:
    """Says on stderr that the run ended before it printed `sample` events."""
    seen = context._sample_seen
    ended = context._until is not None and context.env.now >= context._until
    where = " at until" if ended else ""
    typer.echo(f"sample ended{where} with {seen} of {sample} events", err=True)


def _replicate(
    file: Path,
    n: int,
    seed: Optional[int],
    until: Optional[float],
    level: float,
    measures: Optional[List[str]],
    workers: int,
    drain: bool,
    options: str,
) -> None:
    """Runs the replications and prints their summary; `--drain` drains each."""
    from desflow.analysis.runner import replicate

    if seed is None:
        seed = secrets.randbelow(2**32)
    try:
        result = replicate(
            file,
            n,
            seed=seed,
            until=until,
            measures=measures,
            level=level,
            workers=workers,
            drain=True if drain else None,
        )
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(result.summary_text())
    typer.echo(f"\nSeed {seed}; replication seeds: {', '.join(map(str, result.seeds))}")
    typer.echo(
        f"Repeat one replication with: desflow run {file} --seed <its seed>{options}"
    )

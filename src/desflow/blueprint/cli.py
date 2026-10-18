"""The `desflow` command."""

import logging
import secrets
from pathlib import Path
from typing import Annotated, List, Optional

import typer

from desflow.blueprint.loader import BlueprintError
from desflow.core.context import SimulationContext
from desflow.utils import time_to_seconds

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
def main(
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


@app.command()
def run(
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
            "for more. Defaults to every statistic up to the typical level.",
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
) -> None:
    """Build the simulation in FILE and run it, or run replications of it."""
    stop_at = _parse_until(until)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    if replications is not None:
        # Each replication would log its build and teardown; the sinks still log
        logging.getLogger("desflow.core").setLevel(logging.WARNING)

    try:
        context = SimulationContext.from_yaml(file)
    except BlueprintError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    if seed is not None:
        context.random_seed = seed
    if replications is not None:
        _replicate(
            file, replications, context.random_seed, stop_at, level, measure, workers
        )
        return
    try:
        context.run(until=stop_at)
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user.")


def _replicate(
    file: Path,
    n: int,
    seed: Optional[int],
    until: Optional[float],
    level: float,
    measures: Optional[List[str]],
    workers: int,
) -> None:
    """Runs the replications and prints their summary."""
    from desflow.study.runner import replicate

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
        )
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(result.summary_text())
    typer.echo(f"\nSeed {seed}; replication seeds: {', '.join(map(str, result.seeds))}")
    typer.echo(f"Repeat one replication with: desflow run {file} --seed <its seed>")


@app.command()
def study(
    file: Annotated[
        Path,
        typer.Argument(
            help="The study file.",
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
    """Run the study in FILE, write its artifacts and report, and exit 1 if a check fails."""
    from desflow.study.runner import run_study

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        result = run_study(file, raise_on_failure=False, output=output)
    except (BlueprintError, ValueError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    typer.echo(f"Wrote the artifacts and report.md to {result.output}")
    for row in result.checks:
        state = "passed" if row["passed"] else "FAILED"
        typer.echo(
            f"{state}: {row['design']}: {row['measure']} {row['bound']} "
            f"{row['limit']:g}, checked {row['value']:.4g}"
        )
    if result.failed:
        raise typer.Exit(code=1)


@app.command()
def report(
    folder: Annotated[
        Path,
        typer.Argument(
            help="A study's output folder, with its study.json.",
            metavar="FOLDER",
            exists=True,
            file_okay=False,
        ),
    ],
) -> None:
    """Build the report of a study again from the artifacts in FOLDER."""
    from desflow.study.report import write_report

    try:
        path = write_report(folder)
    except FileNotFoundError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from None
    typer.echo(f"Wrote {path}")


examples_app = typer.Typer(
    help="List the examples, or download one at the version you have installed.",
    invoke_without_command=True,
)
app.add_typer(examples_app, name="examples")


@examples_app.callback()
def examples_list(ctx: typer.Context) -> None:
    """List the examples, with what each shows and what it needs."""
    if ctx.invoked_subcommand is not None:
        return
    from desflow.blueprint.examples import EXAMPLES

    for example in EXAMPLES.values():
        needs = f"odctl up {example.profile}" if example.profile else "no service"
        typer.echo(f"{example.name:<16} {example.shows} ({needs})")
    typer.echo("\nDownload one with: desflow examples get <name>")


@examples_app.command("get")
def examples_get(
    name: Annotated[
        str, typer.Argument(help="The example, as `desflow examples` lists it.")
    ],
    api: Annotated[
        Optional[str],
        typer.Option(help="Only this version: yaml, builder or low-level."),
    ] = None,
    dest: Annotated[Path, typer.Option(help="The folder to download into.")] = Path(
        "."
    ),
    ref: Annotated[
        Optional[str],
        typer.Option(
            help="A git tag or branch. Defaults to the installed release's tag."
        ),
    ] = None,
    force: Annotated[
        bool, typer.Option(help="Replace files that are already there.")
    ] = False,
) -> None:
    """Download an example into <dest>/<name>/<api>/, at the installed release."""
    from desflow import __version__
    from desflow.blueprint.examples import (
        APIS,
        EXAMPLES,
        download,
        release_ref,
        run_hint,
    )

    example = EXAMPLES.get(name)
    if example is None:
        raise typer.BadParameter(
            f"no example called '{name}'; choose one of {', '.join(EXAMPLES)}"
        )
    if api is not None and api not in APIS:
        raise typer.BadParameter(f"--api must be one of {', '.join(APIS)}")
    apis = [api] if api else list(APIS)
    ref = ref or release_ref(__version__)
    try:
        written = download(example, apis, dest, ref, force=force)
    except (FileExistsError, RuntimeError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from None
    for path in written:
        typer.echo(f"wrote {path}")
    typer.echo("")
    if example.extras:
        typer.echo(f'Install: pip install "desflow[{example.extras}]"')
    if example.profile:
        typer.echo(f"Start its service: odctl up {example.profile}")
    for each in apis:
        typer.echo(f"Run the {each} version: {run_hint(example, each)}")
    for note in example.notes:
        typer.echo(note)


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

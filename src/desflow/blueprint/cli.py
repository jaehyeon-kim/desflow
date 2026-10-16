"""The `desflow` command."""

import logging
from pathlib import Path
from typing import Annotated, Optional

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
) -> None:
    """Build the simulation in FILE and run it."""
    stop_at = _parse_until(until)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        context = SimulationContext.from_yaml(file)
    except BlueprintError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1) from None

    if seed is not None:
        context.random_seed = seed
    try:
        context.run(until=stop_at)
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user.")


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

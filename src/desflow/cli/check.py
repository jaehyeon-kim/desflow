"""`desflow check`: validates blueprints as `desflow run` would build them."""

import logging
from pathlib import Path
from typing import Annotated, List, Optional

import typer

from desflow.cli._common import parse_variables, setup_logging

# The examples --help prints after the options, as odctl's help does
_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# Check one blueprint without running it[/dim]\n
  $ [bold cyan]desflow check shop.yaml[/bold cyan]\n\n
  [dim]# Check every blueprint in a folder, as a pre-commit hook does[/dim]\n
  $ [bold cyan]desflow check examples/*.yaml[/bold cyan]
"""


def register(app: typer.Typer) -> None:
    """Adds the `check` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("check", rich_help_panel="Run and Check", epilog=_EPILOG)(_check)


# Each command is private: typer shows its docstring as the help text, so it
# carries no Args section, and register() gives the command its name
def _check(
    files: Annotated[
        List[Path],
        typer.Argument(
            help="The blueprint files.",
            metavar="FILE...",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    var: Annotated[
        Optional[List[str]],
        typer.Option(
            "--var",
            metavar="NAME=VALUE",
            help="Fill ${NAME} in the files and in the files they include, before "
            "the environment does. Repeat it for more.",
        ),
    ] = None,
) -> None:
    """Check that each blueprint in FILE... builds, without running it.

    It reports what `desflow run` reports before the clock starts: YAML errors,
    unknown keys, missing names, unset variables, `!python` references that cannot be
    imported, and connectors whose extra is missing or whose settings are refused. It
    also reports a Parquet or Jsonl entry when PyArrow is not installed. It does not
    call `run.before`, contact a broker, a database or a catalog, or run a `!python`
    function. Importing a `!python` module runs its top-level code, as `desflow run`
    does. Prints `ok FILE (sim_id ID)` for each valid file and every error of an
    invalid one, and exits 1 if any file is invalid.
    """
    from desflow.blueprint.build import check
    from desflow.blueprint.loader import BlueprintError

    variables = parse_variables(var)
    # A warning while building, such as an on: path that does not exist, is shown
    setup_logging(logging.WARNING)
    failed = False
    for file in files:
        if file.suffix == ".py":
            typer.echo(
                f"Error: {file}: desflow check takes a YAML blueprint, and a Python "
                f"file defines its model in code",
                err=True,
            )
            failed = True
            continue
        try:
            context = check(file, variables)
        except (BlueprintError, ValueError, ImportError) as exc:
            typer.echo(f"Error: {exc}", err=True)
            failed = True
            continue
        typer.echo(f"ok {file} (sim_id {context.sim_id})")
    if failed:
        raise typer.Exit(code=1)

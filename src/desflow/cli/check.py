"""`desflow check`: validates blueprints as `desflow run` would build them."""

import logging
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, List, Mapping, NamedTuple, Optional

import typer

from desflow.cli._common import parse_variables, setup_logging

if TYPE_CHECKING:
    from desflow.builder.context import SimulationContext

# The examples --help prints after the options, as odctl's help does
_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# Check one blueprint without running it[/dim]\n
  $ [bold cyan]desflow check shop.yaml[/bold cyan]\n\n
  [dim]# Check every blueprint in a folder, as a pre-commit hook does[/dim]\n
  $ [bold cyan]desflow check examples/*.yaml[/bold cyan]\n\n
  [dim]# Check a builder file, which defines its model in Python[/dim]\n
  $ [bold cyan]desflow check shop.py[/bold cyan]
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
            help="The blueprint files, or builder files.",
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
    """Check that each blueprint or builder file in FILE... builds, without running it.

    It reports what `desflow run` reports before the clock starts: YAML errors,
    unknown keys, missing names, unset variables, `!python` references that cannot
    be imported, and connectors whose extra is missing or whose settings are
    refused. A Parquet, Jsonl or Iceberg connector, in a blueprint or a builder
    file, is refused when its extra is not installed. It also reports a `tests`
    entry with no seed in a file with none, as `desflow test` does. It warns about a
    run with no end: an arrival with a `dist` or a `gap` and no `max_events`, in a
    file with no `run.until`, `run.stop` or `run.segments`. It does not call
    `run.before`, contact a broker, a database or a catalog, or run a `!python`
    function. Importing a `!python` module runs its top-level code, as `desflow run`
    does. A builder file, a `.py` file with a module-level `SimulationContext`, is
    imported as `desflow test` imports it, so an import whose extra is missing is
    reported; its `if __name__ == "__main__":` block does not run. An error or a
    `sys.exit` of the file's code is one line, with the file, its line and the
    error's type. Prints `ok FILE (sim_id ID)` for each valid file and every error
    of an invalid one, and exits 1 if any file is invalid.
    """
    variables = parse_variables(var)
    # A warning while building, such as an on: path that does not exist, is shown
    setup_logging(logging.WARNING)
    failed = False
    for file in files:
        checked = check_file(file, variables)
        if checked.context is None:
            typer.echo(f"Error: {checked.problem}", err=True)
            failed = True
            continue
        typer.echo(f"ok {file} (sim_id {checked.context.sim_id})")
    if failed:
        raise typer.Exit(code=1)


class FileCheck(NamedTuple):
    """What `desflow check` found in one file.

    Attributes:
        context: The built context, or None when the file was refused.
        problem: Why the file was refused, as one message, or None.
        no_context: True when the file is a Python file refused only because it
            defines no module-level `SimulationContext`, as a Low-level file does.
    """

    context: Optional["SimulationContext"]
    problem: Optional[str]
    no_context: bool = False


def check_file(file: Path, variables: Optional[Mapping[str, str]] = None) -> FileCheck:
    """Checks one blueprint or builder file as `desflow check` does.

    Args:
        file: A YAML blueprint, or a Python file with a module-level context.
        variables: Values for `${NAME}` in a blueprint, as `--var` gives them.

    Returns:
        FileCheck: The built context, or why the file was refused.

    Raises:
        Exception: An error of a YAML file other than a `BlueprintError`, an
            `ImportError` or a `ValueError`.
    """
    from desflow.blueprint.build import check
    from desflow.blueprint.loader import BlueprintError
    from desflow.loading import NoContextError, is_builder_file, load_context

    try:
        if is_builder_file(file):
            context = load_context(file.parent, file.name, variables=variables)
        else:
            context = check(file, variables)
    except BlueprintError as exc:
        return FileCheck(None, str(exc))
    except NoContextError as exc:
        return FileCheck(None, str(exc), no_context=True)
    except (Exception, SystemExit) as exc:
        if not is_builder_file(file) and not isinstance(exc, (ImportError, ValueError)):
            raise
        # A builder file runs code of its own, so any error or exit is the file's
        return FileCheck(None, _error_place(file, exc))
    return FileCheck(context, None)


def _error_place(file: Path, exc: BaseException) -> str:
    """An error of a builder file as one line: the file and the line of the file
    where it was raised, its type and its message. An error raised in a module of
    the file's own folder also names that module's file and line. An error that
    desflow raised about the file, from no line of it, is the file and the message.
    An error of a YAML file keeps its own message, which names the file or the
    reference."""
    from desflow.loading import is_builder_file

    if not is_builder_file(file):
        return str(exc)
    line = None
    target = file.resolve()
    frames = traceback.extract_tb(exc.__traceback__)
    for frame in frames:
        if Path(frame.filename).resolve() == target:
            line = frame.lineno
    if line is None:
        message = str(exc)
        named = message.split(" ", 1)[0].endswith(file.name)
        return message if named else f"{file}: {message}"
    kind = type(exc).__name__
    raised = Path(frames[-1].filename).resolve()
    if raised != target and raised.parent == target.parent:
        # Raised in a module of the file's own folder, which the file imports
        kind += f" in {raised.name}:{frames[-1].lineno}"
    return f"{file}:{line}: {kind}: {exc}"

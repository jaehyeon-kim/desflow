"""What more than one `desflow` command uses: logging, error exits and `--until`."""

import contextlib
import logging
import re
from typing import Dict, Iterator, List, NoReturn, Optional

import typer

from desflow.utils import checked_seconds, time_to_seconds

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT = "%H:%M:%S"


def setup_logging(level: int) -> None:
    """Logs to stderr at `level` in the format every command uses."""
    logging.basicConfig(level=level, format=LOG_FORMAT, datefmt=LOG_DATEFMT)


def refuse(message: str) -> NoReturn:
    """Prints `Error: <message>` on stderr and exits 2, for a usage error."""
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=2)


def fail(message: str) -> NoReturn:
    """Prints `Error: <message>` on stderr and exits 1, for a failure such as an
    invalid blueprint."""
    typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=1)


def parse_until(value: Optional[str], *, ends: bool = False) -> Optional[float]:
    """`--until` in seconds: a number, or a time such as `10 min`.

    Infinity is no end, and is refused with `ends`, for runs that are not paced and
    must end: replications and `--verify`.
    """
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            seconds = time_to_seconds(value)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from None
    try:
        return float(checked_seconds(seconds, "--until", ends=ends))
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None


_VARIABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def parse_variables(values: Optional[List[str]]) -> Dict[str, str]:
    """`--var NAME=VALUE` options as a mapping; the last of a repeated name wins.

    Args:
        values: The options as typed, such as `["PACKERS=3", "TOPIC=orders"]`.

    Returns:
        Dict[str, str]: The text of each value, which the blueprint reads as it
            reads an environment variable.

    Raises:
        typer.BadParameter: If an option has no `=`, or its name is not one a
            `${NAME}` placeholder can have.
    """
    found: Dict[str, str] = {}
    for item in values or []:
        name, separator, value = item.partition("=")
        if not separator or not _VARIABLE_NAME.fullmatch(name):
            raise typer.BadParameter(
                f"{item!r} is not NAME=VALUE; the name has letters, digits and "
                f"underscores, and does not start with a digit",
                param_hint="--var",
            )
        found[name] = value
    return found


@contextlib.contextmanager
def quiet_builds() -> Iterator[None]:
    """Shows the warnings of a blueprint build once: inside the block, each further
    build of the same file logs only errors. `desflow run --replications` and
    `desflow test` build the file once for each run, and the warnings of the first
    build are the same."""
    builds = logging.getLogger("desflow.blueprint.build")
    before = builds.level
    builds.setLevel(logging.ERROR)
    try:
        yield
    finally:
        builds.setLevel(before)

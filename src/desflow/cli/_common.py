"""What more than one `desflow` command uses: logging, error exits and `--until`."""

import logging
from typing import NoReturn, Optional

import typer

from desflow.utils import time_to_seconds

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


def parse_until(value: Optional[str]) -> Optional[float]:
    """`--until` in seconds: a number, or a time such as `10 min`."""
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

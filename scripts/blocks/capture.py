"""Running a snippet or an example and keeping what it prints or logs."""

import ast
import contextlib
import importlib.util
import logging
import os
import re
import sys
from pathlib import Path
from typing import Callable, List

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"


_TIMESTAMP = re.compile(r"'timestamp': '[^']*'")


class _Capture(logging.Handler):
    """Keeps the lines ConsoleEgress logs, in order."""

    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.lines: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        # ConsoleEgress logs a batch in one call, one line per record
        for line in record.getMessage().split("\n"):
            if line[:6] in ("[EVT] ", "[TEL] ", "[LBL] "):
                self.lines.append(_TIMESTAMP.sub("'timestamp': '...'", line))


def capture(run: Callable[[], object]) -> List[str]:
    """Runs `run` and returns the records ConsoleEgress printed.

    Args:
        run: The function that runs a simulation.

    Returns:
        List[str]: The console lines, one per record.
    """
    handler = _Capture()
    logger = logging.getLogger("desflow.connectors.egress.local")
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        run()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
    return handler.lines


def yaml_records(path: str, until: object = None) -> List[str]:
    """The records of a blueprint, run unpaced.

    Args:
        path: The blueprint, relative to the repository.
        until: The run's end, as `run(until=...)` takes it; the blueprint's own when None.

    Returns:
        List[str]: The console lines, one per record.
    """
    from desflow import SimulationContext

    def run() -> None:
        app = SimulationContext.from_yaml(ROOT / path)
        app.factor = 0.0
        app.run(until=until)

    return capture(run)


def load_script(path: str):
    """Imports a script by its path, with its folder on the import path.

    Args:
        path: The script, relative to the repository.

    Returns:
        The imported module.
    """
    file = ROOT / path
    sys.path.insert(0, str(file.parent))
    try:
        spec = importlib.util.spec_from_file_location(file.stem, file)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(file.parent))


def fields(line: str) -> dict:
    """The record a console line prints, as a dict.

    Args:
        line: A console line, such as `[EVT] {...}`.

    Returns:
        dict: The record.
    """
    return ast.literal_eval(line[6:])


def records_until(lines: List[str], end: float) -> List[str]:
    """The lines whose record has a simulated time up to `end`.

    Args:
        lines: Console lines.
        end: The last simulated time to keep.

    Returns:
        List[str]: The lines kept, in order.
    """
    return [line for line in lines if fields(line)["sim_ts"] <= end]


def event_lines(lines: List[str]) -> List[str]:
    """The event lines among console lines.

    Args:
        lines: Console lines.

    Returns:
        List[str]: The lines that start with `[EVT] `.
    """
    return [line for line in lines if line.startswith("[EVT] ")]


@contextlib.contextmanager
def cwd(path: Path):
    """Runs the block in `path`, then returns to the folder before it.

    Args:
        path: The folder to work in.
    """
    before = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(before)


def stat_lines(lines: List[str]) -> List[str]:
    """The telemetry lines of statistics among console lines.

    Args:
        lines: Console lines.

    Returns:
        List[str]: The `[TEL] ` lines whose stream has `.stats.`.
    """
    return [line for line in lines if ".stats." in line and line.startswith("[TEL] ")]


def logged_lines(logger_name: str, run: Callable[[], object]) -> List[str]:
    """The messages one logger writes while `run` runs.

    Args:
        logger_name: The logger's name.
        run: The function to run.

    Returns:
        List[str]: The messages, in order.
    """
    lines: List[str] = []

    class _Keep(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            lines.append(record.getMessage())

    handler = _Keep(logging.INFO)
    logger = logging.getLogger(logger_name)
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        run()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
    return lines


def printed_lines(run: Callable[[], object]) -> List[str]:
    """What `run` prints.

    Args:
        run: The function to run.

    Returns:
        List[str]: The printed lines.
    """
    import io

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        run()
    return out.getvalue().rstrip("\n").split("\n")


def script_output(path: str) -> List[str]:
    """What a snippet prints when it runs as a script.

    Args:
        path: The snippet, relative to the repository.

    Returns:
        List[str]: The printed lines.
    """
    import runpy

    return printed_lines(lambda: runpy.run_path(str(ROOT / path), run_name="__main__"))

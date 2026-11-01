"""`desflow run`: runs a blueprint, or replications of it."""

import functools
import logging
import multiprocessing
import os
import secrets
import shlex
import signal
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

import typer

from desflow.cli._common import (
    LOG_DATEFMT,
    LOG_FORMAT,
    fail,
    parse_until,
    parse_variables,
    quiet_builds,
    refuse,
    setup_logging,
)

logger = logging.getLogger("desflow.cli")


# The examples --help prints after the options, as odctl's help does
_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# Run a blueprint, sending its records to its sinks[/dim]\n
  $ [bold cyan]desflow run shop.yaml[/bold cyan]\n\n
  [dim]# Print the first five events, with no services[/dim]\n
  $ [bold cyan]desflow run shop.yaml --sample 5[/bold cyan]\n\n
  [dim]# Print every record as a JSON line instead of sending it[/dim]\n
  $ [bold cyan]desflow run shop.yaml --stdout[/bold cyan]\n\n
  [dim]# Run 20 replications and print the mean and interval of each statistic[/dim]\n
  $ [bold cyan]desflow run shop.yaml --replications 20 --seed 7[/bold cyan]\n\n
  [dim]# Fill ${PACKERS} in the file, and run again whenever the file changes[/dim]\n
  $ [bold cyan]desflow run shop.yaml --var PACKERS=3 --sample 5 --watch[/bold cyan]
"""


def register(app: typer.Typer) -> None:
    """Adds the `run` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("run", rich_help_panel="Run and Check", epilog=_EPILOG)(_run)


_RECORDS = "desflow.connectors.egress.local"


class _RecordFormatter(logging.Formatter):
    """Prints a Console sink's records as they are, and every other line with the format.

    The sink logs a batch of records in one call, so the format's time, level and name
    would start only the first record of each batch.
    """

    def format(self, record: logging.LogRecord) -> str:
        """The record's message alone for the Console sink, else the formatted line.

        Args:
            record: The log record.

        Returns:
            str: The line to print.
        """
        if record.name == _RECORDS:
            return record.getMessage()
        return super().format(record)


@dataclass(frozen=True)
class _RunOptions:
    """What `desflow run` was given besides the file. A `--watch` child process gets
    it, so it is a plain dataclass that can be sent to another process."""

    until: Optional[str] = None
    seed: Optional[int] = None
    drain: bool = False
    replications: Optional[int] = None
    level: float = 0.95
    measure: Optional[List[str]] = None
    workers: int = 1
    sample: Optional[int] = None
    stdout: bool = False
    variables: Dict[str, str] = field(default_factory=dict)


def _run_options(options: _RunOptions) -> str:
    """The options given that change the run, as a repeat command needs them."""
    text = f" --until {shlex.quote(options.until)}" if options.until is not None else ""
    text += " --drain" if options.drain else ""
    for name, value in options.variables.items():
        text += f" --var {shlex.quote(f'{name}={value}')}"
    return text


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
    var: Annotated[
        Optional[List[str]],
        typer.Option(
            "--var",
            metavar="NAME=VALUE",
            help="Fill ${NAME} in the file and in the files it includes, before the "
            "environment does. Repeat it for more.",
        ),
    ] = None,
    watch: Annotated[
        bool,
        typer.Option(
            "--watch",
            help="Run in a child process, and run again whenever the file or a .py "
            "file beside it changes. Needs the watch extra.",
        ),
    ] = False,
) -> None:
    """Build the simulation in FILE and run it, or run replications of it."""
    parse_until(until)
    options = _RunOptions(
        until,
        seed,
        drain,
        replications,
        level,
        measure,
        workers,
        sample,
        stdout,
        parse_variables(var),
    )
    if (sample is not None or stdout) and replications is not None:
        refuse("--sample and --stdout print one run; leave out --replications")
    if file.suffix == ".py":
        refuse("desflow run takes a YAML blueprint; run a Python file with python FILE")
    if watch:
        _watch(file, options)
    else:
        _run_once(str(file), options)


def _run_once(path: str, options: _RunOptions) -> None:
    """Builds the blueprint and runs it once, or runs its replications.

    A `--watch` child runs this for each pass. A failure exits with `fail` or
    `refuse`, which raise `typer.Exit`.

    Args:
        path: The blueprint file.
        options: What `desflow run` was given besides the file.
    """
    from desflow.blueprint.build import Preview
    from desflow.blueprint.loader import BlueprintError
    from desflow.loading import load_context

    file = Path(path)
    until, seed, drain = options.until, options.seed, options.drain
    replications, sample = options.replications, options.sample
    stop_at = parse_until(until)
    preview = Preview(sample) if sample is not None or options.stdout else None

    root = logging.getLogger()
    before = list(root.handlers)
    setup_logging(logging.INFO)
    # Only the handler basicConfig added, so a handler set up by the caller is kept
    for handler in root.handlers:
        if handler not in before:
            handler.setFormatter(_RecordFormatter(LOG_FORMAT, LOG_DATEFMT))
    if replications is not None:
        # Each replication would log its build and teardown; the sinks still log
        logging.getLogger("desflow.core").setLevel(logging.WARNING)

    try:
        context = load_context(file.parent, file.name, preview, options.variables)
    except BlueprintError as exc:
        fail(str(exc))

    if seed is not None:
        context.random_seed = seed
    if context._repeat_hint is not None:
        # The run's seed is filled in later, so braces in the until are doubled
        repeat = _run_options(options)
        context._repeat_hint += repeat.replace("{", "{{").replace("}", "}}")
    if replications is not None:
        with quiet_builds():
            _replicate(file, replications, context, stop_at, options)
        return
    try:
        context.start(until=stop_at, drain=True if drain else None)
    except ValueError as exc:
        # A run value refused before the run starts, such as --drain without an
        # until, is reported as a blueprint error is; an error inside the run keeps
        # its traceback
        fail(str(exc))
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
    context: Any,
    until: Optional[float],
    options: _RunOptions,
) -> None:
    """Runs the replications and prints their summary; `--drain` drains each.

    Args:
        file: The blueprint file.
        n: How many replications.
        context: The context built from the file, which gives the seed and the
            `run.until` the replications take when none is given.
        until: The end time, or None for the file's `run.until`.
        options: What `desflow run` was given besides the file.
    """
    from desflow.analysis.runner import replicate
    from desflow.core.context import SimulationContext

    seed = context.random_seed
    if seed is None:
        seed = secrets.randbelow(2**32)
    model: Any = file
    if options.variables:
        # Each replication builds the file again, with the same variables
        model = functools.partial(
            SimulationContext.from_yaml, file, variables=options.variables
        )
        if until is None:
            until = context._default_until
    try:
        result = replicate(
            model,
            n,
            seed=seed,
            until=until,
            measures=options.measure,
            level=options.level,
            workers=options.workers,
            drain=True if options.drain else None,
        )
    except ValueError as exc:
        fail(str(exc))
    typer.echo(result.summary_text())
    typer.echo(f"\nSeed {seed}; replication seeds: {', '.join(map(str, result.seeds))}")
    typer.echo(
        f"Repeat one replication with: desflow run {file} --seed <its seed>"
        f"{_run_options(options)}"
    )


# How long a stopped run is given to flush its sinks: the default drain_stall_seconds
# of 30 plus 5
_STOP_WAIT = 35
_KILL_WAIT = 5


class _BlueprintFilter:
    """Which changes under the blueprint's folder start a new run: the file itself,
    and a `.py` file below its folder. Output a run writes beside the file, such as
    `data/events.parquet`, and the folders `watchfiles` ignores, such as `.venv` and
    `__pycache__`, start nothing.

    It is a `watchfiles` filter: called with a change and a path, it returns whether
    the change counts.

    Args:
        file: The blueprint file.
    """

    def __init__(self, file: Path):
        from watchfiles import DefaultFilter

        self._default = DefaultFilter()
        self._file = file.resolve()
        self._folder = self._file.parent

    def __call__(self, change: Any, path: str) -> bool:
        if not self._default(change, path):
            return False
        changed = Path(path).resolve()
        return changed == self._file or (
            changed.suffix == ".py" and self._folder in changed.parents
        )


def _child(path: str, options: _RunOptions) -> None:
    """The process target of a `--watch` pass: `_run_once`, whose errors it has
    already printed, and a Ctrl+C before the run starts as a quiet stop."""
    try:
        _run_once(path, options)
    except (typer.Exit, KeyboardInterrupt):
        pass


def _start(context: Any, file: Path, options: _RunOptions) -> Any:
    """Starts one pass of the file in a new process and returns the process."""
    child = context.Process(target=_child, args=(str(file), options))
    child.start()
    return child


def _stop(child: Any, interrupt: bool) -> None:
    """Waits for a pass to end, stopping it first when `interrupt` is true.

    A stop is SIGINT, which the run handles as Ctrl+C: it tears down and flushes its
    sinks. Native Windows has no such signal for another process, so the process is
    terminated there. A pass that has not ended after `_STOP_WAIT` seconds is killed,
    and stderr says its unwritten records are lost.

    Args:
        child: The process of the pass.
        interrupt: True to send the stop; False when Ctrl+C has already reached it.
    """
    if child.is_alive() and interrupt:
        if sys.platform == "win32":
            child.terminate()
        else:
            os.kill(child.pid, signal.SIGINT)
    child.join(_STOP_WAIT)
    if child.is_alive():
        typer.echo(
            f"The run did not stop in {_STOP_WAIT} seconds, so it was killed. Records "
            f"it had not written are lost.",
            err=True,
        )
        child.kill()
        child.join(_KILL_WAIT)


def _watch(file: Path, options: _RunOptions) -> None:
    """Runs the file in a child process, and again each time `_BlueprintFilter`
    accepts a change. Ctrl+C ends the watch with exit code 0 once the run has stopped.

    A new process for each pass starts from a fresh interpreter, so an edited `!python`
    module is imported again and a sink that is stuck cannot keep the next run alive.

    Args:
        file: The blueprint file.
        options: What `desflow run` was given besides the file.
    """
    try:
        from watchfiles import watch
    except ImportError:
        fail('--watch needs watchfiles. Install it with pip install "desflow[watch]"')

    spawn = multiprocessing.get_context("spawn")
    child = _start(spawn, file, options)
    try:
        for changes in watch(
            file.parent, watch_filter=_BlueprintFilter(file), raise_interrupt=False
        ):
            names = ", ".join(sorted({Path(path).name for _, path in changes}))
            typer.echo(f"Changed: {names}. Running again.", err=True)
            _stop(child, interrupt=True)
            child = _start(spawn, file, options)
    except KeyboardInterrupt:
        # Ctrl+C while a pass is being stopped or started: the watch ends with 0
        pass
    finally:
        # Ctrl+C has already reached the child
        _stop(child, interrupt=False)

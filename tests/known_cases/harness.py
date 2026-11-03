"""Shared code of the known-case test: runs the three forms of a case and its original.

A case folder `tests/known_cases/<case-id>/` holds `model.yaml` (the YAML form),
`builder.py` (`build(...)` returns the model and `run(app)` runs it), `low_level.py`
(`build(env, ...)` starts the processes on a `DynamicRealtimeEnvironment`, and `UNTIL`
is the end), and `original.py` (the plain SimPy program, `program()` returns the log
and the error text) or `expected.json`.

The forms are compared as the known-case tests of the batches compare them: the
records a model publishes as the program logs them, (time, key, task, value). The
engine's own `queued`, `started` and `resumed` records are left out unless the
program logs them, because only then does the original show the moment of a grant.
"""

import ast
import importlib
import inspect
import re
import shutil
import sys
import types
from pathlib import Path
from typing import Any

from desflow.blueprint.loader import BlueprintError
from desflow.core.context import SimulationContext
from tests.conformance.harness import Outcome, RecordingEgress
from tests.conformance.harness import run_low_level as low_level_run

FOLDER = Path(__file__).parent
SIZES = [1, 3, 10]
TOPICS = [
    "Environment and Time",
    "Events and Signals",
    "Processes and Tasks",
    "Conditions and Waits",
    "Interrupts and Failures",
    "Resources",
    "Containers",
    "Stores",
    "Real Time",
    "Generated Values",
    "Related Records",
    "Arrivals and Sources",
    "Statistics and Monitoring",
    "Sinks and Connectors",
]
# Fields of a record that hold the wall clock, which differs from one run to the next
WALL_FIELDS: tuple[str, ...] = ("timestamp",)
WALL_PATHS: tuple[str, ...] = (
    "system.simulation.lag_seconds",
)  # the wall-clock lag of a paced run
_KEEP_STATUS = (
    "interrupted",
    "preempted",
    "reneged",
    "balked",
    "failed",
    "down",
    "repaired",
)


def folder_of(case: str) -> str:
    """The folder name of a case id: lower case, with `-` as `_`."""
    return case.lower().replace("-", "_")


def form(case: str, name: str) -> Any:
    """The module `builder`, `low_level` or `original` of a case."""
    return importlib.import_module(f"tests.known_cases.{folder_of(case)}.{name}")


SHARED = "the program names an entity or a process by a fixed key, or its entities share one event or flag, so another size is another scenario"
SIGNAL = "each entity fires the one signal, where the program makes one event for each process"
NOT_SIZED = {
    "ST-P06": "the program joins one process by a fixed name",
    "ST-T05": SIGNAL,
    "ST-V01": SIGNAL,
    "ST-I06": "the entities interrupt one shared target",
    "EX-5": SHARED,
    "GD-P3": SHARED,
    "ST-I05": SHARED,
    "ST-I10": SHARED,
    "ST-P02": SHARED,
    "ST-U05": SIGNAL,
    "ST-RT1": "the program starts two entities with two sleep times, so it has one size",
    "GD-M1": "the program starts the one process `a-1` and runs until it ends",
    "GD-R1": "the program starts the one process `a-1`, times it on a paced and on a plain environment and runs until it ends",
    "GD-S3": "the program interrupts the one process `v-1` by name",
    "GD-TT1": "the program has one parent, one child and one interruptor, each named",
    "GD-TT4": "the program has one worker and one interruptor, each named",
    "GD-TT6": "a process that raises ends the run, so the case is one process",
    "GD-TT7": "the program loops in one process until the time is 2",
    "P75": "the program interrupts the one process and reads the time it has left",
    "ST-R06": "two children with fixed names ask for the one unit in a fixed order",
    "ST-R21": "the case is three puts into a store of capacity 2: the third waits",
    "ST-R23": "the case is seven puts into a priority store of capacity 3 and three getters",
    "ST-R24": "the case is three items of equal priority and the order they come out in",
    "ST-RT6": "a paced run: the entities share the wall-clock delay, so a larger size does not finish in the program's time",
}


def needs_size(case: str) -> bool:
    """Whether the case's document has an arrival that starts a number of entities at
    once (`initial: <n>`), which the test runs at other sizes."""
    text = (FOLDER / folder_of(case) / "model.yaml").read_text(encoding="utf-8")
    return re.search(r"initial: \d+", text) is not None


def is_sized(case: str) -> bool:
    """Whether the case has an arrival that starts a number of entities at once, which
    the test runs at sizes 1, 3 and 10: its Builder `build` takes a `size`."""
    if case in NOT_SIZED:
        return False
    try:
        build = form(case, "builder").build
    except (
        ImportError,
        AttributeError,
    ):  # a form that is not written yet fails its own test
        return False
    return "size" in inspect.signature(build).parameters


def reading_series(
    records: list[dict[str, Any]], resource: str = "res"
) -> list[tuple[Any, ...]]:
    """The `in_use` and `queue_length` readings of a resource: (time, field, value)."""
    found = []
    for record in records:
        if record["stream_type"] != "telemetry":
            continue
        for field in ("in_use", "queue_length"):
            if str(record["path_id"]).endswith(f".resources.{resource}.{field}"):
                found.append((round(record["sim_ts"], 3), field, record["value"]))
    return found


def calls_as_readings(calls: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    """What a reading after each `request` and `release` call gives: the first call of
    an instant gives both values, and each later call only the one that changed."""
    found: list[tuple[Any, ...]] = []
    last, previous = None, None
    for at, count, queued in calls:
        at = round(at, 3)
        if at != last:
            previous, last = None, at
        if previous is None or previous[0] != count:
            found.append((at, "in_use", count))
        if previous is None or previous[1] != queued:
            found.append((at, "queue_length", queued))
        previous = (count, queued)
    return found


def trace_series(records: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    """The trace records of a run as (time, priority, event class), with the plain
    `Event` left out: the arrival process is the one extra process a form starts."""
    return [
        (round(r["sim_ts"], 3), r["value"]["priority"], r["value"]["event"])
        for r in records
        if r["stream_type"] == "trace" and r["value"]["event"] != "Event"
    ]


def is_paced(case: str) -> bool:
    """Whether the case's document runs against the wall clock (`factor` above 0), so
    CPU time taken by other tests in the same run can make it late."""
    text = (FOLDER / folder_of(case) / "model.yaml").read_text(encoding="utf-8")
    return re.search(r"factor: (0\.\d*[1-9]|[1-9])", text) is not None


def has_draws(case: str) -> bool:
    """Whether the case's original fixes random draws that its forms make."""
    path = FOLDER / folder_of(case) / "original.py"
    return path.exists() and bool(getattr(form(case, "original"), "DRAWS", []))


def normal(
    records: list[dict[str, Any]], grants: bool = False
) -> list[tuple[Any, ...]]:
    """The event records of a run as an original logs them: (time, key, task, value).

    A record that keeps a status loses `path_id`, which no original has. The engine's
    own `started`, `queued` and `resumed` records are left out unless `grants`.
    """
    found = []
    for record in records:
        if record["stream_type"] != "event":
            continue
        value = record["value"]
        if isinstance(value, dict) and "status" in value:
            if value["status"] in _KEEP_STATUS:
                value = {k: v for k, v in value.items() if k != "path_id"}
            elif set(value) <= {"status", "path_id", "reason"}:
                if not grants:
                    continue
                value = {k: v for k, v in value.items() if k != "path_id"}
        found.append((round(record["sim_ts"], 3), record["key"], record["task"], value))
    return found


def _outcome(app: SimulationContext, run: Any) -> Outcome:
    recording = RecordingEgress()
    app.add_egress(recording)
    traced = RecordingEgress()
    if app._stats_trace:  # a sink for the trace records, as the program's trace needs
        app.add_egress(traced, when="trace")
    error: BaseException | None = None
    value: Any = None
    try:
        value = run(app)
    except Exception as exc:  # a run failure is a result of the case
        error = exc
    outcome = Outcome(recording.records, value, error, app)
    outcome.trace = traced.records  # type: ignore[attr-defined]
    return outcome


def run_yaml(
    case: str, tmp_path: Path, size: int | None = None, fixed: bool = False
) -> Outcome:
    """Runs the case's `model.yaml` from a copy of its folder, with `size` entities in
    the first arrival that starts a number of them at once. With `fixed`, the random
    draws are replaced by the values `DRAWS` of `original.py` names, which the original
    program uses as well."""
    source = FOLDER / folder_of(case)
    work = tmp_path / ("fixed" if fixed else "yaml")
    work.mkdir()
    for path in source.iterdir():
        if path.suffix in (".csv", ".json", ".txt"):
            shutil.copy(path, work / path.name)
    text = (source / "model.yaml").read_text(encoding="utf-8")
    if fixed and (source / "original.py").exists():
        for old, new in getattr(form(case, "original"), "DRAWS", []):
            assert old in text, f"{case}: the text {old!r} is not in model.yaml"
            text = text.replace(old, new, 1)
    if size is not None:
        text = re.sub(r"initial: \d+", f"initial: {size}", text, count=1)
    (work / "model.yaml").write_text(text, encoding="utf-8")
    try:
        app = SimulationContext.from_yaml(work / "model.yaml")
    except BlueprintError as exc:  # a document the load refuses: the case's result
        return Outcome(
            records=[],
            value=None,
            error=exc,
            app_or_env=None,  # type: ignore[arg-type]
        )
    return _outcome(app, lambda app: app.run())


def run_builder(case: str, size: int | None = None) -> Outcome:
    """Runs the case's `builder.py`."""
    module = form(case, "builder")
    app = module.build() if size is None else module.build(size)
    return _outcome(app, module.run)


def run_low_level(case: str, size: int | None = None) -> Outcome:
    """Runs the case's `low_level.py` on a `DynamicRealtimeEnvironment` that publishes
    to a recording egress."""
    module = form(case, "low_level")

    until = [module.UNTIL]
    driver: list[Any] = []
    environment: list[Any] = []

    def start(env: Any) -> None:
        environment.append(env)
        made = module.build(env) if size is None else module.build(env, size)
        if callable(made):  # a form that drives the environment itself
            driver.append(made)
        elif made is not None:  # the run ends with this process or event
            until[0] = made

    result = low_level_run(
        getattr(module, "PARAMS", None),
        start,
        lambda: until[0],
        stats=getattr(module, "STATS", None),
        initial_time=getattr(module, "INITIAL_TIME", 0.0),
        factor=getattr(module, "FACTOR", 0.0),
        strict=getattr(module, "STRICT", False),
        raises=True,
        drive=lambda: driver[0]() if driver else environment[0].run(until=until[0]),
        trace=getattr(module, "TRACE", False),
    )
    assert isinstance(result, Outcome)
    return result


def original_program(case: str, size: int | None) -> Any:
    """The case's program from `original.py`. At a size the checks of the asserts are
    removed, because they state the original's own scenario and another size is
    another one; the tests are still evaluated."""
    module = form(case, "original")
    function = module.program
    if size is None:
        return function
    home = sys.modules[function.__module__]
    path = Path(inspect.getsourcefile(home) or "")

    class _NoAssert(ast.NodeTransformer):
        def visit_Assert(self, node: ast.Assert) -> Any:
            # the test stays as an expression statement, so a call inside it (an
            # `env.run`) still runs and only the check goes
            return ast.copy_location(ast.Expr(node.test), node)

    tree = ast.fix_missing_locations(
        _NoAssert().visit(ast.parse(path.read_text(encoding="utf-8")))
    )
    plain = types.ModuleType(f"{home.__name__}_without_asserts")
    plain.__file__ = str(path)
    exec(compile(tree, str(path), "exec"), plain.__dict__)  # noqa: S102
    for name in ("PROCS", "MODE", "PENDING"):  # a module that holds its own harness
        if name in plain.__dict__ and name in home.__dict__:
            setattr(plain, name, getattr(home, name))
    stripped = getattr(plain, function.__name__)
    for name in ("ties", "grants", "sizes"):
        if hasattr(function, name):
            setattr(stripped, name, getattr(function, name))
    return stripped


def run_original(
    case: str, size: int | None = None
) -> tuple[list[tuple[Any, ...]], str | None, bool]:
    """Runs the case's original: its log with the times rounded to three decimals,
    the text of the error it ended with (None for none) and whether it logs grants."""
    harness = form(case, "original").HARNESS
    function = original_program(case, size)
    ties = bool(getattr(function, "ties", False))
    harness.MODE.update(on=size is not None and not ties, n=size, rev=False)
    try:
        log, error = function(size, False) if ties and size else function()
    finally:
        harness.MODE.update(on=False, n=None, rev=False)
        harness.PENDING.clear()
    return (
        [(round(t, 3), k, task, v) for t, k, task, v in log],
        error,
        bool(getattr(function, "grants", False)),
    )

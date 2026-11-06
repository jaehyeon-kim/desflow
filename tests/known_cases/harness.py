"""Shared code of the known-case test: runs the three forms of a case and its original.

A case folder `tests/known_cases/<case-id>/` holds `model.yaml` (the YAML form),
`builder.py` (`build(...)` returns the model and `run(app)` runs it), `low_level.py`
(`build(env, ...)` starts the processes on a `DynamicRealtimeEnvironment`, and `UNTIL`
is the end), and `original.py` or `expected.json`. A case whose forms are one of
SimPy's examples (the EX- rows) or a gallery example (GA-1 to GA-3) holds only
`original.py`, whose `EXAMPLE` names the example; its three forms are the example's
files under `examples/`, so the model is written once (`form_paths`). The `original.py` holds one of:

- a plain SimPy program: `program()` returns the log and the error text, and `DRAWS`
  fixes the random draws of `model.yaml` for the comparison;
- a function `check(records)` that states the checks of the row (`states_its_checks`);
- a project's own generator: `generate(config)`, `CONFIGS` and `compare`, and may have
  `VARIABLES` and `fix(document)` (`has_a_generator`);
- `PYTHON_FORM`, the document of a docs snippet or an example that runs its `!python`,
  with `LOGIC`, `project` or `compare` (`has_a_snippet`).

The harness also writes the certificate files of a case (`write_tls_files`).

The forms are compared as the known-case tests of the batches compare them: the
records a model publishes as the program logs them, (time, key, task, value). The
engine's own `queued`, `started` and `resumed` records are left out unless the
program logs them, because only then does the original show the moment of a grant.
"""

import ast
import contextlib
import importlib
import importlib.util
import inspect
import re
import shutil
import sys
import time
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from desflow.blueprint.loader import BlueprintError
from desflow.core.context import SimulationContext
from tests.conformance.harness import Outcome, RecordingEgress
from tests.conformance.harness import run_low_level as low_level_run

FOLDER = Path(__file__).parent
REPOSITORY = FOLDER.parents[1]
EXAMPLES = REPOSITORY / "examples"
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


# Cases whose original is plain SimPy and raises its own wording of a rule that desflow
# checks first: the YAML's error has the same class and its own message.
OWN_WORDING = {"ST-T02"}

# Cases whose program raises an error that a task or a signal of the YAML reports as a
# `TaskFailed` or a `SignalFailed` with the program's message as its reason: the class
# the program raises.
WRAPPED = {
    "CV-060": "ValueError",
    "CV-114": "RuntimeError",
    "GD-TT6": "ValueError",
    "ST-C09": "ValueError",
    "ST-P08": "AttributeError",
    "ST-X02": "ValueError",
    "ST-X03": "ValueError",
}

# The rows that known-cases.md lists under "Left out": no case, so no folder
LEFT_OUT = {
    "CV-017",
    "CV-023",
    "CV-041",
    "CV-042",
    "CV-048",
    "CV-057",
    "CV-058",
}


def python_tags(path: Path) -> list[str]:
    """The tags of a document that name Python code (`!python`), read from its YAML
    nodes with a loader that keeps every tag. The files an `!include` names are read as
    well, and a `${VAR}` in a path stands for any name, so each file the include could
    read is checked.

    Args:
        path: The document.

    Returns:
        list[str]: Each tag of the document and of the files it includes, such as
            `!python` or `!include`, once for each node that carries it.
    """
    import yaml

    class Loader(yaml.SafeLoader):
        pass

    found: list[str] = []

    def keep(loader: Any, suffix: str, node: Any) -> None:
        found.append(f"!{suffix}")
        if suffix == "include" and isinstance(node, yaml.ScalarNode):
            pattern = re.sub(r"\$\{[^}]*\}", "*", node.value)
            for included in sorted(path.parent.glob(pattern)):
                found.extend(python_tags(included))

    Loader.add_multi_constructor("!", keep)
    yaml.load(path.read_text(encoding="utf-8"), Loader=Loader)  # noqa: S506
    return found


def folder_of(case: str) -> str:
    """The folder name of a case id: lower case, with `-` as `_`."""
    return case.lower().replace("-", "_")


def example_of(case: str) -> str | None:
    """The example whose files are the case's three forms: `EXAMPLE` of its
    `original.py`, such as `carwash` for `examples/yaml/simpy/carwash.yaml`, or the slug
    of a gallery example, such as `orders_wait_for_busy_packers`.

    Args:
        case: The case's id, such as `EX-2`.

    Returns:
        str | None: The example's name, such as `carwash`, which its three files carry;
            None for a case whose forms are in its own folder.
    """
    if not (FOLDER / folder_of(case) / "original.py").exists():
        return None
    return getattr(form(case, "original"), "EXAMPLE", None)


def form_paths(case: str) -> dict[str, Path]:
    """The files of the case's three forms: `model.yaml`, `builder.py` and
    `low_level.py` of its folder, or the example's files that `example_of` names: the
    three files `examples/gallery/<slug>.yaml`, `<slug>_builder.py` and
    `<slug>_low_level.py` when the gallery has the slug, else the SimPy example's.

    Args:
        case: The case's id, such as `EX-2`.

    Returns:
        dict[str, Path]: The file of each form, by the name of the form's file in a
            case folder.
    """
    example = example_of(case)
    if example is None:
        folder = FOLDER / folder_of(case)
        return {
            name: folder / name for name in ("model.yaml", "builder.py", "low_level.py")
        }
    gallery = EXAMPLES / "gallery"
    if (gallery / f"{example}.yaml").exists():
        return {
            "model.yaml": gallery / f"{example}.yaml",
            "builder.py": gallery / f"{example}_builder.py",
            "low_level.py": gallery / f"{example}_low_level.py",
        }
    return {
        "model.yaml": EXAMPLES / "yaml" / "simpy" / f"{example}.yaml",
        "builder.py": EXAMPLES / "builder" / "simpy" / f"{example}_example.py",
        "low_level.py": EXAMPLES / "low-level" / "simpy" / f"{example}_example.py",
    }


def form(case: str, name: str) -> Any:
    """The module `builder`, `low_level` or `original` of a case. The forms of a case
    that runs an example are imported from the example's files by their paths, because
    the examples are scripts, not a package.

    Args:
        case: The case's id, such as `EX-2`.
        name: `builder`, `low_level` or `original`.

    Returns:
        ModuleType: The imported module.
    """
    module_name = f"tests.known_cases.{folder_of(case)}.{name}"
    if name == "original" or example_of(case) is None:
        return importlib.import_module(module_name)
    if module_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            module_name, form_paths(case)[f"{name}.py"]
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    return sys.modules[module_name]


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
    "CV-132": "the one worker fires a once signal, which a second worker would fire again and fail",
    "CV-056": "the interrupter hits the one worker by its task, at a fixed time",
    "CV-087": "the program takes one item by value, so a larger size finds none left",
    "HR-01": "the first `initial` arrival is the one drift clock, so a larger size is a drift step for each clock in every tick, which the twin has once",
    "BT-LD1": "the `initial` arrival is the catalogue of the project's 260 products, so another size is another catalogue, and an order of four different products needs four of them",
    "ST-RT6": "a paced run: the entities share the wall-clock delay, so a larger size does not finish in the program's time",
}


def needs_size(case: str) -> bool:
    """Whether the case's document has an arrival that starts a number of entities at
    once (`initial: <n>`), which the test runs at other sizes."""
    text = form_paths(case)["model.yaml"].read_text(encoding="utf-8")
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
    """Whether the case's document runs against the wall clock (`factor` above 0, or
    `PACED` of its `original.py`, for a document that is paced after its `go_live_at`),
    so CPU time taken by other tests in the same run can make it late."""
    if (FOLDER / folder_of(case) / "original.py").exists() and getattr(
        form(case, "original"), "PACED", False
    ):
        return True
    text = form_paths(case)["model.yaml"].read_text(encoding="utf-8")
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


def _without_sinks(app: SimulationContext, console: bool = True) -> None:
    """Takes the sinks that need a service away, as `scripts/regression.py capture`
    replaces them: the ingress, the set-up that runs before the run (such as the topics
    of a broker), and every egress but the console. The case's recording egress takes
    their place, so the run needs no broker, database or bucket. Without `console`
    the console goes as well: an example prints every record, which a test does not
    need to see."""
    app._sinks.ingress = []
    app._defaults.before_run = []
    app._sinks.egress = [
        sink
        for sink in app._sinks.egress
        if console and type(sink.provider).__name__ == "ConsoleEgress"
    ]


def _outcome(app: SimulationContext, run: Any, console: bool = True) -> Outcome:
    _without_sinks(app, console)
    recording = RecordingEgress()
    app.add_egress(recording)
    traced = RecordingEgress()
    if (
        app._sinks.stats_trace
    ):  # a sink for the trace records, as the program's trace needs
        app.add_egress(traced, when="trace")
    labelled = RecordingEgress()  # the ground-truth rows of scenario steps, which no
    app.add_egress(labelled, when="labels")  # other sink receives
    error: BaseException | None = None
    value: Any = None
    started = time.perf_counter()
    try:
        value = run(app)
    except Exception as exc:  # a run failure is a result of the case
        error = exc
    seconds = time.perf_counter() - started
    outcome = Outcome(recording.records, value, error, app)
    outcome.seconds = seconds  # type: ignore[attr-defined]
    outcome.trace = traced.records  # type: ignore[attr-defined]
    outcome.labels = labelled.records  # type: ignore[attr-defined]
    return outcome


def run_yaml(
    case: str,
    tmp_path: Path,
    size: int | None = None,
    fixed: bool = False,
    variables: dict[str, str] | None = None,
) -> Outcome:
    """Runs the case's YAML form (`form_paths`) as `model.yaml` in a copy of its
    folder, with `size` entities in the first arrival that starts a number of them at
    once. With `fixed`, the random draws are replaced by the values `DRAWS` of
    `original.py` names, which the original program uses as well, and by what its
    function `fix(document)` returns.

    `VARIABLES` of `original.py` gives the values of `${NAME}` in the document (a case
    that runs unpaced sets `FACTOR`), and `variables` adds to them or replaces them.
    """
    source = FOLDER / folder_of(case)
    work = tmp_path / ("fixed" if fixed else "yaml")
    work.mkdir()
    original = form(case, "original") if (source / "original.py").exists() else None
    values = {**getattr(original, "VARIABLES", {}), **(variables or {})}
    for path in source.iterdir():
        if path.suffix in (".csv", ".json", ".txt", ".desc", ".avsc"):
            shutil.copy(path, work / path.name)
        elif path.is_dir() and path.name != "__pycache__":  # such as `catalogs/`
            shutil.copytree(path, work / path.name)
    text = form_paths(case)["model.yaml"].read_text(encoding="utf-8")
    if fixed and (source / "original.py").exists():
        for old, new in getattr(form(case, "original"), "DRAWS", []):
            assert old in text, f"{case}: the text {old!r} is not in model.yaml"
            text = text.replace(old, new, 1)
    if size is not None:
        text = re.sub(r"initial: \d+", f"initial: {size}", text, count=1)
    (work / "model.yaml").write_text(text, encoding="utf-8")
    fix = getattr(original, "fix", None) if fixed else None
    if fix is not None:  # a document the original fixes as a tree, not as text
        import yaml

        from desflow.blueprint.loader import read_document

        document = read_document(work / "model.yaml", variables=values)[0]
        (work / "model.yaml").write_text(
            yaml.safe_dump(fix(document), sort_keys=False), encoding="utf-8"
        )
    try:
        app = SimulationContext.from_yaml(work / "model.yaml", variables=values)
    except BlueprintError as exc:  # a document the load refuses: the case's result
        return Outcome(
            records=[],
            value=None,
            error=exc,
            app_or_env=None,  # type: ignore[arg-type]
        )
    return _outcome(app, lambda app: app.run(), console=example_of(case) is None)


def run_builder(case: str, size: int | None = None) -> Outcome:
    """Runs the case's `builder.py`."""
    module = form(case, "builder")
    app = module.build() if size is None else module.build(size)
    # A gallery file has no `run(app)`: it ends at its `UNTIL`
    run = getattr(module, "run", None) or (lambda app: app.run(until=module.UNTIL))
    return _outcome(app, run, console=example_of(case) is None)


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
        environment=getattr(module, "ENVIRONMENT", None),
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


def states_its_checks(case: str) -> bool:
    """Whether the case's `original.py` is a function `check(records)` that states the
    checks of the row (the test asserts of a gallery example, the property of a project's
    generator), in place of a SimPy program."""
    try:
        return callable(getattr(form(case, "original"), "check", None))
    except ImportError:
        return False


def has_a_generator(case: str) -> bool:
    """Whether the case's `original.py` runs a project's own Python generator (the hot
    rolling twin, a benchtop project) instead of a SimPy program: it has a function
    `generate(config)`.

    Args:
        case: The case's id, such as `HR-01`.

    Returns:
        bool: True when `original.py` has `generate`.
    """
    try:
        return callable(getattr(form(case, "original"), "generate", None))
    except ImportError:
        return False


def run_against_generator(case: str, tmp_path: Path, size: int | None = None) -> None:
    """Runs the YAML with its random draws fixed, and the project's generator with the
    same draws, once for each entry of `CONFIGS` of `original.py`, and has the module's
    `compare(records, result, config)` assert that they agree.

    An entry of `CONFIGS` has `variables` (the values of `${NAME}` for the YAML) and
    `args` (what `generate` takes). The module owns what a generator's output is and
    how it equals the YAML's, because a project's records differ in key, order and
    rounding from a model's.

    Args:
        case: The case's id, such as `HR-01`.
        tmp_path: A folder for the runs' files.
        size: The number of entities of the first arrival that starts a number at
            once, or None for the document's own.

    Raises:
        AssertionError: When a configuration's records differ, or the YAML fails.
    """
    module = form(case, "original")
    for number, config in enumerate(module.CONFIGS):
        work = tmp_path / f"config{number}"
        work.mkdir()
        run = run_yaml(case, work, size, fixed=True, variables=config.get("variables"))
        assert run.error is None, run.error
        module.compare(run.records, module.generate(config), config)


def matches_generator(case: str, tmp_path: Path, size: int | None) -> bool:
    """Whether `run_against_generator` finds the YAML and the generator in agreement.

    Args:
        case: The case's id.
        tmp_path: A folder for the runs' files.
        size: The number of entities of the first arrival that starts a number at
            once, or None for the document's own.

    Returns:
        bool: True when every configuration agrees.
    """
    try:
        run_against_generator(case, tmp_path, size)
    except AssertionError:
        return False
    return True


def same_within(
    a: Any, b: Any, relative: float = 2.5e-5, absolute: float = 5e-4
) -> bool:
    """Whether two values, nested in mappings and lists, are equal where two numbers
    differ by at most `relative * abs(b) + absolute`, with `b` the second: a project that
    rounds a value it publishes (the hot rolling twin rounds its features to 3 decimals
    and its forces to 2) is compared with a model that publishes the value unrounded. The
    defaults are the measured need of HR-01, both in its third configuration: 0.000496
    at most on a feature (the length of a slab), and 2.2e-5 of the value on a baseline
    roll force, which the twin computes from the entry thickness rounded to 3 decimals,
    of a slab 20 mm thick. A change of 0.1 per cent in a rate or a constant therefore
    fails.

    Args:
        a: The first value.
        b: The second value, which sets the relative allowance.
        relative: The allowed difference as a share of `b`.
        absolute: The allowed difference added to the relative one.

    Returns:
        bool: True when the values are equal within the allowance.
    """
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(a - b) <= relative * abs(b) + absolute
    if isinstance(a, dict):
        return (
            isinstance(b, dict)
            and a.keys() == b.keys()
            and all(same_within(a[key], b[key], relative, absolute) for key in a)
        )
    if isinstance(a, (list, tuple)):
        return (
            isinstance(b, (list, tuple))
            and len(a) == len(b)
            and all(same_within(x, y, relative, absolute) for x, y in zip(a, b))
        )
    return a == b


def has_a_snippet(case: str) -> bool:
    """Whether the case's `original.py` names the document that holds the snippet's
    `!python` form (`PYTHON_FORM`, a path from the repository root: the docs snippet or
    the example itself, or a file of the case's folder).

    Args:
        case: The case's id, such as `DX-1`.

    Returns:
        bool: True when the case's `original.py` has a `PYTHON_FORM`; False when it
            has none or the case has no `original.py`.
    """
    try:
        return isinstance(getattr(form(case, "original"), "PYTHON_FORM", None), str)
    except ImportError:
        return False


def run_snippet(case: str) -> Outcome:
    """Runs the document `PYTHON_FORM`, a path from the repository root, which names
    the snippet's Python functions with `!python`, as they are written. `LOGIC` is the
    folder that holds the functions, from the repository root, and `UNTIL` the end of
    the run when the document has none. Its sinks are replaced by the recording
    egress, as for the other forms."""
    module = form(case, "original")
    logic = str(REPOSITORY / getattr(module, "LOGIC", "docs/snippets/yaml"))
    sys.path.insert(0, logic)
    try:
        app = SimulationContext.from_yaml(REPOSITORY / module.PYTHON_FORM)
        until = getattr(module, "UNTIL", None)
        return _outcome(
            app, lambda app: app.run() if until is None else app.run(until=until)
        )
    finally:
        sys.path.remove(logic)


def compare_with_snippet(case: str, records: list[dict[str, Any]]) -> None:
    """Checks that the records of the `!python`-free YAML form equal those of the
    snippet's `!python` form: the same events at the same times for the same keys and
    tasks, with the same values. A case that has a function `project(records)` compares
    what it returns for both instead, for forms that key their records differently, and
    one that has `compare(python_records, yaml_records)` states what holds for both
    runs, for forms that draw from different streams.

    Args:
        case: The case's id, such as `DX-1`.
        records: The records of the YAML form's run.

    Raises:
        AssertionError: If the snippet's run fails, or the records differ from it.
    """
    module = form(case, "original")
    snippet_run = run_snippet(case)
    assert snippet_run.error is None, snippet_run.error
    compare = getattr(module, "compare", None)
    if compare is not None:  # a case whose two forms draw from different streams
        compare(snippet_run.records, records)
        return
    project = getattr(module, "project", None)
    if project is not None:  # a case whose two forms name their records differently
        assert project(records) == project(snippet_run.records)
        return
    want = _events_of(snippet_run.records)
    got = _events_of(records)
    assert len(got) == len(want), (len(got), len(want))
    for a, b in zip(got, want):
        assert a == b, (a, b)


_NOT_A_RESULT = {"status", "path_id", "reason", "event_type"}


def finished_payloads(records: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    """The time and the payload of each event that is a task's result, sorted, as the
    snippet's process and the blueprint give them: a record that only says a task was
    queued, started, interrupted or resumed, and a field that only says what the
    record is, are left out, because a process that calls a task publishes none.

    Args:
        records: The records of a run.

    Returns:
        list[tuple[Any, ...]]: The time, to 6 decimals, and the sorted fields of each
            result's payload, sorted.
    """
    return sorted(
        (
            round(r["sim_ts"], 6),
            sorted(
                (k, repr(v)) for k, v in r["value"].items() if k not in _NOT_A_RESULT
            ),
        )
        for r in records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and r["value"].get("status")
        not in ("queued", "started", "interrupted", "resumed")
        and set(r["value"]) - _NOT_A_RESULT
    )


@contextlib.contextmanager
def guide_avro_models() -> Iterator[types.ModuleType]:
    """The module `avro_and_pydantic_models` of the guides' snippets, imported from
    `docs/snippets/guides`, with `avro_and_pydantic_logic` importable inside the block.
    The models need `dataclasses-avroschema`, which is in the dev dependency group. The
    modules are removed on leaving."""
    folders = [
        str(REPOSITORY / "docs" / "snippets" / "guides"),
        str(REPOSITORY / "docs" / "snippets" / "yaml"),
    ]
    sys.path[:0] = folders
    sys.modules.pop("avro_and_pydantic_models", None)
    try:
        yield importlib.import_module("avro_and_pydantic_models")
    finally:
        for folder in folders:
            sys.path.remove(folder)
        for name in ("avro_and_pydantic_models", "avro_and_pydantic_logic"):
            sys.modules.pop(name, None)


def write_tls_files(folder: Path) -> dict[str, Path]:
    """Writes a CA certificate, and a certificate and an unencrypted key of a service
    it signs, as PEM files in `folder`, for a case or a unit test that builds an SSL
    context from certificate files. `cryptography` of the dev dependency group makes them.

    Args:
        folder: The folder the files are written to.

    Returns:
        dict[str, Path]: A mapping of `ca`, `cert` and `key` to the file written.
    """
    from datetime import datetime, timedelta, timezone

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    def name(common: str) -> x509.Name:
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common)])

    def certificate(subject, key, issuer, signer, ca):
        now = datetime.now(timezone.utc)
        return (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=1))
            .not_valid_after(now + timedelta(days=30))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), True)
            .sign(signer, hashes.SHA256())
        )

    ca_key, service_key = (
        ec.generate_private_key(ec.SECP256R1()),
        ec.generate_private_key(ec.SECP256R1()),
    )
    ca = certificate(
        name("known-cases CA"), ca_key, name("known-cases CA"), ca_key, True
    )
    service = certificate(
        name("known-cases service"), service_key, name("known-cases CA"), ca_key, False
    )
    pem = serialization.Encoding.PEM
    files = {
        "ca": folder / "ca.pem",
        "cert": folder / "service.cert",
        "key": folder / "service.key",
    }
    files["ca"].write_bytes(ca.public_bytes(pem))
    files["cert"].write_bytes(service.public_bytes(pem))
    files["key"].write_bytes(
        service_key.private_bytes(
            pem,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return files


def _events_of(records: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    return [
        (r["sim_ts"], r["key"], r["task"], r["instance"], r["value"])
        for r in records
        if r["stream_type"] == "event" and isinstance(r["value"], dict)
    ]

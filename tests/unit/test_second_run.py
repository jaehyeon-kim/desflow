"""A second run of one context gives what a fresh context gives.

Every YAML blueprint and builder model that `scripts/regression.py capture` runs is run
twice on one context and once on a new context, with the same seed, and the second
run's records, statistics and error are compared with the fresh run's. A run that
leaves state in the context, such as a live change written into a declaration or a
member of a family added to it, makes the second run differ.

Every attribute of the context is also compared before and after the first run, apart
from the state of a run (`RUN_STATE`), because a write into a declaration that no
later read meets changes no record: a member's entry added to `_signals.entries`, or a
change written into a breakdown's distribution of a model whose second run ends
before the repair it changes. And no registry may hold a declared
config object itself, because a write the load checks make into one comes before
the first fingerprint.
"""

import ast
import contextlib
import functools
import importlib.util
import inspect
import logging
import os
import textwrap
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, List, Set, Tuple
from unittest.mock import MagicMock, patch

import pytest

from desflow.core.context import SimulationContext, scratch_registry

# It runs every model the regression capture runs, so CI runs it on one Python
# version on each system (scripts/ci_plan.py).
pytestmark = pytest.mark.every_model

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "regression_script", ROOT / "scripts" / "regression.py"
)
assert _spec is not None and _spec.loader is not None
regression = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(regression)
twins, _harness = regression._load(ROOT)

# The runs that are not compared, each with its reason: each is refused at load, as the
# capture records it, so there is no context to run twice
_AT_LOAD = "refused at load, so there is no context to run twice: "
NOT_COMPARED: Dict[str, str] = {
    "file:docs/snippets/pages/blueprint_error.yaml": _AT_LOAD
    + "it shows an error message on purpose",
    "file:docs/snippets/yaml/python_error.yaml": _AT_LOAD
    + "it shows an error message on purpose",
    "file:tests/known_cases/dx_23/model.yaml": _AT_LOAD + "it needs KAFKA_CA_FILE set",
    "file:tests/known_cases/st_rt5/model.yaml": _AT_LOAD
    + "its `run.until` is -1 on purpose",
}

# The runs compared over a shorter time than the capture runs, each with its reason
SHORTER: Dict[str, Tuple[str, str]] = {
    "file:docs/snippets/yaml/filtered_inventory.yaml": (
        "2 h",
        "its 8 hours take 30 s a run, and a leak shows in the first 2 hours as in "
        "any other run",
    ),
}

Case = Tuple[str, Callable[[], Any], Any, Dict[str, Any]]

# The engine-wide sweeps of the tests leave out the Generators examples: each runs in
# `tests/docs/test_generator_examples.py`, which compares it with the records its page
# shows. The regression capture still runs them.
NOT_SWEPT = "examples/generators/"


def _cases() -> List[Case]:
    """Each run of the capture that runs a context: its name in the capture, how its
    context is made, its `until` and its `stop` and `drain`."""
    cases: List[Case] = []
    for name, yaml_file, extra, until, _processes in twins.EXAMPLE_CASES:
        run = twins.RUN.get(name, {})
        path = twins.EXAMPLES / "yaml" / yaml_file
        cases.append(
            (
                f"example:{name}:yaml",
                functools.partial(twins._from_yaml, path),
                until,
                run,
            )
        )
        builder = twins.EXAMPLES / "builder" / f"{name}_example.py"
        cases.append((f"example:{name}:builder", _builder(builder), until, run))
    for path in sorted((ROOT / "tests" / "conformance" / "blueprints").glob("*.yaml")):
        cases.append(
            (
                f"blueprint:{path.name}",
                functools.partial(twins._from_yaml, path),
                None,
                {},
            )
        )
    left_out = regression._left_out(ROOT / "tests" / "regression" / "left_out.yaml")
    for path in regression._other_blueprints(ROOT, twins, left_out):
        name = path.relative_to(ROOT).as_posix()
        if name.startswith(NOT_SWEPT):
            continue
        until = SHORTER[f"file:{name}"][0] if f"file:{name}" in SHORTER else None
        cases.append(
            (f"file:{name}", functools.partial(twins._from_yaml, path), until, {})
        )
    folder = ROOT / "tests" / "regression" / "scenarios"
    scenarios = regression._scenario_cases(folder)
    for name, yaml_file, _extra, until, _processes in scenarios.SCENARIO_CASES:
        run = scenarios.SCENARIO_RUN.get(name, {})
        path = folder / "yaml" / yaml_file
        cases.append(
            (
                f"scenario:{name}:yaml",
                functools.partial(twins._from_yaml, path),
                until,
                run,
            )
        )
        builder = folder / "builder" / f"{name}_example.py"
        cases.append((f"scenario:{name}:builder", _builder(builder), until, run))
    return cases


def _builder(path: Path) -> Callable[[], Any]:
    """Makes a builder model's context again: its module loaded anew."""

    def make() -> Any:
        module = twins._load(path)
        return module.app if hasattr(module, "app") else module.shop()

    return make


CASES = _cases()


def _outcome(app: Any, until: Any, run: Dict[str, Any]) -> Dict[str, Any]:
    """One run's records and end statistics, or the error it stopped with."""
    if app.random_seed is None:
        app.random_seed = 11
    try:
        records = twins._run_builder(app, until, **run)
    except Exception as exc:  # the error is the outcome compared
        return {"error": f"{type(exc).__name__}: {exc}"}
    return {"records": records, "statistics": app.env.stats.readings()}


def _made_by_each_run() -> Set[str]:
    """The attributes `start`, `_build` and `_check_tasks` assign: the state each
    run makes again."""
    found: Set[str] = set()
    for method in (
        SimulationContext.start,
        SimulationContext._build,
        SimulationContext._check_tasks,
    ):
        tree = ast.parse(textwrap.dedent(inspect.getsource(method)))
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
                targets = [node.target]
            else:
                continue
            for target in targets:
                for part in ast.walk(target):
                    if (
                        isinstance(part, ast.Attribute)
                        and isinstance(part.value, ast.Name)
                        and part.value.id == "self"
                    ):
                        found.add(part.attr)
    return found


# The attributes a run may change, each with its reason; every other attribute is a
# declaration, which a run leaves as written
_BY_THE_CAPTURE = "the capture sets it before each run (`_run_builder`)"
RUN_STATE: Dict[str, str] = {
    **{name: "each run's start makes it again" for name in _made_by_each_run()},
    **{
        name: _BY_THE_CAPTURE
        for name in (
            "_sinks",
            "factor",
            "go_live_at",
        )
    },
    "_runs_of": "the run's task generators, held weakly",
    "_order_warned": "the messages logged once, so a second run does not log again",
}


def _fingerprint(value: Any, depth: int = 0, seen: FrozenSet[int] = frozenset()) -> Any:
    """A value that is equal for two states of an object exactly when every
    attribute, item and key in it, to a depth of 8, is equal."""
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return repr(value)
    if depth > 8 or id(value) in seen:
        return f"<{type(value).__name__}>"
    seen = seen | {id(value)}
    if isinstance(value, dict):
        return tuple(
            (_fingerprint(k, depth + 1, seen), _fingerprint(v, depth + 1, seen))
            for k, v in value.items()
        )
    if isinstance(value, (list, tuple)):
        return tuple(_fingerprint(v, depth + 1, seen) for v in value)
    if isinstance(value, (set, frozenset)):
        return tuple(sorted(repr(_fingerprint(v, depth + 1, seen)) for v in value))
    if callable(value) and not hasattr(value, "__dict__"):
        return f"<{getattr(value, '__qualname__', type(value).__name__)}>"
    if inspect.isfunction(value) or inspect.ismethod(value) or isinstance(value, type):
        return f"<{value.__qualname__}>"
    if hasattr(value, "__dict__"):
        return (type(value).__name__, _fingerprint(dict(vars(value)), depth + 1, seen))
    if hasattr(value, "__slots__"):
        slots = {name: getattr(value, name, None) for name in value.__slots__}
        return (type(value).__name__, _fingerprint(slots, depth + 1, seen))
    return f"<{type(value).__name__}>"


def _declarations(app: Any) -> Dict[str, Any]:
    """Each declaration attribute of a context, fingerprinted."""
    return {
        name: _fingerprint(value)
        for name, value in vars(app).items()
        if name not in RUN_STATE
    }


def _declared_objects(app: Any) -> Dict[int, str]:
    """Each object the declarations hold that a registry writes into, by its id: the
    config objects of `compile_parameters()`, a breakdown's two distributions and the
    variables mapping."""
    params = app.compile_parameters()
    found: Dict[int, str] = {id(params.variables): "variables"}
    for section in ("resources", "containers", "stores", "service", "arrival"):
        for name, config in getattr(params, section).items():
            found[id(config)] = f"{section}.{name}"
    for name, breakdown in params.breakdowns.items():
        found[id(breakdown)] = f"breakdowns.{name}"
        found[id(breakdown.time_to_failure)] = f"breakdowns.{name}.time_to_failure"
        found[id(breakdown.repair)] = f"breakdowns.{name}.repair"
    return found


def _registered_declarations(app: Any) -> List[str]:
    """The paths at which the run's registry, or the registry the load and start
    checks build, holds a declared object itself, not the run's copy: a change there,
    such as a scenario step that the load check applies, would write into the
    declarations before any fingerprint is taken."""
    declared = _declared_objects(app)
    registries = [] if app._env is None else [app._env.registry]
    try:
        registries.append(scratch_registry(app))
    except ValueError:  # a model whose parameters the registry refuses
        pass
    return [
        f"{path} ({declared[id(config)]})"
        for registry in registries
        for path, config in registry.configs().items()
        if id(config) in declared
    ]


def _first_difference(second: Dict[str, Any], fresh: Dict[str, Any]) -> str:
    """The first part, statistic or record in which two outcomes differ."""
    if second.keys() != fresh.keys() or "error" in second:
        return f"second run {second.get('error', 'ran')}; fresh run {fresh.get('error', 'ran')}"
    for name in sorted(set(second["statistics"]) | set(fresh["statistics"])):
        if second["statistics"].get(name) != fresh["statistics"].get(name):
            return (
                f"statistic {name}: second run {second['statistics'].get(name)!r}, "
                f"fresh run {fresh['statistics'].get(name)!r}"
            )
    pairs = zip(second["records"], fresh["records"])
    for index, (left, right) in enumerate(pairs):
        if left != right:
            return f"record {index}: second run {left!r}, fresh run {right!r}"
    return (
        f"{len(second['records'])} records in the second run, "
        f"{len(fresh['records'])} in the fresh run"
    )


@pytest.mark.parametrize("name,make,until,run", CASES, ids=[case[0] for case in CASES])
def test_a_second_run_gives_what_a_fresh_context_gives(name, make, until, run):
    if name in NOT_COMPARED:
        pytest.skip(NOT_COMPARED[name])
    logging.disable(logging.CRITICAL)
    try:
        with (
            regression._restored_imports(),
            contextlib.chdir(ROOT),
            # A blueprint may read ${PWD}, which Windows does not set
            patch.dict(os.environ, {"PWD": str(ROOT)}),
            patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
        ):
            app = make()
            if app.random_seed is None:
                app.random_seed = 11  # as `_outcome` sets it, before the fingerprint
            declared = _declarations(app)
            _outcome(app, until, run)
            after = _declarations(app)
            changed = sorted(
                k
                for k in declared.keys() | after.keys()
                if declared.get(k) != after.get(k)
            )
            assert changed == [], f"{name}: the first run changed {', '.join(changed)}"
            shared = _registered_declarations(app)
            assert shared == [], f"{name}: a registry holds {', '.join(shared)}"
            second = _outcome(app, until, run)
            fresh = _outcome(make(), until, run)
    finally:
        logging.disable(logging.NOTSET)
    assert second == fresh, f"{name}: {_first_difference(second, fresh)}"


def test_every_run_not_compared_is_a_run_of_the_capture():
    names = {case[0] for case in CASES}
    assert sorted((set(NOT_COMPARED) | set(SHORTER)) - names) == []


def test_a_list_attribute_a_task_changes_starts_empty_for_every_entity_and_run():
    seen = []
    app = SimulationContext("S", factor=0, random_seed=1)
    app.add_arrival(
        "orders", gap=1, spawn="serve", max_events=3, attributes={"tags": []}
    )

    @app.task(name="serve", service_time=0)
    def serve(task_id, entity):
        seen.append(list(entity.attributes["tags"]))
        entity.attributes["tags"].append(task_id)
        return {}

    app.run(until=10)
    app.run(until=10)
    assert all(tags == [] for tags in seen), seen

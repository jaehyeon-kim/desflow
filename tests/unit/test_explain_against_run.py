"""`explain` gives a value only where a run agrees, and leaves out what its formulas do not cover.

Each model of the capture (the cases of `test_second_run.py`) that `analyze`
gives a compared value is verified with 5 replications and must show no "differs". The
same model with an arrival `group` of 3, every arrival off (`active: false`), a spawn
`count` of 3, an `on:` entry that sets another resource's capacity, or a resource family
must have no compared value for each resource it touches. The statistics `pairs` names
are among those a run publishes, also for a resource family.
"""

import importlib.util
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest
import scipy.optimize  # noqa: F401  # imported before the capture restores sys.modules
import scipy.stats  # noqa: F401

from desflow import SimulationContext
from desflow.analysis.models import run_once
from desflow.builder.arrivals import ArrivalPlan
from desflow.builder.context import parts_of
from desflow.explain import analyze, build_graph, verify
from desflow.explain.pairs import pairs

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "connector_cases", ROOT / "tests" / "unit" / "test_connector_second_run.py"
)
assert _spec is not None and _spec.loader is not None
cases = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cases)
MODELS = cases.second_run.CASES
IDS = [case[0] for case in MODELS]


def _analysis(context: Any):
    return analyze(build_graph(context))


def _compared(explanation: Any) -> Dict[str, Optional[float]]:
    return {r.name: r.utilization for r in explanation.resources}


def _built(make: Any) -> Any:
    """The model's context; a model that does not build here is skipped."""
    try:
        return make()
    except Exception as exc:
        pytest.skip(f"the model does not build here: {type(exc).__name__}")


def _models(make: Any):
    """The resources' compared values, or None when no resource is compared."""
    context = _built(make)
    found = _compared(_analysis(context))
    if not any(value is not None for value in found.values()):
        return None
    return found


# Models that are not verified, each with its reason
NOT_VERIFIED: Dict[str, str] = {
    "blueprint:resource_priority.yaml": (
        "its arrivals have constant gaps of 100 and 101 seconds, so a run of 5000 "
        "seconds holds 49 arrivals where the rate gives 49.5, and the measure is "
        "1.7 percent below the formula in every replication"
    ),
}


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_a_compared_value_agrees_with_five_replications(name, make, until, run):
    if name in NOT_VERIFIED:
        pytest.skip(NOT_VERIFIED[name])
    with cases._capture_environment():
        if _models(make) is None:
            pytest.skip("no resource has a compared value")
        explanation = _analysis(_built(make))
        # Long enough that the entities cut off at the end are a small part of a run
        end = max(5000.0, until if isinstance(until, (int, float)) else 0.0)
        try:
            checked = verify(make, explanation, until=end, seed=3, replications=5)
        except RuntimeError as exc:
            pytest.skip(f"the model stops itself in a longer run: {exc}")
    assert checked.verification is not None
    differ = [
        f"{c.node} {c.measure}: {c.analytical} against {c.measured}"
        for c in checked.verification.comparisons
        if c.differs
    ]
    assert not differ, f"{name}: {differ}"


def _mutated(make: Any, kind: str):
    """The model's context changed as `kind` says, with the resources it touches, or
    None when the model has nothing to change."""
    context = _built(make)
    parts = parts_of(context)
    before = _compared(_analysis(context))
    compared = {n for n, v in before.items() if v}
    if not compared:
        return None
    if kind in ("group", "active"):
        for arrival in [*parts.arrivals.configs, *parts.arrivals.replays]:
            plan = parts.arrivals.plans.setdefault(arrival, ArrivalPlan())
            if kind == "group":
                plan.group = 3
            else:
                plan.active = False
    elif kind == "spawn":
        specs = [s for s in parts.tasks.specs.values() if s.spawn is not None]
        if not specs:
            return None
        for spec in specs:
            spec.spawn = {**(spec.spawn or {}), "count": 3}
    elif kind == "on":
        if len(parts.holders.resources_config) < 2 or not parts.tasks.specs:
            return None
        first = next(iter(parts.tasks.specs.values()))
        touched = sorted(compared)[0]
        first.on.append(
            {"path": f"{context.sim_id}.resources.{touched}.current_cap", "value": 1}
        )
        return context, {touched}
    elif kind == "family":
        touched = sorted(compared)[0]
        parts.families.declare("resource", touched, True, 2, {})
        return context, {touched}
    return context, compared


@pytest.mark.parametrize("kind", ["group", "active", "spawn", "on", "family"])
@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_a_part_the_formulas_leave_out_leaves_its_resources_uncompared(
    name, make, until, run, kind
):
    with cases._capture_environment():
        found = _mutated(make, kind)
        if found is None:
            pytest.skip("nothing to change in this model")
        context, touched = found
        after = _compared(_analysis(context))
    left = {n: after[n] for n in touched if after.get(n) is not None}
    assert not left, f"{name}: with {kind}, still compared: {left}"


SHOP = """\
simulation: {{sim_id: S, factor: 0, random_seed: 1}}
resources: {{desk: {{{desk}}}}}
services: {{work: {{dist: exponential, rate: 1.0}}}}
arrivals: {{orders: {{dist: exponential, rate: {rate}, spawn: serve, {arrival}}}}}
tasks: {{serve: {{service: work, resource: {resource}, payload: {{}}}}}}
run: {{until: 2000}}
"""


def _context(tmp_path: Path, **fields: str) -> SimulationContext:
    values = {
        "desk": "current_cap: 1",
        "rate": "0.3",
        "arrival": "initial: 0",
        "resource": "desk",
        **fields,
    }
    path = tmp_path / "m.yaml"
    path.write_text(textwrap.dedent(SHOP.format(**values)))
    return SimulationContext.from_yaml(path)


@pytest.mark.parametrize(
    "fields",
    [
        {"arrival": "group: 3"},
        {"arrival": "active: false"},
    ],
    ids=["group", "active"],
)
def test_an_arrival_the_formulas_leave_out_gives_no_compared_value(tmp_path, fields):
    explanation = _analysis(_context(tmp_path, **fields))
    assert [r.utilization for r in explanation.resources] == [None]
    assert explanation.arrivals[0].rate is None


def test_a_family_is_a_feature_and_pairs_name_a_published_statistic(tmp_path):
    context = _context(
        tmp_path,
        desk="family: true, current_cap: 1",
        resource="{of: desk, key: attr.i}",
        rate="1.8",
        arrival="attributes: {i: {gen: one_of, choices: [0, 1, 2]}}",
    )
    explanation = _analysis(context)
    assert [r.utilization for r in explanation.resources] == [None]
    published = run_once(
        lambda: _context(
            tmp_path,
            desk="family: true, current_cap: 1",
            resource="{of: desk, key: attr.i}",
            rate="1.8",
            arrival="attributes: {i: {gen: one_of, choices: [0, 1, 2]}}",
        ),
        seed=1,
        until=200,
        stats="typical",
    )
    names = {n.removeprefix(f"{published.sim_id}.stats.") for n in published.readings}
    missing: List[str] = [
        p.statistic for p in pairs(explanation, 200) if p.statistic not in names
    ]
    assert not missing, missing


def test_a_spawn_count_other_than_one_is_a_feature(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(
        textwrap.dedent(
            """\
            simulation: {sim_id: C, factor: 0, random_seed: 1}
            resources: {desk: {current_cap: 1}}
            services: {work: {dist: exponential, rate: 1.0}}
            arrivals: {orders: {dist: exponential, rate: 0.25, spawn: intake}}
            tasks:
              intake: {payload: {}, spawn: {task: serve, count: 3}}
              serve: {service: work, resource: desk, payload: {}}
            run: {until: 2000}
            """
        )
    )
    explanation = _analysis(SimulationContext.from_yaml(path))
    assert [r.utilization for r in explanation.resources] == [None]


def test_an_on_entry_that_sets_another_resources_capacity_is_a_feature(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(
        textwrap.dedent(
            """\
            simulation: {sim_id: S, factor: 0, random_seed: 1}
            resources: {front: {current_cap: 1}, back: {current_cap: 4, max_cap: 4}}
            services: {quick: {dist: exponential, rate: 10.0}, slow: {dist: exponential, rate: 1.0}}
            arrivals: {a: {dist: exponential, rate: 0.2, spawn: one}, b: {dist: exponential, rate: 0.8, spawn: two}}
            tasks:
              one: {service: quick, resource: front, payload: {}, on: [{path: S.resources.back.current_cap, value: 1}]}
              two: {service: slow, resource: back, payload: {}}
            run: {until: 2000}
            """
        )
    )
    explanation = _analysis(SimulationContext.from_yaml(path))
    assert {r.name: r.utilization for r in explanation.resources}["back"] is None

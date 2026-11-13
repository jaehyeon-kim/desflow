"""`desflow explain` and `context.analyze()`: the graph, the analysis against the
queueing formulas, `--verify`, the JSON and its schema, and the command's exits."""

import asyncio
import json
import logging
import math
import queue
import textwrap
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from desflow import SimulationContext
from desflow.cli import app as cli
from desflow.models.distributions import clamped_normal_moments
from desflow.explain.capacity import _requests
from desflow.explain.models import Explanation

SCHEMA = Path(__file__).resolve().parents[2] / "src/desflow/explain/explain.schema.json"

MM1 = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 7}
resources:
  packers: {current_cap: 1, max_cap: 4}
services:
  packing: {dist: exponential, rate: 1}
arrivals:
  orders: {dist: exponential, rate: 0.8, spawn: pack}
tasks:
  pack: {service: packing, resource: packers, payload: {}}
run: {until: 20000}
"""


def _shop(arrival=None, service=None, packers=1, most=None, **task):
    """One arrival into one task on one resource, with the task's keys."""
    app = SimulationContext(
        sim_id="Shop", factor=0, random_seed=7, logical_start_time=datetime(2026, 1, 1)
    )
    app.add_arrival(
        "orders", **(arrival or {"dist": "exponential", "rate": 0.8}), spawn="pack"
    )
    app.add_service("packing", **(service or {"dist": "exponential", "rate": 1.0}))
    app.add_resource("packers", current_cap=packers, max_cap=most or packers)

    @app.task(service_id="packing", resource_id="packers", **task)
    def pack(task_id):
        return {}

    return app


def _by_name(items, name):
    return next(item for item in items if item.name == name)


def test_mm1_matches_the_formulas():
    explanation = _shop().analyze()
    packers = _by_name(explanation.resources, "packers")
    rho = 0.8
    assert packers.utilization == pytest.approx(rho, abs=1e-9)
    assert packers.wait == pytest.approx(rho / (1 - rho) * 1.0, abs=1e-9)
    assert packers.queue_length == pytest.approx(0.8 * 4.0, abs=1e-9)
    assert packers.in_system == pytest.approx(0.8 * 5.0, abs=1e-9)
    assert (packers.status, packers.method, explanation.bottleneck) == (
        "OK",
        "kingman",
        "packers",
    )


def test_mm3_uses_erlang_c():
    explanation = _shop({"dist": "exponential", "rate": 2.4}, packers=3).analyze()
    packers = _by_name(explanation.resources, "packers")
    # Erlang C in the form of Wikipedia's M/M/c queue page, with c = 3, rho = 0.8
    c, rho = 3, 0.8
    tail = sum((c * rho) ** k / math.factorial(k) for k in range(c))
    erlang = 1 / (1 + (1 - rho) * math.factorial(c) / (c * rho) ** c * tail)
    assert packers.method == "erlang_c"
    assert packers.wait == pytest.approx(erlang / (c * 1.0 - 2.4), abs=1e-9)
    assert packers.status == "OK"


def test_md1_is_kingman_with_constant_service():
    explanation = _shop(service={"dist": "constant", "value": 1.0}).analyze()
    packers = _by_name(explanation.resources, "packers")
    assert packers.wait == pytest.approx(0.8 / 0.2 * (1 + 0) / 2 * 1.0, abs=1e-9)
    assert packers.method == "kingman"


def test_kingman_with_constant_gaps_is_an_estimate_not_compared():
    """D/M/1: Kingman's formula approximates the wait, so verify leaves it out."""
    from desflow.explain.pairs import pairs

    explanation = _shop(arrival={"dist": "constant", "value": 1.25}).analyze()
    packers = _by_name(explanation.resources, "packers")
    assert packers.wait == pytest.approx(0.8 / 0.2 * (0 + 1) / 2 * 1.0, abs=1e-9)
    assert packers.method == "kingman_approx"
    found = {(p.node, p.measure): p for p in pairs(explanation, 100.0)}
    for key in (("packers", "queue_length"), ("pack", "wait")):
        assert found[key].analytical is None
        assert found[key].reason.startswith("Kingman's formula approximates")
    assert found[("packers", "utilization")].analytical == pytest.approx(0.8)


def test_md3_wait_is_unsupported_with_its_reason():
    explanation = _shop(
        {"dist": "exponential", "rate": 2.4},
        {"dist": "constant", "value": 1.0},
        packers=3,
    ).analyze()
    packers = _by_name(explanation.resources, "packers")
    assert packers.utilization == pytest.approx(0.8)
    assert packers.wait is None
    assert packers.wait_reason == (
        "no closed formula for c > 1 with non-exponential times; run --verify to measure it"
    )


def _flow(next_pack, next_check=None):
    """Orders at rate 1 to pack, then by `next_pack`; check goes by `next_check`."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=1.0, spawn="pack")

    @app.task(next=next_pack)
    def pack(task_id):
        return {}

    for name in ("ship", "rework", "check"):
        app.task(name=name, next=next_check if name == "check" else None)(
            lambda task_id: {}
        )
    return {t.name: t for t in app.analyze().tasks}


def test_weights_split_the_rate():
    tasks = _flow({"weights": {"ship": 9, "rework": 1}})
    assert tasks["ship"].rate == pytest.approx(0.9)
    assert tasks["rework"].rate == pytest.approx(0.1)


def test_a_rework_loop_raises_the_rate_of_its_tasks():
    tasks = _flow("check", {"weights": {"ship": 9, "pack": 1}})
    assert tasks["pack"].rate == pytest.approx(1 / 0.9)
    assert tasks["ship"].rate == pytest.approx(1.0)


def test_a_loop_with_no_exit_is_unknown():
    tasks = _flow("check", "pack")
    assert tasks["pack"].rate is None and tasks["check"].rate is None
    assert tasks["pack"].reason == "routing loop with no exit"


def test_when_rules_make_the_split_unknown():
    tasks = _flow(
        {"when": [{"condition": "t > 5", "task": "ship"}], "default": "rework"}
    )
    assert tasks["pack"].rate == pytest.approx(1.0)
    assert tasks["ship"].rate is None
    assert "depends on the run" in tasks["ship"].reason
    assert tasks["ship"].reason == tasks["rework"].reason


@pytest.mark.parametrize(
    "task, feature",
    [
        ({"patience": 5}, "abandonment (patience)"),
        ({"balk": "t > 5"}, "abandonment (balk)"),
        ({"wait": {"timeout": 1}}, "wait"),
        ({"signal": "done"}, "signal"),
        ({"subscribe": {"signal": "done"}}, "subscribe"),
        ({"interrupt": {"task": "pack"}}, "interrupt"),
        ({"on": [{"path": "Shop.variables.x", "value": 1}]}, "on actions"),
        (
            {
                "listen": {
                    "signal": "done",
                    "on": [{"path": "Shop.variables.x", "value": 1}],
                }
            },
            "listen",
        ),
    ],
)
def test_a_task_feature_makes_the_wait_unsupported(task, feature):
    app = _shop(**task)
    app.add_signal("done")
    app.add_variable("x", 0)
    _check_feature(app, feature)


def _check_feature(app, feature):
    """A feature takes the task's rate, so the resource has no utilisation, status
    or wait, and no bottleneck can be named."""
    explanation = app.analyze()
    packers = _by_name(explanation.resources, "packers")
    assert packers.utilization is None and packers.status is None
    assert packers.wait is None and packers.queue_length is None
    assert feature in packers.reason and packers.reason.endswith(
        "; run --verify to measure it"
    )
    assert explanation.bottleneck is None
    assert feature in explanation.bottleneck_reason


def _requesting_shop(wait):
    """The shop of `_shop`, with the packer won by a `request` in the task's wait
    instead of by `resource_id`."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=0.8, spawn="pack")
    app.add_service("packing", dist="exponential", rate=1.0)
    app.add_resource("packers")
    app.task("packing", name="pack", wait=wait)(lambda task_id: {})
    return app


def test_a_unit_requested_in_a_wait_counts_as_the_resource_the_task_uses():
    held = _by_name(
        _requesting_shop({"request": "packers"}).analyze().resources, "packers"
    )
    plain = _by_name(_shop().analyze().resources, "packers")
    assert held.utilization == pytest.approx(plain.utilization) == pytest.approx(0.8)
    assert held.wait == pytest.approx(plain.wait)


def test_a_request_a_timeout_can_end_is_a_feature():
    wait = {"any": {"unit": {"request": "packers"}, "limit": {"timeout": 30}}}
    _check_feature(_requesting_shop(wait), "wait group with a timeout")


@pytest.mark.parametrize(
    "wait",
    [
        {"all": {"a": {"request": "packers"}, "b": {"request": "packers"}}},
        {"any": {"a": {"request": "packers"}, "b": {"request": "packers"}}},
        {
            "at_least": 2,
            "of": {"a": {"request": "packers"}, "b": {"request": "packers"}},
        },
    ],
    ids=["all", "any", "at_least"],
)
def test_a_wait_with_two_requests_of_one_resource_is_a_feature(wait):
    # An all holds both units, and an any keeps every request granted at once
    _check_feature(_requesting_shop(wait), "wait group with several requests")


def test_an_any_of_two_resources_does_not_hold_several_resources():
    app = _requesting_shop(
        {"any": {"a": {"request": "packers"}, "b": {"request": "benches"}}}
    )
    app.add_resource("benches")
    reason = _by_name(app.analyze().resources, "packers").reason
    assert "wait group with several requests" in reason
    assert "several resources" not in reason


def test_a_current_cap_above_max_cap_is_analysed_as_the_run_uses_it(caplog):
    """The builder clamps current_cap to max_cap with a warning, so analyze() sees
    the one unit the run uses, not two."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=0.8, spawn="pack")
    app.add_service("packing", dist="exponential", rate=1.5)
    with caplog.at_level(logging.WARNING, logger="desflow.builder.context"):
        app.add_resource("packers", current_cap=2, max_cap=1)
    app.task("packing", "packers", name="pack")(lambda task_id: {})
    packers = _by_name(app.analyze().resources, "packers")
    assert packers.servers == 1
    assert packers.utilization == pytest.approx(0.8 / 1.5)
    assert "current_cap 2 of resource 'packers'" in caplog.text


def test_infinity_declared_as_a_current_cap_is_clamped_to_a_whole_number(caplog):
    app = SimulationContext(sim_id="Shop", factor=0)
    with caplog.at_level(logging.WARNING, logger="desflow.builder.context"):
        app.add_resource("packers", current_cap=math.inf, max_cap=5)
        app.add_container("tank", current_cap=math.inf, max_cap=5)
    assert [m.rsplit(". ", 1)[1] for m in caplog.messages] == ["Using 5.", "Using 5.0."]


def test_a_current_cap_clamped_to_a_whole_float_max_cap_is_a_whole_number(caplog):
    app = SimulationContext(sim_id="Shop", factor=0)
    with caplog.at_level(logging.WARNING, logger="desflow.builder.context"):
        app.add_resource("packers", current_cap=7, max_cap=5.0)
    assert [m.rsplit(". ", 1)[1] for m in caplog.messages] == ["Using 5."]


def test_a_float_current_cap_clamped_to_max_cap_keeps_its_float_type(caplog):
    """`7.0` under `max_cap` 5 gives `5.0`, as on rc3: the clamp keeps the type of
    `current_cap`, and only an infinite one becomes a whole number."""
    app = SimulationContext(sim_id="Shop", factor=0)
    with caplog.at_level(logging.WARNING, logger="desflow.builder.context"):
        app.add_resource("packers", current_cap=7.0, max_cap=5)
        app.add_store("shelf", current_cap=7.0, max_cap=5)
    assert [m.rsplit(". ", 1)[1] for m in caplog.messages] == [
        "Using 5.0.",
        "Using 5.0.",
    ]


def test_a_hint_above_max_cap_says_to_raise_max_cap():
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=0.8, spawn="pack")
    app.add_service("packing", dist="exponential", rate=1.0)
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.task("packing", "packers", name="pack")(lambda task_id: {})
    texts = [h.text for h in app.analyze(target=0.5).hints if h.kind == "servers"]
    assert texts == [
        "packers with 2 units: utilisation 0.4, at most 0.5 "
        "(raise max_cap from 1 to 2 as well)"
    ]


def test_a_blueprint_current_cap_above_max_cap_is_clamped(tmp_path, caplog):
    blueprint = tmp_path / "shop.yaml"
    blueprint.write_text(
        "simulation: {sim_id: Shop, factor: 0}\n"
        "resources:\n  packers: {current_cap: 2, max_cap: 1}\n"
        "services:\n  packing: {dist: exponential, rate: 1.5}\n"
        "arrivals:\n  orders: {dist: exponential, rate: 0.8, spawn: pack}\n"
        "tasks:\n  pack: {resource: packers, service: packing, payload: {}}\n"
    )
    with caplog.at_level(logging.WARNING, logger="desflow.builder.context"):
        app = SimulationContext.from_yaml(blueprint)
    assert _by_name(app.analyze().resources, "packers").servers == 1
    assert "outside [0, max_cap 1]" in caplog.text


def test_get_put_preemption_and_several_resources_name_their_features():
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=0.8, spawn="pack")
    app.add_service("packing", dist="exponential", rate=1.0)
    app.add_resource("packers", queue="priority", preempt=True)
    app.add_resource("benches")
    app.add_container("boxes", level=10)
    app.add_store("parcels")
    app.task(
        "packing",
        ["packers", "benches"],
        name="pack",
        get={"container": "boxes"},
        put={"store": "parcels"},
    )(lambda task_id: {})
    reason = _by_name(app.analyze().resources, "packers").reason
    assert reason == (
        "task pack has no known rate: it has preemption, several resources, get, "
        "put, which the formulas leave out; run --verify to measure it"
    )


@pytest.mark.parametrize(
    "change", ["queue_limit", "type", "breakdown", "scenario", "ingress"]
)
def test_a_resource_or_model_feature_makes_the_wait_unsupported(change):
    from desflow.resources.resource import DynamicResource

    app = _shop()
    feature = {
        "queue_limit": "abandonment (queue_limit)",
        "type": "custom resource type",
        "breakdown": "breakdowns",
        "scenario": "scenario steps",
        "ingress": "live ingress",
    }[change]
    if change == "queue_limit":
        app.add_resource("packers", queue_limit=5)
    elif change == "type":
        app.add_resource("packers", type=type("Mine", (DynamicResource,), {}))
    elif change == "breakdown":
        app.add_breakdown(
            "packers",
            {"dist": "exponential", "rate": 0.01},
            {"dist": "constant", "value": 5},
        )
    elif change == "scenario":
        app.add_step(at=10, path="Shop.arrival.orders.rate", value=0.5)
    else:
        app.add_ingress(object())
    _check_feature(app, feature)


def test_a_replayed_arrival_is_unknown():
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival_replay("orders", [{"time": 1}], spawn="pack")
    app.task(name="pack")(lambda task_id: {})
    explanation = app.analyze()
    assert explanation.arrivals[0].reason == "replayed rows; run --verify to measure it"
    assert explanation.tasks[0].rate is None


def test_an_arrival_with_initial_alone_is_unknown(tmp_path):
    """An arrival with `initial` and no gap starts its entities when the run is built
    and draws no gaps: it is a node of the graph, with no rate, and the task it starts
    has none either."""
    path = tmp_path / "model.yaml"
    path.write_text(
        "simulation: {sim_id: C, factor: 0}\n"
        "arrivals:\n  workers: {initial: 2, spawn: t}\n"
        "tasks:\n  t: {payload: {}}\n",
        encoding="utf-8",
    )
    explanation = SimulationContext.from_yaml(path).analyze()
    features = {node.id: node.features for node in explanation.graph.nodes}
    assert features["arrival:workers"] == ["entities at the start only"]
    assert explanation.arrivals[0].reason == (
        "it starts only its initial entities, at the start of the run; run --verify to measure it"
    )
    assert explanation.tasks[0].rate is None


def test_a_hand_written_arrival_loop_and_a_process_are_opaque():
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=1.0)
    app.task(name="pack")(lambda task_id: {})

    @app.arrival_loop("orders")
    def orders(context):
        yield context.wait_for_arrival("orders")

    def drift(context):
        yield context.env.timeout(1)

    app.add_process(drift)
    app.add_step(at=5, record={"flash_sale": True})  # a record only, so no feature
    explanation = app.analyze()
    opaque = {n.label for n in explanation.graph.nodes if n.opaque}
    assert opaque == {"arrival_loop of orders", "process drift"}
    edges = {(e.source, e.target) for e in explanation.graph.edges}
    assert ("arrival:orders", "opaque:orders.arrival_loop") in edges
    assert explanation.graph.features == []
    assert explanation.tasks[0].reason == (
        "arrival_loop of orders is code that can start any task; run --verify to measure it"
    )


@pytest.mark.parametrize("name", ["pack", "process.pack"])
def test_a_task_named_like_a_process_is_not_read_as_one(name):
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=1)
        .add_resource("packers", current_cap=2, max_cap=4)
        .add_service("packing", dist="exponential", rate=1.0)
        .add_arrival("orders", dist="exponential", rate=0.5, spawn=name)
    )

    @app.task(
        service_id="packing", resource_id="packers", name=name, next=lambda up: None
    )
    def pack(task_id, context):
        return {"event_type": "packed"}

    explanation = app.analyze()
    assert explanation.arrivals[0].rate == 0.5
    assert explanation.arrivals[0].reason is None


def test_a_process_makes_the_resources_unsupported():
    app = _shop()

    def drift(context):
        yield context.env.timeout(1)

    app.add_process(drift)
    explanation = app.analyze()
    # The process can hold a packer, so the packers' load is not the tasks' alone
    packers = explanation.resources[0]
    assert packers.utilization is None and packers.wait is None
    assert packers.reason == (
        "task pack has no known rate: the formulas leave out process drift, which "
        "can change any work; run --verify to measure it"
    )
    assert explanation.hints == []


def test_a_next_function_makes_every_task_unknown():
    tasks = _flow(lambda task_id, payload: "ship")
    assert {t.rate for t in tasks.values()} == {None}
    assert tasks["pack"].reason == (
        "next of pack is code that can start any task; run --verify to measure it"
    )


def test_a_spawn_after_a_task_that_can_fail_is_unknown():
    app = _shop(fail="t >= 0", spawn={"task": "ship"})
    app.task(name="ship")(lambda task_id: {})
    explanation = app.analyze()
    pack = _by_name(explanation.tasks, "pack")
    assert pack.rate is None and "fail" in pack.reason
    ship = _by_name(explanation.tasks, "ship")
    assert ship.rate is None
    assert ship.reason == (
        "how often pack starts it is not known before a run; run --verify to measure it"
    )


def test_keep_makes_the_wait_unsupported_for_both_tasks():
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=0.5, spawn="pick")
    app.add_service("walking", dist="exponential", rate=2.0)
    app.add_resource("picker")
    app.task("walking", "picker", name="pick", keep=["picker"], next="carry")(
        lambda task_id: {}
    )
    app.task("walking", "picker", name="carry")(lambda task_id: {})
    explanation = app.analyze()
    for task in ("pick", "carry"):
        node = next(n for n in explanation.graph.nodes if n.id == f"task:{task}")
        assert "keep (a unit held across tasks)" in node.features
    picker = explanation.resources[0]
    assert picker.wait is None and "keep" in picker.wait_reason


@pytest.mark.parametrize("rush_rate", [1.0, 5.0])
def test_a_priority_queue_with_two_priorities_has_no_wait(rush_rate):
    """With one service time or two, ρ stays and the wait is unsupported."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("express", dist="exponential", rate=0.4, spawn="rush")
    app.add_arrival("normal", dist="exponential", rate=0.4, spawn="pack")
    app.add_service("rushing", dist="exponential", rate=rush_rate)
    app.add_service("packing", dist="exponential", rate=1.0)
    app.add_resource("packers", queue="priority")
    app.task("rushing", "packers", name="rush", priority=0)(lambda task_id: {})
    app.task("packing", "packers", name="pack", priority=1)(lambda task_id: {})
    packers = app.analyze().resources[0]
    assert packers.utilization == pytest.approx(0.4 / rush_rate + 0.4)
    assert packers.wait is None and packers.queue_length is None
    assert "priority queue" in packers.wait_reason


def test_a_put_without_a_resource_marks_everything_after_it():
    """A put into a full store holds the flow, so the packers after it have no
    supported load, and verify compares neither them nor the throughput."""
    from desflow.explain.pairs import pairs

    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=1.0, spawn="park")
    app.add_service("packing", dist="exponential", rate=2.0)
    app.add_resource("packers")
    app.add_store("shelf", current_cap=2, max_cap=2)
    app.task(name="park", put={"store": "shelf"}, next="pack")(lambda task_id: {})
    app.task("packing", "packers", name="pack")(lambda task_id: {})
    explanation = app.analyze()
    packers = explanation.resources[0]
    assert packers.utilization is None and packers.status is None
    assert "its work comes through park, whose put" in packers.reason
    found = {(p.node, p.measure): p for p in pairs(explanation, 100.0)}
    for key in (("packers", "utilization"), ("orders", "throughput")):
        assert found[key].analytical is None and "park" in found[key].reason


def test_a_supported_measure_with_no_reading_says_so():
    from desflow.analysis.intervals import interval
    from desflow.explain.compare import compare
    from desflow.explain.pairs import Pair

    row = compare(Pair("orders", "throughput", "x", 0.5, 1.0, None), None, interval)
    assert row.measured is None and row.differs is None
    assert row.reason == "no reading: nothing it measures happened in the run"


def test_replications_with_constant_times_agree_within_1_percent():
    """Every run gives the same value, so the interval has a width of 0."""
    from desflow.analysis.intervals import interval
    from desflow.explain.compare import compare
    from desflow.explain.pairs import Pair

    pair = Pair("packers", "utilization", "x", 0.75, 1.0, None)
    row = compare(pair, [0.74995] * 10, interval)
    assert row.half_width == 0 and row.differs is False
    assert compare(pair, [0.7] * 10, interval).differs is True


def test_a_resource_with_no_units_and_work_is_the_bottleneck():
    app = _shop(next="check")
    app.add_resource("packers", current_cap=0, max_cap=2)
    app.add_resource("checkers")
    app.task("packing", "checkers", name="check")(lambda task_id: {})
    explanation = app.analyze()
    assert _by_name(explanation.resources, "packers").status == "UNSTABLE"
    assert explanation.bottleneck == "packers"


def _loop(**rework):
    """Orders at 0.6/s to pack on the packers; half of the packed orders go to
    `rework` on the packers again, with `rework`'s keys, and half to ship."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=0.6, spawn="pack")
    app.add_service("packing", dist="exponential", rate=1.0)
    app.add_resource("packers")
    app.add_resource("shippers")
    app.task(
        "packing", "packers", name="pack", next={"weights": {"ship": 1, "rework": 1}}
    )(lambda task_id: {})
    app.task("packing", "packers", name="rework", next="pack", **rework)(
        lambda task_id: {}
    )
    app.task("packing", "shippers", name="ship")(lambda task_id: {})
    return app.analyze()


def test_an_unstable_resource_in_a_rework_loop_keeps_its_status():
    """The loop reaches the packers' own tasks, which keep their rates; only the
    tasks after them lose theirs."""
    explanation = _loop()
    packers = _by_name(explanation.resources, "packers")
    assert packers.utilization == pytest.approx(1.8) and packers.status == "UNSTABLE"
    assert explanation.bottleneck == "packers"
    shippers = _by_name(explanation.resources, "shippers")
    assert shippers.utilization is None
    assert "its work comes through unstable packers" in shippers.reason


def test_a_priority_queue_in_a_rework_loop_keeps_its_utilisation():
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=0.3, spawn="pack")
    app.add_service("packing", dist="exponential", rate=1.0)
    app.add_resource("packers", queue="priority")
    app.task(
        "packing",
        "packers",
        name="pack",
        priority=1,
        next={"weights": {"ship": 1, "rework": 1}},
    )(lambda task_id: {})
    app.task("packing", "packers", name="rework", next="pack", priority=0)(
        lambda task_id: {}
    )
    app.task(name="ship")(lambda task_id: {})
    packers = app.analyze().resources[0]
    assert packers.utilization == pytest.approx(0.9) and packers.status is not None
    assert packers.wait is None and "priority queue" in packers.wait_reason


def test_a_clamped_normal_uses_its_effective_mean():
    app = _shop(service={"dist": "normal", "mean": 0.5, "std": 1.0})
    mean, _ = clamped_normal_moments(0.5, 1.0)
    packers = _by_name(app.analyze().resources, "packers")
    assert mean > 0.5
    assert packers.utilization == pytest.approx(0.8 * mean)


def test_a_gap_of_zero_has_no_rate():
    """A constant gap of 0 stays 0, so the arrival has no rate to divide by, and what
    depends on it is unknown rather than a division by zero."""
    explanation = _shop(arrival={"dist": "constant", "value": 0}).analyze()
    arrival = explanation.arrivals[0]
    assert arrival.rate is None and arrival.reason == "a gap of 0 has no rate"
    assert _by_name(explanation.resources, "packers").utilization is None


def test_capacity_rows():
    small = _by_name(_shop(packers=1, most=4).analyze().resources, "packers")
    assert [(r.capacity, r.status) for r in small.capacities] == [
        (1, "OK"),
        (2, "OK"),
        (3, "OK"),
        (4, "OK"),
    ]
    # 30 units of work: stable from 31, below 0.85 from 36, now 40, at most 50
    large = _shop({"dist": "exponential", "rate": 30.0}, packers=40, most=50).analyze()
    rows = _by_name(large.resources, "packers").capacities
    assert [r.capacity for r in rows] == [31, 36, 40, 50]


def test_hints_for_a_target_below_and_above_1():
    below = _shop({"dist": "exponential", "rate": 2.7}, packers=1).analyze()
    assert [(h.kind, h.value) for h in below.hints] == [
        ("servers", 4),
        ("rate", pytest.approx(0.85)),
    ]
    assert below.hints[1].text == (
        "packers with 1 unit: utilisation 0.85 at a request every 1.18 seconds "
        "(51 a minute, 0.85 a second)"
    )
    above = _shop({"dist": "exponential", "rate": 2.7}, packers=1).analyze(target=1.2)
    assert [(h.kind, h.value) for h in above.hints] == [
        ("servers", 2),
        ("rate", pytest.approx(1.2)),
    ]
    assert above.hints[1].text.endswith("utilisation 1.2 at 1.2 requests a second")
    assert _requests(1.0) == "1 request a second"
    assert (
        _requests(0.996) == "a request every 1 second (59.76 a minute, 0.996 a second)"
    )
    with pytest.raises(ValueError, match="target"):
        _shop().analyze(target=0)


class _Records:
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _records(context):
    capture = _Records()
    context.add_egress(capture)
    context.run(until=50)
    return capture.records


def test_analyze_does_not_start_the_run():
    analyzed = _shop()
    analyzed.analyze()
    assert analyzed.started is False
    assert _records(analyzed) == _records(_shop())


def test_verify_refuses_an_end_inside_the_warm_up(tmp_path):
    path = tmp_path / "warm.yaml"
    path.write_text(
        MM1.replace("random_seed: 7}", "random_seed: 7, warmup: 100}"),
        encoding="utf-8",
    )
    context = SimulationContext.from_yaml(path)
    with pytest.raises(ValueError, match=r"^verify measures after the warm-up"):
        context.analyze(verify=True, until=50)


def test_verify_refuses_a_context_built_in_code():
    with pytest.raises(ValueError, match="verify needs a model it can build again"):
        _shop().analyze(verify=True, until=100)


def test_verify_of_a_low_level_model_says_it_cannot_be_built_again(tmp_path):
    from desflow import LowLevelModel
    from desflow.explain import verify

    path = tmp_path / "mm1.yaml"
    path.write_text(MM1, encoding="utf-8")
    explanation = SimulationContext.from_yaml(path).analyze()
    model = LowLevelModel(lambda env, sampler: [], "S")
    with pytest.raises(ValueError, match="a LowLevelModel cannot be verified"):
        verify(model, explanation, until=10)


@pytest.mark.parametrize("replications", [2.5, 0, 1, True, float("nan")])
def test_verify_refuses_replications_that_are_not_a_whole_number_of_2_or_more(
    tmp_path, replications
):
    from desflow.explain import verify

    path = tmp_path / "mm1.yaml"
    path.write_text(MM1, encoding="utf-8")
    explanation = SimulationContext.from_yaml(path).analyze()
    with pytest.raises(ValueError, match="replications is a whole number of 2"):
        verify(str(path), explanation, until=100, replications=replications)


def test_verify_agrees_on_the_mm1_shop(tmp_path):
    path = tmp_path / "mm1.yaml"
    path.write_text(MM1, encoding="utf-8")
    context = SimulationContext.from_yaml(path)
    explanation = context.analyze(verify=True, replications=10)
    verification = explanation.verification
    found = {(c.node, c.measure): c for c in verification.comparisons}
    for key in (("packers", "utilization"), ("pack", "wait")):
        assert found[key].differs is False, found[key]
    assert verification.replications == 10
    assert "does not show that the model is close enough" in verification.note


def _verify(tmp_path, text, until=3000):
    path = tmp_path / "shop.yaml"
    path.write_text(text, encoding="utf-8")
    explanation = SimulationContext.from_yaml(path).analyze(verify=True, until=until)
    return {(c.node, c.measure): c for c in explanation.verification.comparisons}


def test_verify_does_not_compare_where_a_feature_applies(tmp_path):
    found = _verify(tmp_path, MM1.replace("payload: {}", "payload: {}, patience: 2"))
    for key in (("packers", "utilization"), ("pack", "wait"), ("orders", "throughput")):
        assert found[key].analytical is None and found[key].differs is None
    assert found[("packers", "utilization")].reason == (
        "task pack has no known rate: it has abandonment (patience), which the "
        "formulas leave out"
    )
    assert found[("orders", "throughput")].reason == (
        "task pack: it has abandonment (patience), which the formulas leave out"
    )


def test_not_estimated_drops_the_advice_to_run_verify_while_verify_runs(tmp_path):
    from desflow.explain.render import render_terminal

    path = tmp_path / "shop.yaml"
    path.write_text(MM1.replace("payload: {}", "payload: {}, patience: 2"))
    context = SimulationContext.from_yaml(path)
    line = "  task pack: it has abandonment (patience), which the formulas leave out"
    alone = render_terminal(context.analyze(), width=200).splitlines()
    assert line + "; run --verify to measure it" in alone
    verified = render_terminal(context.analyze(verify=True, until=500), width=200)
    assert line in verified.splitlines() and "run --verify" not in verified


def test_verify_reads_a_flow_with_no_finished_entity_as_0(tmp_path):
    """One arrival every 10 s on average over 5 s: some replications finish no
    entity and publish no `flows.orders.finished`, which is 0 in them, not missing."""
    path = tmp_path / "rare.yaml"
    path.write_text(
        MM1.replace("rate: 0.8", "rate: 0.1").replace("rate: 1}", "rate: 2}"),
        encoding="utf-8",
    )
    context = SimulationContext.from_yaml(path)
    explanation = context.analyze(verify=True, until=5, replications=10)
    found = {(c.node, c.measure): c for c in explanation.verification.comparisons}
    throughput = found[("orders", "throughput")]
    assert throughput.measured is not None, throughput
    assert "no reading" not in (throughput.reason or "")


def test_verify_does_not_compare_after_an_unstable_resource(tmp_path):
    text = MM1.replace(
        "packers: {current_cap: 1, max_cap: 4}",
        "packers: {current_cap: 0, max_cap: 4}\n  checkers: {current_cap: 1}",
    ).replace(
        "pack: {service: packing, resource: packers, payload: {}}",
        "pack: {service: packing, resource: packers, payload: {}, next: check}\n"
        "  check: {service: packing, resource: checkers, payload: {}}",
    )
    found = _verify(tmp_path, text, until=500)
    assert found[("packers", "utilization")].reason == (
        "unstable: it has no steady state to compare with"
    )
    assert found[("checkers", "utilization")].reason == (
        "task check has no known rate: its work comes through unstable packers"
    )
    assert all(c.differs is None for c in found.values())


def test_verify_does_not_compare_a_throughput_after_run_stop_ends_the_run(tmp_path):
    """The signal fires at 500 s, so each run is a quarter of `until` long."""
    text = (
        MM1.replace(
            "arrivals:\n",
            "arrivals:\n  closer: {dist: constant, value: 500, max_events: 1, spawn: close}\n",
        )
        .replace(
            "tasks:\n",
            "signals: {closing: {}}\ntasks:\n  close: {signal: closing, payload: {}}\n",
        )
        .replace("run: {until: 20000}", "run: {until: 2000, stop: {signal: closing}}")
    )
    found = _verify(tmp_path, text, until=2000)
    throughput = found[("orders", "throughput")]
    assert throughput.analytical is None and throughput.differs is None
    assert throughput.reason == (
        "run.stop can end a run before until, so a run has no fixed time to divide by"
    )
    assert found[("packers", "utilization")].analytical is not None

    from desflow.explain.render import render_terminal

    explanation = SimulationContext.from_yaml(tmp_path / "shop.yaml").analyze(
        verify=True, until=2000
    )
    assert "Verify: 1 run to run.stop or 2000 s, seed 7" in render_terminal(explanation)


def test_names_with_square_brackets_are_printed_as_written(tmp_path):
    from desflow.explain.render import render_terminal

    path = tmp_path / "names.yaml"
    path.write_text(
        MM1.replace("  packers:", '  "[bold]packers":')
        .replace("resource: packers", 'resource: "[bold]packers"')
        .replace("orders:", '"orders[/x]":')
        .replace("  pack:", '  "[dim]pack":')
        .replace("spawn: pack", 'spawn: "[dim]pack"'),
        encoding="utf-8",
    )
    text = render_terminal(SimulationContext.from_yaml(path).analyze(), width=120)
    for name in ("[bold]packers", "orders[/x]", "[dim]pack"):
        assert name in text
    assert "Bottleneck: [bold]packers" in text


def test_a_rate_written_1_and_1_0_is_one_service_rate_for_erlang_c(tmp_path):
    texts = [
        f"""\
simulation: {{sim_id: Shop, random_seed: 1}}
resources: {{packers: {{current_cap: 2, max_cap: 2}}}}
services:
  a: {{dist: exponential, rate: 1}}
  b: {{dist: exponential, rate: {second}}}
arrivals:
  x: {{dist: exponential, rate: 0.5, spawn: pa}}
  y: {{dist: exponential, rate: 0.5, spawn: pb}}
tasks:
  pa: {{service: a, resource: packers, payload: {{}}}}
  pb: {{service: b, resource: packers, payload: {{}}}}
run: {{until: 100}}
"""
        for second in ("1", "1.0")
    ]
    waits = []
    for number, text in enumerate(texts):
        path = tmp_path / f"shop{number}.yaml"
        path.write_text(text)
        waits.append(SimulationContext.from_yaml(path).analyze().resources[0].wait)
    assert waits[0] == waits[1] and waits[0] is not None


def test_json_validates_and_the_committed_schema_is_current():
    from desflow.explain.render import to_json

    text = to_json(_shop().analyze())
    assert json.loads(text)["schema_version"] == "0"
    Explanation.model_validate_json(text)
    committed = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert committed == Explanation.model_json_schema(), (
        "The committed schema is out of date; run `uv run python scripts/gen_schema.py`"
    )


@pytest.mark.parametrize(
    "gap, refusal",
    [
        ("dist: exponential, rate: 1e-200", "exponential with rate 1e-200"),
        (f"dist: constant, value: {10**160}", f"constant with value {10**160}"),
        (
            f"dist: normal, mean: {10**160}, std: 1",
            f"normal with mean {10**160}, std 1",
        ),
    ],
    ids=["a float rate", "an integer constant", "an integer normal mean"],
)
def test_an_arrival_gap_too_large_for_a_float_is_refused_without_a_traceback(
    tmp_path, gap, refusal
):
    (tmp_path / "tiny.yaml").write_text(
        MM1.replace("dist: exponential, rate: 0.8", gap), encoding="utf-8"
    )
    result = CliRunner().invoke(cli, ["explain", str(tmp_path / "tiny.yaml")])
    assert result.exit_code == 1, result.output
    assert (
        f"arrival orders: {refusal} has a mean or variance too large for a float"
        in result.output
    )
    assert "Traceback" not in result.output


def test_a_service_rate_too_small_for_a_float_names_the_task(tmp_path):
    (tmp_path / "tiny.yaml").write_text(
        MM1.replace("rate: 1}", "rate: 1e-200}"), encoding="utf-8"
    )
    result = CliRunner().invoke(cli, ["explain", str(tmp_path / "tiny.yaml")])
    assert result.exit_code == 1, result.output
    assert "task pack: exponential with rate 1e-200" in result.output


def test_the_command_prints_the_tree_and_the_resources(tmp_path):
    (tmp_path / "mm1.yaml").write_text(MM1, encoding="utf-8")
    result = CliRunner().invoke(cli, ["explain", str(tmp_path / "mm1.yaml")])
    assert result.exit_code == 0, result.output
    assert "spawn -> pack (task, rate 0.8/s)" in result.stdout
    assert "Bottleneck: packers" in result.stdout
    as_json = CliRunner().invoke(cli, ["explain", str(tmp_path / "mm1.yaml"), "--json"])
    assert (
        Explanation.model_validate_json(as_json.stdout).resources[0].utilization == 0.8
    )


def test_the_command_loads_a_builder_file(tmp_path):
    (tmp_path / "shop.py").write_text(
        textwrap.dedent("""\
        from desflow import SimulationContext

        app = SimulationContext(sim_id="Shop", factor=0)
        app.add_arrival("orders", dist="exponential", rate=0.8, spawn="pack")
        app.add_service("packing", dist="exponential", rate=1)
        app.add_resource("packers")
        app.task("packing", "packers", name="pack")(lambda task_id: {})
        """),
        encoding="utf-8",
    )
    result = CliRunner().invoke(cli, ["explain", str(tmp_path / "shop.py"), "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["resources"][0]["utilization"] == 0.8


@pytest.mark.parametrize(
    "spawn, task, message",
    [
        ("pak", "", "arrival 'orders' spawns task 'pak', which is not added"),
        (
            "pack",
            ', next="shipp"',
            "task 'pack' routes to 'shipp', which is not a task",
        ),
    ],
)
def test_a_builder_file_naming_no_task_fails_as_run_does(
    tmp_path, spawn, task, message
):
    from desflow.loading import load_context

    (tmp_path / "shop.py").write_text(
        textwrap.dedent(f"""\
        from desflow import SimulationContext

        app = SimulationContext(sim_id="Shop", factor=0)
        app.add_arrival("orders", dist="exponential", rate=0.8, spawn="{spawn}")
        app.add_service("packing", dist="exponential", rate=1)
        app.add_resource("packers")
        app.task("packing", "packers", name="pack"{task})(lambda task_id: {{}})
        """),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=message):
        load_context(str(tmp_path), "shop.py").analyze()
    result = CliRunner().invoke(cli, ["explain", str(tmp_path / "shop.py")])
    assert result.exit_code == 1
    assert f"Error: {message}" in result.stderr


@pytest.mark.parametrize(
    "file, arguments, message",
    [
        (None, [], "no file"),
        ("low_level.py", [], "defines no SimulationContext"),
        ("no_until.yaml", ["--verify"], "pass --until"),
        ("no_until.yaml", ["--seed", "1"], "apply to --verify"),
        ("no_until.yaml", ["--target", "11"], "--target"),
        ("no_until.yaml", ["--verify", "--until", "0"], "initial_time + warmup (0)"),
        ("mm1.yaml", ["--verify", "--until", "1000"], "(2000); got until 1000"),
    ],
)
def test_the_command_exits_2(tmp_path, file, arguments, message):
    (tmp_path / "low_level.py").write_text("x = 1\n", encoding="utf-8")
    (tmp_path / "no_until.yaml").write_text(
        MM1.replace("run: {until: 20000}\n", ""), encoding="utf-8"
    )
    (tmp_path / "mm1.yaml").write_text(
        MM1.replace("random_seed: 7}", "random_seed: 7, warmup: 2000}"),
        encoding="utf-8",
    )
    source = str(tmp_path / (file or "missing.yaml"))
    result = CliRunner().invoke(cli, ["explain", source, *arguments])
    assert result.exit_code == 2
    assert message in result.stderr


def test_on_renege_and_on_balk_edges_have_no_probability():
    """A task with `on_renege` or `on_balk` can leave its route, so its edges carry
    no share, and the target of each is a next edge."""
    app = _shop(patience=5, balk="t > 5", on_renege="gave_up", on_balk="turned_away")
    for name in ("gave_up", "turned_away"):
        app.task(name=name)(lambda task_id: {})
    explanation = app.analyze()
    edges = [
        e
        for e in explanation.graph.edges
        if e.source == "task:pack" and e.kind == "next"
    ]
    assert {e.target for e in edges} >= {"task:gave_up", "task:turned_away"}
    assert all(e.probability is None for e in edges)
    assert _by_name(explanation.tasks, "gave_up").rate is None


@pytest.mark.parametrize("ship", [1, "attr.w"])
def test_weights_on_a_task_that_can_divert_end_no_flow(ship):
    """Fixed weights, like weights read when the task finishes, name every route, so
    a task with `on_renege` and weights has no edge to the exit."""
    app = _shop(
        patience=5, on_renege="lost", next={"weights": {"ship": ship, "hold": 1}}
    )
    for name in ("ship", "hold", "lost"):
        app.task(name=name)(lambda task_id: {})
    targets = {e.target for e in app.analyze().graph.edges if e.source == "task:pack"}
    assert "exit" not in targets
    assert {"task:ship", "task:hold", "task:lost"} <= targets


STARTS = """\
simulation: {{sim_id: S, factor: 0}}
signals: {{done: {{on: [{{spawn: {{task: side}}}}]}}, ping: {{}}}}
arrivals: {{a: {{dist: exponential, rate: 0.5, spawn: work}}}}
tasks:
  work: {{service_time: 1, payload: {{}}, {keys}}}
  side: {{payload: {{}}}}
"""


@pytest.mark.parametrize(
    "keys, kind",
    [
        ("on_interrupt: abandon, on_interrupt_next: side", "next"),
        ("on_interrupt: resume, on_interrupt_run: side", "next"),
        ("on: [{spawn: {task: side}}]", "spawn"),
        ("listen: {signal: ping, on: [{spawn: {task: side}}]}", "spawn"),
        ("wait: {task: side}", "spawn"),
        ("subscribe: {task: side}", "spawn"),
        ("open: {child: {task: side}}", "spawn"),
        ("signal: done", "spawn"),
    ],
)
def test_every_way_a_task_starts_another_is_an_edge_with_no_share(tmp_path, keys, kind):
    """Each way `work` starts `side`, other than `spawn:` and its routes, is an edge
    with no share, so the rate of `side` is unknown because of `work`, not because
    nothing starts it."""
    path = tmp_path / "model.yaml"
    path.write_text(STARTS.format(keys=keys))
    explanation = SimulationContext.from_yaml(path).analyze()
    edges = [e for e in explanation.graph.edges if e.target == "task:side"]
    assert [(e.source, e.kind, e.probability) for e in edges] == [
        ("task:work", kind, None)
    ]
    side = _by_name(explanation.tasks, "side")
    assert side.rate is None and "work" in side.reason


INTERRUPTED = """\
simulation: {{sim_id: S, factor: 0}}
signals: {{stop: {{on: [{{interrupt: {{task: work}}}}]}}}}
arrivals:
  a: {{dist: exponential, rate: 0.5, spawn: work}}
  b: {{dist: exponential, rate: 0.1, spawn: boss}}
tasks:
  work: {{service_time: 1, payload: {{}}, on_interrupt: abandon}}
  boss: {{payload: {{}}, {keys}}}
{scenario}"""


@pytest.mark.parametrize(
    "keys, scenario",
    [
        ("interrupt: {task: work}", ""),
        ("on: [{interrupt: {task: work}}]", ""),
        ("signal: stop", ""),
        ("service_time: 1", "scenario: [{at: 5, interrupt: {task: work}}]\n"),
    ],
)
def test_a_task_an_interrupt_names_has_a_feature(tmp_path, keys, scenario):
    """A task that an `interrupt` of a task, an `on:` entry of a task or a signal, or
    a scenario step names ends some services early, so it has a feature."""
    path = tmp_path / "model.yaml"
    path.write_text(INTERRUPTED.format(keys=keys, scenario=scenario))
    explanation = SimulationContext.from_yaml(path).analyze()
    nodes = {node.id: node for node in explanation.graph.nodes}
    assert "interruptions (an interrupt names it)" in nodes["task:work"].features
    assert "interruptions (an interrupt names it)" not in nodes["task:boss"].features


COMPUTED = """\
simulation: {sim_id: C, factor: 0}
resources: {m: {current_cap: 1}}
arrivals:
  gapped: {gap: 2, spawn: t}
  limited: {dist: exponential, rate: 1, max_events: 3, spawn: t}
tasks:
  t:
    resource: m
    service_time: 1
    payload: {}
    next: {weights: {u: "attr.p", v: 1}}
  u: {payload: {}}
  v: {payload: {}}
"""


def test_an_arrival_key_that_can_repeat_leaves_the_rate_out(tmp_path):
    """A start whose key a running entity holds is skipped, so the arrival's rate is
    not the distribution's. A key that never repeats keeps the rate."""
    text = (
        "simulation: {sim_id: C, factor: 0}\n"
        "arrivals:\n"
        "  fixed: {dist: exponential, rate: 1, key: k, spawn: t}\n"
        "  numbered: {dist: exponential, rate: 1, key: {gen: sequence}, spawn: t}\n"
        "tasks:\n  t: {payload: {}}\n"
    )
    path = tmp_path / "model.yaml"
    path.write_text(text, encoding="utf-8")
    result = SimulationContext.from_yaml(path).analyze()
    by_name = {a.name: a for a in result.arrivals}
    assert by_name["fixed"].reason.startswith("count, keys, key or max_events")
    assert by_name["fixed"].rate is None
    assert by_name["numbered"].rate == 1.0


def test_computed_times_and_arrivals_leave_the_formulas_out(tmp_path):
    """A `gap`, a `max_events`, a `service_time` and expression weights are read when
    the run needs them, so the formulas give no rate or wait for them, and a router
    with expression weights does not end the flow."""
    path = tmp_path / "model.yaml"
    path.write_text(COMPUTED, encoding="utf-8")
    graph = SimulationContext.from_yaml(path).analyze().graph
    features = {node.id: node.features for node in graph.nodes}

    assert features["arrival:gapped"] == ["computed gaps"]
    assert features["arrival:limited"] == ["limited or repeated arrivals"]
    assert features["task:t"] == ["computed service time"]
    edges = {(e.source, e.target) for e in graph.edges}
    assert ("task:t", "exit") not in edges
    assert ("task:u", "exit") in edges
    result = SimulationContext.from_yaml(path).analyze()
    reasons = {a.name: a.reason for a in result.arrivals}
    assert reasons["gapped"].startswith("computed gaps")
    assert reasons["limited"].startswith("count, keys, key or max_events")


def test_a_deep_graph_prints_every_task_at_width_80(tmp_path):
    """Rich drops the nodes that no longer fit beside the indent of a deep tree."""
    from desflow.explain.render import render_terminal

    count = 30
    tasks = "".join(
        f"  t{i}: {{payload: {{}}"
        + (f", next: t{i + 1}" if i < count - 1 else "")
        + "}\n"
        for i in range(count)
    )
    path = tmp_path / "chain.yaml"
    path.write_text(
        "simulation: {sim_id: Chain, factor: 0, random_seed: 1}\n"
        "arrivals:\n  a: {dist: constant, value: 1, spawn: t0}\n"
        f"tasks:\n{tasks}run: {{until: 50}}\n"
    )
    text = render_terminal(SimulationContext.from_yaml(path).analyze(), width=80)
    for i in range(count):
        assert f"-> t{i} (task" in text or f"t{i} (task" in text

"""`desflow explain` and `context.analyze()`: the graph, the analysis against the
queueing formulas, `--verify`, the JSON and its schema, and the command's exits."""

import asyncio
import json
import math
import queue
import textwrap
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from desflow import SimulationContext
from desflow.cli import app as cli
from desflow.core.distributions import clamped_normal_moments
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
    ],
)
def test_a_task_feature_makes_the_wait_unsupported(task, feature):
    app = _shop(**task)
    app.add_signal("done")
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
    assert "the share that fails is not known before a run" in ship.reason


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


def test_json_validates_and_the_committed_schema_is_current():
    from desflow.explain.render import to_json

    text = to_json(_shop().analyze())
    assert json.loads(text)["schema_version"] == "0"
    Explanation.model_validate_json(text)
    committed = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert committed == Explanation.model_json_schema(), (
        "The committed schema is out of date; run `uv run python scripts/gen_schema.py`"
    )


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

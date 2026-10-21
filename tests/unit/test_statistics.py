"""Built-in statistics, the warm-up and ground-truth records."""

import asyncio
import queue
import textwrap

import numpy as np
import pytest

from desflow import SimulationContext
from desflow.blueprint import BlueprintError


class _Capture:
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _shop(level, rate=0.5, service=None, seed=11, interval=10.0, **kwargs):
    app = (
        SimulationContext("Shop", factor=0, random_seed=seed, **kwargs)
        .add_resource("packers", 1, 2)
        .add_service("packing", **(service or {"dist": "exponential", "rate": 1.0}))
        .add_arrival("orders", "exponential", rate=rate)
        .with_stats(level, interval=interval)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id, context):
        return {}

    def loop(context):
        number = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack(number, context, entity=entity))
            number += 1

    app.add_process(loop)
    return app


def test_mm1_utilization_queue_and_wait_match_the_formulas():
    # M/M/1 with arrival rate 0.5 and service rate 1: utilization 0.5, mean queue
    # length rho^2 / (1 - rho) = 0.5, mean wait rho / (mu - lambda) = 1
    app = _shop("typical")
    app.run(until=40_000)
    readings = app.env.stats.readings()
    prefix = "Shop.stats"
    assert readings[f"{prefix}.resources.packers.utilization_avg"] == pytest.approx(
        50, abs=2.5
    )
    assert readings[f"{prefix}.resources.packers.queue_length_avg"] == pytest.approx(
        0.5, abs=0.1
    )
    assert readings[f"{prefix}.tasks.pack.wait_mean"] == pytest.approx(1.0, abs=0.12)
    assert readings[f"{prefix}.tasks.pack.service_mean"] == pytest.approx(1.0, abs=0.05)
    assert readings[f"{prefix}.tasks.pack.finished"] == pytest.approx(20_000, rel=0.03)
    assert (
        readings[f"{prefix}.flows.orders.finished"]
        == (readings[f"{prefix}.tasks.pack.finished"])
    )


def test_utilization_counts_busy_units_across_capacity_changes():
    app = (
        SimulationContext("Shop", factor=0, random_seed=1)
        .add_resource("packers", current_cap=2, max_cap=2)
        .add_service("packing", "constant", value=100)
        .with_stats("basic", interval=1000)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id, context):
        return {}

    def orders(context):
        # One order at 0.25 seconds and one at 0.5; both pack for 100 seconds
        for number in range(2):
            yield context.env.timeout(0.25)
            context.spawn(pack(number, context))

    def changes(context):
        yield context.env.timeout(10)
        context.env.registry.update("Shop.resources.packers.current_cap", 1)
        yield context.env.timeout(10)
        context.env.registry.update("Shop.resources.packers.current_cap", 2)

    app.add_process(orders)
    app.add_process(changes)
    app.run(until=30)
    # From 10 to 20 the capacity is 1 while both units stay busy: 200%, which
    # capacity - pool.level would hide
    expected = (0.25 * 0 + 0.25 * 50 + 9.5 * 100 + 10 * 200 + 10 * 100) / 30
    assert app.env.stats.readings()[
        "Shop.stats.resources.packers.utilization_avg"
    ] == pytest.approx(expected)


def test_store_and_container_readings_and_level_average():
    app = (
        SimulationContext("Shop", factor=0, random_seed=1)
        .add_store("parcels", current_cap=1, max_cap=1)
        .add_container("stock", current_cap=10, max_cap=10, level=10)
        .with_stats("basic", interval=1000)
    )

    def model(context):
        parcels, stock = context.get_store("parcels"), context.get_container("stock")
        yield context.env.timeout(5)
        yield stock.get(4)
        parcels.put("a")
        parcels.put("b")  # waits: the store holds one item
        stock.get(50)  # waits: more than the stock holds

    app.add_process(model)
    app.run(until=10)
    readings = {
        name.removeprefix("Shop.stats."): value
        for name, value in app.env.stats.readings().items()
    }
    assert readings == {
        "stores.parcels.capacity": 1,
        "stores.parcels.level": 1,
        "stores.parcels.getters_waiting": 0,
        "stores.parcels.putters_waiting": 1,
        "stores.parcels.level_avg": 0.5,
        "containers.stock.capacity": 10.0,
        "containers.stock.level": 6.0,
        "containers.stock.getters_waiting": 1,
        "containers.stock.putters_waiting": 0,
        "containers.stock.level_avg": 8.0,
    }


def test_each_level_adds_its_statistics():
    names = {}
    readings = {}
    for level in ("minimal", "basic", "typical", "all"):
        app = _shop(level)
        app.run(until=500)
        readings[level] = app.env.stats.readings()
        names[level] = {name.removeprefix("Shop.stats.") for name in readings[level]}

    assert names["minimal"] == {
        "resources.packers.capacity",
        "resources.packers.in_use",
        "resources.packers.queue_length",
        "resources.packers.utilization",
        "tasks.pack.in_progress",
    }
    assert names["basic"] - names["minimal"] == {
        "resources.packers.utilization_avg",
        "resources.packers.queue_length_avg",
        "tasks.pack.finished",
        "flows.orders.finished",
        "flows.orders.started",
        "flows.orders.in_system",
    }
    # Every order that started is finished or still in the shop
    r = readings["basic"]
    assert r["Shop.stats.flows.orders.started"] == (
        r["Shop.stats.flows.orders.finished"] + r["Shop.stats.flows.orders.in_system"]
    )
    assert (
        r["Shop.stats.flows.orders.in_system"] == r["Shop.stats.tasks.pack.in_progress"]
    )
    assert names["typical"] - names["basic"] == {
        f"tasks.pack.{kind}_{stat}"
        for kind in ("wait", "service")
        for stat in ("mean", "max")
    }
    assert names["all"] - names["typical"] == {
        f"tasks.pack.{kind}_p{point}"
        for kind in ("wait", "service")
        for point in (50, 95, 99)
    }
    # Percentiles are exact: NumPy's percentile of every kept observation
    app = _shop("all")
    app.run(until=500)
    waits = app.env.stats._tasks["pack"].wait.values
    assert app.env.stats.readings()["Shop.stats.tasks.pack.wait_p95"] == float(
        np.percentile(waits, 95)
    )
    assert readings["all"]["Shop.stats.tasks.pack.wait_max"] == max(waits)


def test_the_warmup_leaves_early_records_out_of_every_sink_and_resets_the_stats():
    app = _shop("basic", warmup="1 min", interval=30)
    data, labels = _Capture(), _Capture()
    app.add_egress(data)
    app.add_egress(labels, when="labels")

    def faults(context):
        for at in (30, 90):
            yield context.env.timeout(at - context.env.now)
            context.env.publish_label({"at": at})

    app.add_process(faults)
    app.run(until=120)

    assert min(r["sim_ts"] for r in data.records) == 60
    assert [r["label"] for r in labels.records] == [{"at": 90}]
    finished = {
        r["sim_ts"]: r["value"]
        for r in data.records
        if r.get("path_id") == "Shop.stats.tasks.pack.finished"
    }
    after = sum(
        1
        for r in data.records
        if r["stream_type"] == "event" and r["value"] == {} and r["sim_ts"] <= 120
    )
    # Counted from the end of the warm-up only
    assert finished[120.0] == after


SCENARIO = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 1}
egress:
  - type: Console
  - type: Console
    when: labels
services:
  packing: {dist: constant, value: 1}
scenario:
  - {at: 5, path: Shop.service.packing.value, value: 2, record: {fault: slow}}
  - {at: 7, record: {note: checked}}
  - {at: 9, path: Shop.service.packing.value, value: 3}
run: {until: 10}
"""


def test_ground_truth_reaches_only_the_labels_sink(tmp_path):
    path = tmp_path / "labels.yaml"
    path.write_text(SCENARIO, encoding="utf-8")
    app = SimulationContext.from_yaml(path)
    data, labels = _Capture(), _Capture()
    app._egress_providers[:] = [data, labels]
    app.run()

    assert [
        {k: v for k, v in r.items() if k != "timestamp"} for r in labels.records
    ] == [
        {
            "stream_type": "label",
            "sim_ts": 5.0,
            "path": "Shop.service.packing.value",
            "old": 1.0,
            "new": 2.0,
            "label": {"fault": "slow"},
        },
        {
            "stream_type": "label",
            "sim_ts": 7.0,
            "path": None,
            "old": None,
            "new": None,
            "label": {"note": "checked"},
        },
    ]
    assert all(r["stream_type"] != "label" for r in data.records)
    assert app.env.registry.get("Shop.service.packing.value").value == 3.0


@pytest.mark.parametrize(
    "text,line,fragment",
    [
        ("stats: busy\n", 9, "level must be one of"),
        ("stats: {level: all, interval: 0}\n", 9, "interval"),
        ("scenario:\n  - {at: 1}\n", 10, "a path and value, a record"),
        (
            "scenario:\n  - {at: 1, path: Shop.service.packing.value}\n",
            10,
            "a path needs a value",
        ),
    ],
)
def test_stats_and_scenario_errors_name_the_line(tmp_path, text, line, fragment):
    base = SCENARIO.split("scenario:")[0] + "run: {until: 10}\n"
    path = tmp_path / "bad.yaml"
    path.write_text(textwrap.dedent(base + text), encoding="utf-8")
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    message = str(error.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert fragment in message


def test_a_task_refused_after_it_queued_leaves_the_counts():
    """A task that waits first, then asks a resource whose line is full, is counted
    as balked and no longer in progress."""
    app = SimulationContext("Shop", factor=0)
    app.add_resource("packers", 1, 1, queue_limit=0)
    app.add_service("packing", dist="constant", value=1.0)
    app.with_stats("typical", interval=100.0)

    @app.task(
        service_id="packing",
        resource_id="packers",
        wait={"timeout": 1},
        name="pack",
    )
    def pack(task_id, context):
        return {}

    def script(context):
        context.spawn(pack(0))
        yield context.env.timeout(5)

    app.add_process(script)
    app.run(until=5)
    readings = app.env.stats.readings()
    assert readings["Shop.stats.tasks.pack.balked"] == 1
    assert readings["Shop.stats.tasks.pack.in_progress"] == 0

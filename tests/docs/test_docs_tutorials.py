"""The tutorial pages' code runs, in every tab, and gives the answer each page states.

Each page of Tutorial: the Shop shows sections of three files: a blueprint under
docs/snippets/yaml/, a builder script and a low-level script under
docs/snippets/tutorials/. test_docs_match_examples.py keeps the copies equal to them.
This runs all three unpaced and checks the number the page gives as its answer, so a
change to the code that changes the answer fails here until the page is updated.
"""

import ast
import importlib.util
import logging
import sys
from pathlib import Path

import pytest

from desflow import SimulationContext

ROOT = Path(__file__).resolve().parents[2]
TUTORIALS = ROOT / "docs" / "snippets" / "tutorials"
BLUEPRINTS = ROOT / "docs" / "snippets" / "yaml"
APIS = ["yaml", "builder", "low-level"]


def _load(name):
    sys.path.insert(0, str(TUTORIALS))
    try:
        spec = importlib.util.spec_from_file_location(name, TUTORIALS / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(TUTORIALS))


def _events(caplog):
    """Every event the console egress printed, as a dict, in order."""
    return [
        ast.literal_eval(r.getMessage()[6:])
        for r in caplog.records
        if r.getMessage().startswith("[EVT] ")
    ]


def _run_first_shop(api):
    if api == "yaml":
        app = SimulationContext.from_yaml(BLUEPRINTS / "first_shop.yaml")
        app.factor = 0.0
        app.run()
    elif api == "builder":
        module = _load("first_shop")
        module.app.factor = 0.0
        module.app.run(until=20)
    else:
        _load("first_shop_low_level").run(factor=0.0)


def test_first_simulation_same_records_and_first_wait(caplog):
    """The three tabs print the same events; orders-2 waits from 2.696 to 4.052."""
    runs = []
    for api in APIS:
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="desflow"):
            _run_first_shop(api)
        runs.append(
            [{k: v for k, v in e.items() if k != "timestamp"} for e in _events(caplog)]
        )
    assert runs[0] and runs[0] == runs[1] == runs[2]
    times = {}
    for event in runs[0]:
        status = event["value"].get("status", "finished")
        times.setdefault(event["key"], {})[status] = event["sim_ts"]
    waiting = [
        (key, t["started"] - t["queued"])
        for key, t in times.items()
        if "started" in t and t["started"] > t["queued"]
    ]
    key, wait = waiting[0]
    assert key == "orders-2"
    assert round(wait, 3) == 1.356


QUEUE_WAITS = {1: 1127.21, 2: 426.31, 3: 1.27, 4: 0.20}


def _queue_wait(api, packers, monkeypatch):
    if api == "yaml":
        monkeypatch.setenv("PACKERS", str(packers))
        app = SimulationContext.from_yaml(BLUEPRINTS / "queues_and_capacity.yaml")
        app.run()
        return app.env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]
    name = "queues_and_capacity" + ("" if api == "builder" else "_low_level")
    return _load(name).mean_wait(packers)


@pytest.mark.parametrize("api", APIS)
def test_queues_and_capacity_three_packers(api, monkeypatch):
    """The table on the page, and its answer: 3 packers keep the wait under 10 s."""
    waits = {n: _queue_wait(api, n, monkeypatch) for n in QUEUE_WAITS}
    assert {n: round(w, 2) for n, w in waits.items()} == QUEUE_WAITS
    assert min(n for n, w in waits.items() if w < 10) == 3


def _wait_of(events, key):
    times = {e["value"].get("status"): e["sim_ts"] for e in events if e["key"] == key}
    return round(times["started"] - times["queued"], 3)


def _run_yaml(path, caplog):
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="desflow"):
        SimulationContext.from_yaml(path).run()
    return _events(caplog)


@pytest.mark.parametrize("api", APIS)
def test_randomness_and_seeds_twelfth_order(api, caplog):
    """orders-12 waits 0.702 s with the seed 42, in every tab."""
    with caplog.at_level(logging.INFO, logger="desflow"):
        if api == "yaml":
            SimulationContext.from_yaml(BLUEPRINTS / "randomness_and_seeds.yaml").run()
        elif api == "builder":
            _load("randomness_and_seeds").app.run(until="1 min")
        else:
            _load("randomness_and_seeds_low_level").run()
    assert _wait_of(_events(caplog), "orders-12") == 0.702


def test_randomness_and_seeds_same_wait_on_every_run_and_after_returns(
    caplog, tmp_path
):
    """A second run and a run without returns agree."""
    import yaml

    source = BLUEPRINTS / "randomness_and_seeds.yaml"
    blueprint = yaml.safe_load(source.read_text())
    del blueprint["arrivals"]["returns"], blueprint["tasks"]["receive_return"]
    no_returns = tmp_path / "no_returns.yaml"
    no_returns.write_text(yaml.safe_dump(blueprint))

    for path in (source, source, no_returns):
        assert _wait_of(_run_yaml(path, caplog), "orders-12") == 0.702


CANDIDATE_WAITS = {
    "constant": 1.27,
    "triangular": 1.28,
    "empirical": 1.35,
    "exponential": 2.37,
}


def _candidate_wait(api, packing, monkeypatch):
    if api == "yaml":
        monkeypatch.setenv("PACKING", packing)
        app = SimulationContext.from_yaml(BLUEPRINTS / "choosing_inputs.yaml")
        app.run()
        return app.env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]
    name = "choosing_inputs" + ("" if api == "builder" else "_low_level")
    return _load(name).mean_wait(packing)


@pytest.mark.parametrize("api", APIS)
def test_choosing_inputs_spread_lengthens_waits(api, monkeypatch):
    """Same mean, ordered waits, and the empirical answer of 1.35 s."""
    waits = {p: _candidate_wait(api, p, monkeypatch) for p in CANDIDATE_WAITS}
    assert {p: round(w, 2) for p, w in waits.items()} == CANDIDATE_WAITS
    assert sorted(waits, key=waits.get) == list(CANDIDATE_WAITS)


def _measuring_readings(api, path=None):
    if api == "yaml":
        app = SimulationContext.from_yaml(path or BLUEPRINTS / "measuring_a_run.yaml")
        app.run()
        return app.env.stats.readings()
    if api == "builder":
        app = _load("measuring_a_run").app
        app.run(until="1 h")
        return app.env.stats.readings()
    return _load("measuring_a_run_low_level").run().stats.readings()


@pytest.mark.parametrize("api", APIS)
def test_measuring_a_run_answer(api):
    """The five numbers of the answer, and the Little's Law check on them."""
    r = _measuring_readings(api)
    name = "Shop.stats.{}".format
    assert round(r[name("resources.packers.utilization_avg")], 1) == 86.6
    assert round(r[name("resources.packers.queue_length_avg")], 2) == 2.41
    assert round(r[name("tasks.pack_order.wait_mean")], 2) == 1.38
    assert round(r[name("tasks.pack_order.wait_max")], 2) == 9.38
    assert r[name("tasks.pack_order.finished")] == 6250
    assert r[name("flows.orders.finished")] == 6250
    rate = r[name("tasks.pack_order.finished")] / 3600
    little = rate * r[name("tasks.pack_order.wait_mean")]
    assert little == pytest.approx(
        r[name("resources.packers.queue_length_avg")], rel=0.01
    )


def _trusting_wait(api, warmup, monkeypatch, path=None):
    if api == "yaml":
        monkeypatch.setenv("WARMUP", warmup)
        app = SimulationContext.from_yaml(path or BLUEPRINTS / "trusting_results.yaml")
        app.run()
        return app.env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]
    seconds = {"0": None, "2 min": 120.0}[warmup]
    if api == "builder":
        return _load("trusting_results").mean_wait(seconds)
    return _load("trusting_results_low_level").mean_wait(seconds)


@pytest.mark.parametrize("api", APIS)
def test_trusting_results_warmup(api, monkeypatch):
    """1.382 s without the 2-minute warm-up and 1.381 s with it."""
    assert round(_trusting_wait(api, "0", monkeypatch), 3) == 1.382
    assert round(_trusting_wait(api, "2 min", monkeypatch), 3) == 1.381


# (mean, half-width, half-width as a share of the mean in percent) after n runs
TRUSTING_PRECISION = {
    ("tasks.pack_order.wait_mean", 5): (2.21, 0.47, 21.2),
    ("tasks.pack_order.wait_mean", 10): (2.25, 0.25, 11.0),
    ("tasks.pack_order.wait_mean", 20): (2.48, 0.32, 13.0),
    ("tasks.pack_order.wait_mean", 40): (2.32, 0.20, 8.4),
    ("resources.packers.utilization_avg", 5): (89.72, 1.37, 1.5),
    ("resources.packers.utilization_avg", 10): (90.34, 0.91, 1.0),
    ("resources.packers.utilization_avg", 20): (90.33, 0.63, 0.7),
    ("resources.packers.utilization_avg", 40): (90.03, 0.45, 0.5),
}


@pytest.mark.parametrize("api", APIS)
def test_trusting_results_replications(api, monkeypatch):
    """The precision table, the range of the 40 waits, and the 10% target: the
    mean wait meets it at 40 replications and utilisation at 5."""
    from desflow import replicate

    builder = _load("trusting_results")
    if api == "yaml":
        monkeypatch.delenv("WARMUP", raising=False)
        result = replicate(
            BLUEPRINTS / "trusting_results.yaml",
            40,
            seed=42,
            until=None,
            measures=builder.MEASURES,
        )
    else:
        module = builder if api == "builder" else _load("trusting_results_low_level")
        result = module.replications()
    table = {
        key: (
            round(i.mean, 2),
            round(i.half_width, 2),
            round(i.half_width / i.mean * 100, 1),
        )
        for key, i in builder.precision(result).items()
    }
    assert table == TRUSTING_PRECISION
    waits = result.values["tasks.pack_order.wait_mean"]
    assert (round(min(waits), 2), round(max(waits), 2)) == (1.45, 4.39)
    assert min(waits) > 1.382


def _stock_wait(api, delivery, monkeypatch, path=None):
    if api == "yaml":
        monkeypatch.setenv("DELIVERY", str(delivery))
        app = SimulationContext.from_yaml(path or BLUEPRINTS / "stock.yaml")
        app.run()
        return app.env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]
    return _load("stock" + ("" if api == "builder" else "_low_level")).mean_wait(
        delivery
    )


@pytest.mark.parametrize("api", APIS)
def test_stock_deliveries(api, monkeypatch):
    """1.93 s with deliveries of 60 boxes, 341.89 s with deliveries of 40."""
    assert round(_stock_wait(api, 60, monkeypatch), 2) == 1.93
    assert round(_stock_wait(api, 40, monkeypatch), 2) == 341.89


PRIORITY_WAITS = {
    "fifo": {"pack_express": 5.84, "pack_order": 5.32},
    "priority": {"pack_express": 0.84, "pack_order": 6.94},
}


def _yaml_waits(path, tasks=("pack_express", "pack_order")):
    app = SimulationContext.from_yaml(path)
    app.run()
    r = app.env.stats.readings()
    return {t: round(r[f"Shop.stats.tasks.{t}.wait_mean"], 2) for t in tasks}


def _priority_yaml(tmp_path, name, fifo=False, orders=0.45, express=0.15):
    text = (BLUEPRINTS / "priorities.yaml").read_text()
    text = text.replace("rate: 0.45, spawn", f"rate: {orders}, spawn")
    text = text.replace("rate: 0.15, spawn", f"rate: {express}, spawn")
    if fifo:
        text = text.replace(", queue: priority}", "}")
        text = "\n".join(
            line
            for line in text.split("\n")
            if not line.strip().startswith("priority:")
        )
    path = tmp_path / f"{name}.yaml"
    path.write_text(text)
    return path


@pytest.mark.parametrize("api", APIS)
def test_priorities_four_waits(api, tmp_path):
    """Express jumps the queue under priority, and regular orders pay for it."""
    if api == "yaml":
        waits = {
            "fifo": _yaml_waits(_priority_yaml(tmp_path, "fifo", fifo=True)),
            "priority": _yaml_waits(BLUEPRINTS / "priorities.yaml"),
        }
    else:
        module = _load("priorities" + ("" if api == "builder" else "_low_level"))
        waits = {
            q: {t: round(w, 2) for t, w in module.mean_waits(q).items()}
            for q in PRIORITY_WAITS
        }
    assert waits == PRIORITY_WAITS


def test_priorities_starvation(tmp_path):
    """The regular waits as load rises."""
    for total, regular in ((0.5, 2.50), (0.55, 3.98)):
        path = _priority_yaml(
            tmp_path,
            f"load{total}",
            orders=round(total * 0.75, 4),
            express=round(total / 4, 4),
        )
        waits = _yaml_waits(path)
        assert waits["pack_order"] == regular and waits["pack_express"] < 1


FLOW_COUNTS = {
    "pick_order": 1857,
    "backorder": 80,
    "pack_order": 1849,
    "ship_order": 1775,
}


def _flow_counts(api, path=None):
    if api == "yaml":
        app = SimulationContext.from_yaml(path or BLUEPRINTS / "flow_of_tasks.yaml")
        app.run()
        readings = app.env.stats.readings()
    elif api == "builder":
        app = _load("flow_of_tasks").app
        app.run(until="1 h")
        readings = app.env.stats.readings()
    else:
        readings = _load("flow_of_tasks_low_level").run().stats.readings()
    return {
        name.split(".")[3]: value
        for name, value in readings.items()
        if name.startswith("Shop.stats.tasks.") and name.endswith(".finished")
    }


@pytest.mark.parametrize("api", APIS)
def test_flow_of_tasks_shares(api):
    """80 of 1,857 picks led to backorder, and 74 of 1,849 packs were packed again."""
    counts = _flow_counts(api)
    assert {t: counts[t] for t in FLOW_COUNTS} == FLOW_COUNTS
    assert round(counts["backorder"] / counts["pick_order"] * 100, 1) == 4.3
    reworked = counts["pack_order"] - counts["ship_order"]
    assert round(reworked / counts["pack_order"] * 100, 1) == 4.0


def test_flow_of_tasks_one_order(caplog):
    """orders-172 is backordered, picked again, packed twice and shipped."""
    events = _run_yaml(BLUEPRINTS / "flow_of_tasks.yaml", caplog)
    flow = [
        e["task"]
        for e in events
        if e["key"] == "orders-172" and "event_type" in e["value"]
    ]
    assert flow == [
        "pick_order",
        "backorder",
        "pick_order",
        "pack_order",
        "pack_order",
        "ship_order",
    ]


PATIENCE_OUTCOMES = {
    30: {"finished": 2173, "reneged": 69, "balked": 194, "wait_mean": 14.57},
    3600: {"finished": 2242, "reneged": 0, "balked": 194, "wait_mean": 72.16},
}


def _patience_outcomes(api, patience, monkeypatch):
    if api == "yaml":
        monkeypatch.setenv("PATIENCE", f"{patience} s")
        app = SimulationContext.from_yaml(BLUEPRINTS / "patience.yaml")
        app.run()
        readings = app.env.stats.readings()
        name = "Shop.stats.tasks.pack_order.{}".format
        result = {
            o: readings.get(name(o), 0)
            for o in ("finished", "reneged", "balked", "wait_mean")
        }
    else:
        module = _load("patience" + ("" if api == "builder" else "_low_level"))
        result = module.outcomes(patience)
    result["wait_mean"] = round(result["wait_mean"], 2)
    return result


@pytest.mark.parametrize("api", APIS)
def test_patience_outcomes(api, monkeypatch):
    """69 orders renege with a patience of 30 s, and the mean wait falls from 72.16 s
    to 14.57 s; 194 balk in the last five minutes either way."""
    for patience, expected in PATIENCE_OUTCOMES.items():
        assert _patience_outcomes(api, patience, monkeypatch) == expected

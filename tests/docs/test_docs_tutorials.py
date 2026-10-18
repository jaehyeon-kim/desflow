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


FILTERED_OUTCOMES = {
    ("S", "filled"): 732,
    ("M", "filled"): 1110,
    ("M", "cancelled"): 315,
    ("L", "filled"): 679,
}


@pytest.mark.parametrize("api", APIS)
def test_filtered_inventory_outcomes(api):
    """315 medium orders are cancelled in 8 hours, and no small or large one."""
    builder = _load("filtered_inventory")
    if api == "yaml":
        app = SimulationContext.from_yaml(BLUEPRINTS / "filtered_inventory.yaml")
        outcome = builder._Outcomes()
        app.add_egress(outcome)
        app.run()
        counts = outcome.counts
    elif api == "builder":
        counts = builder.outcomes()
    else:
        counts = _load("filtered_inventory_low_level").outcomes()
    assert dict(counts) == FILTERED_OUTCOMES


COURIER_OUTCOMES = {
    300: {"by_courier": 6952, "by_post": 7020, "wait_mean": 225.3},
    480: {"by_courier": 11005, "by_post": 2872, "wait_mean": 288.2},
}


def _courier_outcomes(api, limit, monkeypatch):
    if api == "yaml":
        monkeypatch.setenv("LIMIT", f"{limit} s")
        app = SimulationContext.from_yaml(BLUEPRINTS / "process_interaction.yaml")
        app.run()
        readings = app.env.stats.readings()
        name = "Shop.stats.tasks.{}".format
        result = {
            "by_courier": readings[name("by_courier.finished")],
            "by_post": readings[name("by_post.finished")],
            "wait_mean": readings[name("dispatch.wait_mean")],
        }
    else:
        name = "process_interaction" + ("" if api == "builder" else "_low_level")
        result = _load(name).outcomes(limit)
    result["wait_mean"] = round(result["wait_mean"], 1)
    return result


@pytest.mark.parametrize("api", APIS)
def test_process_interaction_courier_shares(api, monkeypatch):
    """Half the orders catch the courier with a 5-minute limit, 8 in 10 with 8."""
    for limit, expected in COURIER_OUTCOMES.items():
        assert _courier_outcomes(api, limit, monkeypatch) == expected


INTERRUPTION_OUTCOMES = {
    "resume": (344, 1607, 8.12, 1.50),
    "restart": (393, 1606, 25.03, 1.69),
    "abandon": (313, 1294, 3.60, 1.50),
}


@pytest.mark.parametrize("api", APIS)
def test_interruptions_policies(api, monkeypatch):
    """Restart triples the regular wait, abandon loses 313 orders."""
    for policy, expected in INTERRUPTION_OUTCOMES.items():
        if api == "yaml":
            monkeypatch.setenv("POLICY", policy)
            app = SimulationContext.from_yaml(BLUEPRINTS / "interruptions.yaml")
            app.run()
            r = app.env.stats.readings()
            name = "Shop.stats.tasks.pack_order.{}".format
            got = (
                r[name("preempted")],
                r[name("finished")],
                r[name("wait_mean")],
                r[name("service_mean")],
            )
        else:
            module = _load("interruptions" + ("" if api == "builder" else "_low_level"))
            o = module.outcomes(policy)
            got = (o["preempted"], o["finished"], o["wait_mean"], o["service_mean"])
        assert (got[0], got[1], round(got[2], 2), round(got[3], 2)) == expected


@pytest.mark.parametrize("api", APIS)
def test_competing_resources_handoff(api):
    """Keeping the picker moves the wait to the picker: 9.85 s in all, not 6.21."""
    if api == "yaml":
        app = SimulationContext.from_yaml(BLUEPRINTS / "competing_resources.yaml")
        app.run()
        r = app.env.stats.readings()
        kept = {
            "pick_wait": r["Shop.stats.tasks.pick_order.wait_mean"],
            "pack_wait": r["Shop.stats.tasks.pack_order.wait_mean"],
            "picker_busy": r["Shop.stats.resources.picker.utilization_avg"],
        }
        runs = {True: kept}
    else:
        module = _load(
            "competing_resources" + ("" if api == "builder" else "_low_level")
        )
        runs = {keep: module.outcomes(keep) for keep in (False, True)}
    expected = {False: (0.32, 5.89, 39.8, 6.21), True: (8.77, 1.09, 82.7, 9.85)}
    for keep, r in runs.items():
        got = (
            round(r["pick_wait"], 2),
            round(r["pack_wait"], 2),
            round(r["picker_busy"], 1),
            round(r["pick_wait"] + r["pack_wait"], 2),
        )
        assert got == expected[keep]


END_OUTCOMES = {
    False: {
        "end": 3600.0,
        "started": 2436,
        "finished": 2398,
        "in_system": 38,
        "wait_mean": 71.70,
    },
    True: {
        "end": 3656.4,
        "started": 2436,
        "finished": 2436,
        "in_system": 0,
        "wait_mean": 71.51,
    },
}


@pytest.mark.parametrize("api", APIS)
def test_end_conditions_horizon_and_drain(api, monkeypatch):
    """The fixed horizon leaves 38 orders in the shop; the drain packs them by
    3,656.4 s."""
    for drain, expected in END_OUTCOMES.items():
        if api == "yaml":
            monkeypatch.setenv("DRAIN", "true" if drain else "false")
            app = SimulationContext.from_yaml(BLUEPRINTS / "end_conditions.yaml")
            app.run()
            r = app.env.stats.readings()
            n = "Shop.stats.{}".format
            got = {
                "end": app.env.now,
                "started": r[n("flows.orders.started")],
                "finished": r[n("flows.orders.finished")],
                "in_system": r[n("flows.orders.in_system")],
                "wait_mean": r[n("tasks.pack_order.wait_mean")],
            }
            if not drain:
                # one being packed and 37 waiting
                assert r[n("tasks.pack_order.in_progress")] == 38
                assert r[n("resources.packers.in_use")] == 1
        else:
            module = _load(
                "end_conditions" + ("" if api == "builder" else "_low_level")
            )
            got = module.outcomes(drain)
        got["end"], got["wait_mean"] = round(got["end"], 1), round(got["wait_mean"], 2)
        assert got == expected


RUSH_OUTCOMES = {
    1: {"finished": 1978, "wait_mean": 147.74, "wait_max": 416.75},
    2: {"finished": 1978, "wait_mean": 0.74, "wait_max": 6.06},
}


@pytest.mark.parametrize("api", APIS)
def test_changing_demand_rush(api, monkeypatch):
    """With one packer through the rush the mean wait is 147.74 s; with a second
    packer for it, 0.74 s. Both pack the same 1,978 orders."""
    for packers, expected in RUSH_OUTCOMES.items():
        if api == "yaml":
            monkeypatch.setenv("RUSH_PACKERS", str(packers))
            app = SimulationContext.from_yaml(BLUEPRINTS / "changing_demand.yaml")
            app.run()
            r = app.env.stats.readings()
            n = "Shop.stats.tasks.pack_order.{}".format
            got = {
                "finished": r[n("finished")],
                "wait_mean": r[n("wait_mean")],
                "wait_max": r[n("wait_max")],
            }
        else:
            module = _load(
                "changing_demand" + ("" if api == "builder" else "_low_level")
            )
            got = module.outcomes(packers)
        got["wait_mean"] = round(got["wait_mean"], 2)
        got["wait_max"] = round(got["wait_max"], 2)
        assert got == expected


REPLAY_OUTCOMES = {
    True: {"finished": 1962, "wait_mean": 91.31, "wait_max": 295.40},
    False: {"finished": 1905, "wait_mean": 2.57, "wait_max": 14.34},
}


@pytest.mark.parametrize("api", APIS)
def test_local_replay_recorded_and_modelled(api, caplog):
    """The recorded morning waits 91.31 s on average, the model of it at the same
    mean rate 2.57 s; the row without a time is skipped with a warning."""
    for replay, expected in REPLAY_OUTCOMES.items():
        if api == "yaml":
            if not replay:
                continue  # the blueprint replays the recorded orders only
            app = SimulationContext.from_yaml(BLUEPRINTS / "local_replay.yaml")
            app.run()
            r = app.env.stats.readings()
            n = "Shop.stats.tasks.pack_order.{}".format
            got = {
                "finished": r[n("finished")],
                "wait_mean": r[n("wait_mean")],
                "wait_max": r[n("wait_max")],
            }
        else:
            module = _load("local_replay" + ("" if api == "builder" else "_low_level"))
            got = module.outcomes(replay)
        got["wait_mean"] = round(got["wait_mean"], 2)
        got["wait_max"] = round(got["wait_max"], 2)
        assert got == expected
    assert "Skipped row 501, which has no valid ordered_at: ''" in caplog.text

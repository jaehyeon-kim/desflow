"""The tutorial pages' code runs, in every tab, and gives the answer each page states.

Each page of the shop tutorial shows sections of three files: a blueprint under
docs/snippets/yaml/, a builder script and a low-level script under
docs/snippets/tutorials/. test_docs_match_examples.py keeps the copies equal to them.
This runs all three unpaced and checks the number the page gives as its answer, so a
change to the code that changes the answer fails here until the page is updated.
"""

import ast
import importlib.util
import logging
import os
import runpy
import statistics
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
    """Every event the console egress printed, as a dict, in order; it prints one
    line per record, a batch of lines per log call."""
    return [
        ast.literal_eval(line[6:])
        for r in caplog.records
        for line in r.getMessage().split("\n")
        if line.startswith("[EVT] ")
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
    """The three tabs print the same events; orders-3 waits from 5.024 to 6.434."""
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
    assert key == "orders-3"
    assert round(wait, 3) == 1.41


def test_first_simulation_blueprint_test_passes_and_a_wrong_one_fails(tmp_path):
    """The `tests` entry on the page passes with `desflow test` and prints the line the
    page shows; the same entry with another count fails and names both numbers."""
    from typer.testing import CliRunner

    from desflow.cli import app as cli

    source = BLUEPRINTS / "first_shop.yaml"
    run = CliRunner().invoke(cli, ["test", str(source)])
    assert run.exit_code == 0, run.output
    assert run.output.startswith("ok ")
    assert run.output.strip().endswith("first_shop.yaml: seven_orders_in_20_seconds")
    wrong = tmp_path / "first_shop.yaml"
    wrong.write_text(source.read_text().replace("finished: 7}", "finished: 6}"))
    run = CliRunner().invoke(cli, ["test", str(wrong)])
    assert run.exit_code == 1
    assert "expected 6, measured 7" in run.output


QUEUE_WAITS = {1: 1139.88, 2: 484.65, 3: 1.88, 4: 0.24}


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


def test_queues_and_capacity_explain_is_the_same_in_yaml_and_builder(monkeypatch):
    """The Builder tab prints what desflow explain prints for the blueprint: one
    packer at utilisation 2.7, and 4 packers the fewest below 0.85."""
    from typer.testing import CliRunner

    from desflow.cli import app

    monkeypatch.delenv("PACKERS", raising=False)
    path = BLUEPRINTS / "queues_and_capacity.yaml"
    printed = CliRunner().invoke(app, ["explain", str(path)]).stdout
    assert _load("queues_and_capacity").explain(1) == printed
    packers = SimulationContext.from_yaml(path).analyze().resources[0]
    assert (packers.utilization, packers.status) == (pytest.approx(2.7), "UNSTABLE")
    assert (
        packers.capacities[3].status == "OK"
        and packers.capacities[2].status == "CONGESTED"
    )


def _counts(readings):
    return {
        k: v
        for k, v in readings.items()
        if any(part in k for part in (".tasks.", ".flows.", ".resources."))
    }


def test_queues_and_capacity_network_is_the_same_in_three_apis():
    """Networks and Bottlenecks: the Builder tab's model explains as the blueprint
    does, with the checker the bottleneck, and runs as the Low-level tab's does."""
    from typer.testing import CliRunner

    from desflow.cli import app
    from desflow.explain.render import render_terminal

    path = BLUEPRINTS / "queues_network.yaml"
    printed = CliRunner().invoke(app, ["explain", str(path)]).stdout
    builder = _load("queues_and_capacity").network()
    explanation = builder.analyze()
    assert render_terminal(explanation) == printed
    assert explanation.bottleneck == "checkers"
    builder.with_stats("typical", interval=3600)
    builder.run(until=3600)
    low_level = _load("queues_and_capacity_low_level").run_network()
    assert _counts(builder.env.stats.readings()) == _counts(low_level.stats.readings())


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
    """orders-12 waits 0 s with the seed 42, in every tab."""
    with caplog.at_level(logging.INFO, logger="desflow"):
        if api == "yaml":
            SimulationContext.from_yaml(BLUEPRINTS / "randomness_and_seeds.yaml").run()
        elif api == "builder":
            _load("randomness_and_seeds").app.run(until="1 min")
        else:
            _load("randomness_and_seeds_low_level").run()
    assert _wait_of(_events(caplog), "orders-12") == 0.0


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
        assert _wait_of(_run_yaml(path, caplog), "orders-12") == 0.0


CANDIDATE_WAITS = {
    "constant": 1.88,
    "triangular": 1.95,
    "empirical": 1.81,
    "exponential": 3.46,
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
    """Same mean, the exponential wait far above the others, and the empirical answer of 1.81 s."""
    waits = {p: _candidate_wait(api, p, monkeypatch) for p in CANDIDATE_WAITS}
    assert {p: round(w, 2) for p, w in waits.items()} == CANDIDATE_WAITS
    assert max(waits, key=waits.get) == "exponential"
    assert waits["exponential"] > 1.5 * max(
        w for p, w in waits.items() if p != "exponential"
    )


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
    assert round(r[name("resources.packers.utilization_avg")], 1) == 90.0
    assert round(r[name("resources.packers.queue_length_avg")], 2) == 3.62
    assert round(r[name("tasks.pack_order.wait_mean")], 2) == 2.01
    assert round(r[name("tasks.pack_order.wait_max")], 2) == 11.94
    assert r[name("tasks.pack_order.finished")] == 6488
    assert r[name("flows.orders.finished")] == 6488
    assert r[name("flows.orders.started")] == 6491
    assert r[name("flows.orders.in_system")] == 3
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
    """2.007 s without the 2-minute warm-up and 2.05 s with it."""
    assert round(_trusting_wait(api, "0", monkeypatch), 3) == 2.007
    assert round(_trusting_wait(api, "2 min", monkeypatch), 3) == 2.05


# The mean wait of 5 runs after each candidate warm-up: (mean, half-width), seconds
TRUSTING_WARMUPS = {60: (2.01, 0.41), 120: (2.006, 0.43), 240: (2.005, 0.49)}


def _shortest_warmup(table):
    """The shortest warm-up after which no longer one moves the mean by more than
    that one's half-width."""
    warmups = sorted(table)
    for i, warmup in enumerate(warmups):
        mean = table[warmup][0]
        later = warmups[i + 1 :]
        if all(abs(table[w][0] - mean) <= table[w][1] for w in later):
            return warmup


@pytest.mark.parametrize("api", APIS)
def test_trusting_results_candidate_warmups(api, monkeypatch):
    """Five runs after warm-ups of 1, 2 and 4 minutes; by the rule, 1 minute is enough."""
    wait = "tasks.pack_order.wait_mean"
    if api == "yaml":
        # The page's command, with each warm-up through the blueprint's ${WARMUP}
        got = {}
        for seconds, text in ((60, "1 min"), (120, "2 min"), (240, "4 min")):
            monkeypatch.setenv("WARMUP", text)
            mean, half_width = _cli_waits("trusting_results.yaml", 5, wait)[wait]
            got[seconds] = (mean, round(half_width, 2))
    else:
        module = _load("trusting_results" + ("" if api == "builder" else "_low_level"))
        rows = [result.summary[0] for result in module.warmups().values()]
        got = {
            seconds: (round(row["mean"], 3), round(row["half_width"], 2))
            for seconds, row in zip(TRUSTING_WARMUPS, rows)
        }
    assert got == TRUSTING_WARMUPS
    assert _shortest_warmup(got) == 60


# (mean, half-width, half-width as a share of the mean in percent) after n runs
TRUSTING_PRECISION = {
    ("tasks.pack_order.wait_mean", 5): (2.01, 0.43, 21.4),
    ("tasks.pack_order.wait_mean", 10): (2.15, 0.3, 14.0),
    ("tasks.pack_order.wait_mean", 20): (2.15, 0.15, 7.1),
    ("tasks.pack_order.wait_mean", 40): (2.1, 0.1, 4.8),
    ("resources.packers.utilization_avg", 5): (89.6, 1.78, 2.0),
    ("resources.packers.utilization_avg", 10): (90.0, 1.08, 1.2),
    ("resources.packers.utilization_avg", 20): (89.99, 0.6, 0.7),
    ("resources.packers.utilization_avg", 40): (89.82, 0.39, 0.4),
}


@pytest.mark.parametrize("api", APIS)
def test_trusting_results_replications(api, monkeypatch):
    """The precision table, the range of the 40 waits, and the 10% target: the
    mean wait meets it at 20 replications and utilisation at 5."""
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
    assert (round(min(waits), 2), round(max(waits), 2)) == (1.52, 3.05)
    assert min(waits) < 2.007 < max(waits)


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
    """2.54 s with deliveries of 60 boxes, 330.02 s with deliveries of 40."""
    assert round(_stock_wait(api, 60, monkeypatch), 2) == 2.54
    assert round(_stock_wait(api, 40, monkeypatch), 2) == 330.02


PRIORITY_WAITS = {
    "fifo": {"pack_express": 5.44, "pack_order": 5.72},
    "priority": {"pack_express": 0.86, "pack_order": 7.17},
}


def _yaml_waits(path, tasks=("pack_express", "pack_order")):
    app = SimulationContext.from_yaml(path)
    app.run()
    r = app.env.stats.readings()
    return {t: round(r[f"Shop.stats.tasks.{t}.wait_mean"], 2) for t in tasks}


def _priority_yaml(tmp_path, name, fifo=False):
    text = (BLUEPRINTS / "priorities.yaml").read_text()
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


def test_priorities_starvation(monkeypatch):
    """The regular waits as load rises, with the rates the page's command sets."""
    for orders, express, regular in (
        ("0.375", "0.125", 2.70),
        ("0.4125", "0.1375", 4.12),
    ):
        monkeypatch.setenv("ORDERS_RATE", orders)
        monkeypatch.setenv("EXPRESS_RATE", express)
        waits = _yaml_waits(BLUEPRINTS / "priorities.yaml")
        assert waits["pack_order"] == regular and waits["pack_express"] < 1


FLOW_COUNTS = {
    "pick_order": 2011,
    "backorder": 198,
    "pack_order": 1904,
    "ship_order": 1812,
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
    """198 of 2,011 picks led to backorder, and 92 of 1,904 packs were packed again."""
    counts = _flow_counts(api)
    assert {t: counts[t] for t in FLOW_COUNTS} == FLOW_COUNTS
    assert round(counts["backorder"] / counts["pick_order"] * 100, 1) == 9.8
    reworked = counts["pack_order"] - counts["ship_order"]
    assert round(reworked / counts["pack_order"] * 100, 1) == 4.8


def test_flow_of_tasks_one_order(caplog):
    """orders-851 is backordered, picked again, packed twice and shipped."""
    events = _run_yaml(BLUEPRINTS / "flow_of_tasks.yaml", caplog)
    flow = [
        e["task"]
        for e in events
        if e["key"] == "orders-851" and "event_type" in e["value"]
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
    30: {"finished": 2200, "reneged": 144, "balked": 217, "wait_mean": 19.56},
    3600: {"finished": 2344, "reneged": 0, "balked": 217, "wait_mean": 120.75},
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
    """144 orders renege with a patience of 30 s, and the mean wait falls from 120.75 s
    to 19.56 s; 217 balk in the last five minutes either way."""
    for patience, expected in PATIENCE_OUTCOMES.items():
        assert _patience_outcomes(api, patience, monkeypatch) == expected


# Per size: started, filled, still waiting and cancelled orders, then the shirts left
FILTERED_OUTCOMES = {
    "S": (719, 711, 0, 8),
    "M": (1383, 1181, 35, 167),
    "L": (803, 779, 0, 24),
    "left": 197,
}


@pytest.mark.parametrize("api", APIS)
def test_filtered_inventory_outcomes(api):
    """199 orders are cancelled in 8 hours, 167 medium, 24 large and 8 small; the
    page's sum is 1,383 started - 1,181 filled - 35 waiting, and 197 shirts are left.
    Every tab reads them from the statistics."""
    if api == "yaml":
        app = SimulationContext.from_yaml(BLUEPRINTS / "filtered_inventory.yaml")
        app.run()
        readings = app.env.stats.readings()
        counts = {}
        for size in "SML":
            flow = f"Shop.stats.flows.orders_{size.lower()}"
            started = readings[f"{flow}.started"]
            filled = readings[f"{flow}.finished"]
            waiting = readings[f"{flow}.in_system"]
            counts[size] = (started, filled, waiting, started - filled - waiting)
        counts["left"] = readings["Shop.stats.stores.shelf.level"]
        assert readings["Shop.stats.tasks.take_shirt.reneged"] == 199
    elif api == "builder":
        counts = _load("filtered_inventory").outcomes()
    else:
        counts = _load("filtered_inventory_low_level").outcomes()
    assert counts == FILTERED_OUTCOMES


COURIER_OUTCOMES = {
    300: {"by_courier": 7131, "by_post": 7247, "wait_mean": 225.0},
    480: {"by_courier": 11376, "by_post": 2903, "wait_mean": 287.6},
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
    "resume": (349, 3, 1640, 8.17, 1.50),
    "restart": (389, 24, 1640, 44.11, 1.67),
    "abandon": (311, 2, 1328, 3.64, 1.50),
}


@pytest.mark.parametrize("api", APIS)
def test_interruptions_policies(api, monkeypatch):
    """Restart multiplies the regular wait by more than five, abandon loses 311
    orders, and the call at 31 minutes reaches orders under every policy."""
    for policy, expected in INTERRUPTION_OUTCOMES.items():
        if api == "yaml":
            monkeypatch.setenv("POLICY", policy)
            app = SimulationContext.from_yaml(BLUEPRINTS / "interruptions.yaml")
            app.run()
            r = app.env.stats.readings()
            name = "Shop.stats.tasks.pack_order.{}".format
            got = (
                r[name("preempted")],
                r.get(name("interrupted"), 0),
                r[name("finished")],
                r[name("wait_mean")],
                r[name("service_mean")],
            )
        else:
            module = _load("interruptions" + ("" if api == "builder" else "_low_level"))
            o = module.outcomes(policy)
            got = (
                o["preempted"],
                o["interrupted"],
                o["finished"],
                o["wait_mean"],
                o["service_mean"],
            )
        assert (*got[:3], round(got[3], 2), round(got[4], 2)) == expected


@pytest.mark.parametrize("api", APIS)
def test_competing_resources_handoff(api):
    """Keeping the picker moves the wait to the picker: 15.34 s in all, not 8.78."""
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
    expected = {False: (0.35, 8.43, 40.4, 8.78), True: (14.23, 1.11, 85.2, 15.34)}
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
        "started": 2561,
        "finished": 2386,
        "in_system": 175,
        "wait_mean": 122.81,
    },
    True: {
        "end": 3861.8,
        "started": 2561,
        "finished": 2561,
        "in_system": 0,
        "wait_mean": 131.27,
    },
}


@pytest.mark.parametrize("api", APIS)
def test_end_conditions_horizon_and_drain(api, monkeypatch):
    """The fixed horizon leaves 175 orders in the shop; the drain packs them by
    3,861.8 s."""
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
                # one being packed and 174 waiting
                assert r[n("tasks.pack_order.in_progress")] == 175
                assert r[n("resources.packers.in_use")] == 1
        else:
            module = _load(
                "end_conditions" + ("" if api == "builder" else "_low_level")
            )
            got = module.outcomes(drain)
        got["end"], got["wait_mean"] = round(got["end"], 1), round(got["wait_mean"], 2)
        assert got == expected


RUSH_OUTCOMES = {
    1: {"finished": 2054, "wait_mean": 177.81, "wait_max": 453.38},
    2: {"finished": 2054, "wait_mean": 0.93, "wait_max": 11.48},
}


@pytest.mark.parametrize("api", APIS)
def test_changing_demand_rush(api, monkeypatch):
    """With one packer through the rush the mean wait is 177.81 s; with a second
    packer for it, 0.93 s. Both pack the same 2,054 orders."""
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
    False: {"finished": 1984, "wait_mean": 3.79, "wait_max": 19.24},
}


@pytest.mark.parametrize("api", APIS)
def test_local_replay_recorded_and_modelled(api, caplog):
    """The recorded morning waits 91.31 s on average, the model of it at the same
    mean rate 3.79 s; the row without a time is skipped with a warning."""
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


def _experiment(name, api, tmp_path, monkeypatch):
    """Runs a tutorial's experiment in one API, writing its files under tmp_path."""
    from desflow import run_experiment

    if api == "yaml":
        monkeypatch.delenv("WARMUP", raising=False)
        return run_experiment(BLUEPRINTS / f"{name}_experiment.yaml", output=tmp_path)
    module = _load(name + ("" if api == "builder" else "_low_level"))
    return module.experiment(output=tmp_path)


def _means(result, statistic):
    return {
        r["design"]: (round(r["mean"], 2), round(r["half_width"], 2))
        for r in result.summary
        if r["statistic"] == statistic
    }


@pytest.mark.parametrize("api", APIS)
def test_comparing_designs_fourth_packer(api, tmp_path, monkeypatch):
    """A fourth packer cuts the mean wait by 1.89 +/- 0.14 s; the paired utilisation
    difference is narrower than either design's own interval; the check passes."""
    result = _experiment("comparing_designs", api, tmp_path, monkeypatch)
    assert _means(result, "tasks.pack_order.wait_mean") == {
        "three": (2.15, 0.15),
        "four": (0.27, 0.01),
    }
    assert _means(result, "resources.packers.utilization_avg") == {
        "three": (89.99, 0.6),
        "four": (67.51, 0.46),
    }
    difference = {
        r["measure"]: (round(r["difference"], 2), round(r["half_width"], 2))
        for r in result.comparisons
    }
    assert difference == {
        "tasks.pack_order.wait_mean": (-1.89, 0.14),
        "resources.packers.utilization_avg": (-22.49, 0.15),
    }
    assert {r["streams_both"] for r in result.comparisons} == {
        "arrival.orders;service.packing"
    }
    [check] = result.checks
    assert check["passed"] and round(check["value"], 3) == 0.276
    assert "The provenance of `Shop.arrival.orders` is not stated." in result.report


@pytest.mark.parametrize("api", APIS)
def test_parameter_sweeps_demand(api, tmp_path, monkeypatch):
    """The table of mean waits at four rates, for three packers and for four."""
    result = _experiment("parameter_sweeps", api, tmp_path, monkeypatch)
    rate = "Shop.arrival.orders.rate"
    assert _means(result, "tasks.pack_order.wait_mean") == {
        f"three {rate}=1.6": (0.9, 0.05),
        f"three {rate}=1.7": (1.32, 0.07),
        f"three {rate}=1.8": (2.15, 0.15),
        f"three {rate}=1.9": (4.52, 0.64),
        f"four {rate}=1.6": (0.16, 0.01),
        f"four {rate}=1.7": (0.21, 0.01),
        f"four {rate}=1.8": (0.27, 0.01),
        f"four {rate}=1.9": (0.34, 0.02),
    }


def _checking_fixed(api):
    if api == "yaml":
        app = SimulationContext.from_yaml(BLUEPRINTS / "checking_a_model.yaml")
        app.run()
        readings = app.env.stats.readings()
        return {k.removeprefix("Shop.stats."): v for k, v in readings.items()}
    return _load(
        "checking_a_model" + ("" if api == "builder" else "_low_level")
    ).fixed_run()


@pytest.mark.parametrize("api", APIS)
def test_checking_a_model_fixed_trace(api):
    """Ten seconds of fixed inputs give the counts and waits of the hand trace."""
    readings = _checking_fixed(api)
    assert readings["tasks.pack_order.finished"] == 4
    assert readings["tasks.pack_order.in_progress"] == 5
    assert readings["flows.orders.started"] == 9
    assert round(readings["tasks.pack_order.wait_mean"], 3) == 0.714
    assert readings["tasks.pack_order.wait_max"] == 2
    assert readings["resources.packers.utilization_avg"] == 80
    assert round(readings["resources.packers.queue_length_avg"], 3) == 0.8


@pytest.mark.parametrize(
    "name", ["checking_a_model_observed.csv", "local_replay_orders.csv"]
)
def test_data_file_copies(name):
    """The YAML blueprints and the scripts read the same data files."""
    assert (BLUEPRINTS / name).read_text() == (TUTORIALS / name).read_text()


@pytest.mark.parametrize("api", APIS)
def test_checking_a_model_experiment(api, tmp_path, monkeypatch):
    """The extremes against the hand estimates, Little's law for three packers
    and for one, the three checks passing, and the observed values."""
    result = _experiment("checking_a_model", api, tmp_path, monkeypatch)
    means = {
        (r["design"], r["statistic"]): (round(r["mean"], 2), round(r["half_width"], 2))
        for r in result.summary
    }
    wait, use, queue, done = (
        "tasks.pack_order.wait_mean",
        "resources.packers.utilization_avg",
        "resources.packers.queue_length_avg",
        "tasks.pack_order.finished",
    )
    assert means["three", wait] == (2.15, 0.15)
    assert means["three", use] == (89.99, 0.6)
    assert means["three", queue][0] == 3.88
    assert means["one", use] == (100.0, 0.0)
    assert round(means["one", done][0]) == 2318
    assert round(means["one", wait][0]) == 1170
    assert means["ten", use][0] == 27.0
    assert means["ten", wait][0] == 0.0
    assert means["four-busier", wait] == (0.34, 0.02)
    little = _load(
        "checking_a_model" + ("_low_level" if api == "low-level" else "")
    ).littles_law
    q, rate, w = little(result, "three")
    assert (round(q, 3), round(rate, 4), round(w, 3)) == (3.882, 1.8016, 2.151)
    assert round(rate * w, 3) == 3.875
    q, rate, w = little(result, "one")
    assert (round(q, 1), round(rate, 4), round(w, 1)) == (2101.7, 1.8016, 1169.7)
    assert round(rate * w, 1) == 2107.3
    assert [c["passed"] for c in result.checks] == [True, True, True]
    assert round(result.checks[2]["value"], 2) == 0.36
    observed = {
        r["measure"]: (r["value"], round(r["low"], 2), round(r["high"], 2), r["inside"])
        for r in result.observed
    }
    assert observed == {
        wait: (2.6, 2.0, 2.3, False),
        use: (91.0, 89.39, 90.6, False),
    }
    # One observed hour varies like one run, not like a mean of 20 runs
    runs = {
        m: [
            r["value"]
            for r in result.results
            if r["design"] == "three" and r["statistic"] == m
        ]
        for m in (wait, use)
    }
    spread = {m: statistics.stdev(v) for m, v in runs.items()}
    assert {m: round(s, 2) for m, s in spread.items()} == {wait: 0.32, use: 1.29}
    inside = {
        r["measure"]: sum(r["low"] <= v <= r["high"] for v in runs[r["measure"]])
        for r in result.observed
    }
    assert inside == {wait: 4, use: 5}
    current = {
        r["statistic"]: r["mean"] for r in result.summary if r["design"] == "three"
    }
    away = {
        r["measure"]: round(
            abs(r["value"] - current[r["measure"]]) / spread[r["measure"]], 2
        )
        for r in result.observed
    }
    assert away == {wait: 1.38, use: 0.78}
    assert round((95 - current[use]) / spread[use], 1) == 3.9


LIVE_OUTCOMES = {
    True: {"jam": 45.79, "after": 14.6},
    False: {"jam": 1.26, "after": 1.3},
    "last_longer": 1982.276,
    "labels": [
        (1200.0, "printer_jam", 1.5, 3.0),
        (1800.0, "printer_fixed", 3.0, 1.5),
    ],
}


def _live_yaml(tmp_path, monkeypatch, helpers):
    """Runs the blueprint, and again without its scenario block for the baseline."""
    import yaml

    blueprint = yaml.safe_load((BLUEPRINTS / "live_changes.yaml").read_text())
    del blueprint["scenario"]
    (tmp_path / "baseline").mkdir()
    baseline = tmp_path / "baseline" / "live_changes.yaml"
    baseline.write_text(yaml.safe_dump(blueprint))
    (tmp_path / "jam").mkdir()
    monkeypatch.chdir(tmp_path / "jam")
    SimulationContext.from_yaml(BLUEPRINTS / "live_changes.yaml").run()
    monkeypatch.chdir(tmp_path / "baseline")
    SimulationContext.from_yaml(baseline).run()
    by_run = {
        jam: helpers.waits(tmp_path / name)
        for jam, name in ((True, "jam"), (False, "baseline"))
    }
    result = {
        jam: {
            "jam": round(helpers.mean_wait(by_time, 1200, 1800), 2),
            "after": round(helpers.mean_wait(by_time, 1800, 2400), 2),
        }
        for jam, by_time in by_run.items()
    }
    result["last_longer"] = max(
        at for at, wait in by_run[True].items() if wait > by_run[False][at] + 0.001
    )
    labels = helpers.read_rows(tmp_path / "jam" / "labels")
    result["labels"] = [
        (row["sim_ts"], row["label"]["intervention"], row["old"], row["new"])
        for row in sorted(labels, key=lambda row: row["sim_ts"])
    ]
    return result


@pytest.mark.parametrize("api", APIS)
def test_live_changes_jam(api, tmp_path, monkeypatch):
    """Orders queued in the jam wait 45.79 s against 1.26; in the 10 minutes after the
    fix 14.60 s against 1.30; the last longer wait is queued at 1,982.3 s; two labels
    name the change at 1,200 and 1,800 s."""
    helpers = _load("live_changes")
    if api == "yaml":
        got = _live_yaml(tmp_path, monkeypatch, helpers)
    else:
        module = _load("live_changes" + ("" if api == "builder" else "_low_level"))
        got = module.outcomes(tmp_path)
    got["last_longer"] = round(got["last_longer"], 3)
    assert got == LIVE_OUTCOMES
    label = helpers.read_rows(tmp_path / "jam" / "labels")[0]
    assert list(label) == [
        "stream_type",
        "sim_ts",
        "timestamp",
        "path",
        "old",
        "new",
        "label",
    ]
    assert label["path"] == "Shop.service.packing.value"


@pytest.mark.parametrize("api", APIS)
def test_live_changes_local_ingress(api, tmp_path, monkeypatch):
    """LocalIngress sets packing to 3 s about 100 simulated seconds in, at a time that
    depends on the machine, and publishes no label, so labels/ is never created."""
    helpers = _load("live_changes")
    monkeypatch.chdir(tmp_path)
    if api == "yaml":
        app = SimulationContext.from_yaml(BLUEPRINTS / "live_changes_live.yaml")
        app.run()
        env, out = app.env, tmp_path / "live"
    elif api == "builder":
        env, out = helpers.live(tmp_path).env, tmp_path
    else:
        env, out = _load("live_changes_low_level").live(tmp_path), tmp_path
    assert env.registry.get_config("Shop.service.packing").value == 3.0
    rows = helpers.read_rows(out / "events")
    started = {r["instance"]: r["sim_ts"] for r in rows if r.get("status") == "started"}
    packed = {r["instance"]: r["sim_ts"] for r in rows if r.get("event_type")}
    slow = [started[i] for i in packed if abs(packed[i] - started[i] - 3.0) < 1e-6]
    assert slow and 0 < min(slow) < 300
    assert not (out / "labels").exists()


@pytest.mark.parametrize("name", ["live_changes", "live_changes_low_level"])
def test_live_changes_live_then_scenario(name, tmp_path, monkeypatch, capsys):
    """python <script> live prints packing at 3.0 s, and a later scenario run in the
    same module still starts from 1.5 s, so its answers do not change."""
    module = _load(name)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", [f"{name}.py", "live"])
    runpy.run_path(str(TUTORIALS / f"{name}.py"), run_name="__main__")
    captured = capsys.readouterr()
    assert captured.out == "packing after the live change: 3.0 s\n"
    assert "flushed on its timer" not in captured.err
    module.live(tmp_path / "live")
    got = module.outcomes(tmp_path)
    got["last_longer"] = round(got["last_longer"], 3)
    assert got == LIVE_OUTCOMES


def _streaming_run(api, tmp_path, monkeypatch):
    if api == "yaml":
        monkeypatch.chdir(tmp_path)
        SimulationContext.from_yaml(BLUEPRINTS / "streaming_out.yaml").run()
        files = _load("streaming_out").read_files(tmp_path / "packed")
        return sorted(len(rows) for rows in files.values()), files
    module = _load("streaming_out" + ("" if api == "builder" else "_low_level"))
    result = module.outcomes(tmp_path)
    files = module.read_files(tmp_path / "packed")
    assert result["row"]["sim_ts"] == 35.8280675455825
    return result["file_rows"], files


@pytest.mark.parametrize("api", APIS)
def test_streaming_out_records(api, tmp_path, monkeypatch, caplog):
    """The console gets 4,362 events (1,454 queued, 1,454 started, 1,454 packed) and 30
    telemetry records; the file sink gets the 1,454 packed orders in files of 500, 500
    and 454 rows; orders-12 is queued at 33.9733, started at 34.3281, packed at 35.8281."""
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="desflow"):
        file_rows, files = _streaming_run(api, tmp_path, monkeypatch)
    events = _events(caplog)
    statuses = [e["value"].get("status", e["value"].get("event_type")) for e in events]
    assert len(events) == 4362
    assert [statuses.count(s) for s in ("queued", "started", "order_packed")] == [
        1454,
        1454,
        1454,
    ]
    telemetry = [
        line
        for r in caplog.records
        for line in r.getMessage().split("\n")
        if line.startswith("[TEL] ")
    ]
    assert len(telemetry) == 30
    assert file_rows == [454, 500, 500]
    rows = [row for file_rows in files.values() for row in file_rows]
    assert {row["event_type"] for row in rows} == {"order_packed"}
    row = next(row for row in rows if row["key"] == "orders-12")
    assert list(row) == [
        "stream_type",
        "sim_ts",
        "timestamp",
        "key",
        "task",
        "instance",
        "event_type",
    ]
    assert (row["sim_ts"], row["task"], row["instance"]) == (
        35.8280675455825,
        "pack_order",
        11,
    )
    order = [
        (e["sim_ts"], e["task"], e["instance"])
        for e in events
        if e["key"] == "orders-12"
    ]
    assert order == [
        (33.9733203858314, "pack_order", 11),
        (34.3280675455825, "pack_order", 11),
        (35.8280675455825, "pack_order", 11),
    ]


FIT_WAITS = {
    "triangular": (2.58, 0.13),
    "gamma": (2.11, 0.1),
    "empirical": (2.1, 0.1),
}


def _fit_cli(*args):
    from typer.testing import CliRunner

    from desflow.cli import app as cli

    run = CliRunner().invoke(cli, ["fit", *map(str, args)])
    assert run.exit_code == 0, run.output
    return run.output


@pytest.mark.parametrize("api", APIS)
def test_fit_the_inputs(api, tmp_path, capsys, monkeypatch):
    """Triangular has the lowest AIC, gamma is 0.11 behind; the triangular raises
    the mean wait to 2.58 s while the gamma agrees with the 40 times. In YAML each
    fit is the file the page shows, and PACKING_FIT loads the gamma's."""
    pytest.importorskip("scipy")
    import yaml

    from desflow import replicate

    if api == "yaml":
        data = BLUEPRINTS / "packing_times.csv"
        blueprint = tmp_path / "fit_the_inputs.yaml"
        blueprint.write_text((BLUEPRINTS / "fit_the_inputs.yaml").read_text())
        monkeypatch.delenv("PACKING_FIT", raising=False)
        waits = {}
        for name, file, extra in (
            ("triangular", "packing.yaml", []),
            ("gamma", "packing_gamma.yaml", ["--candidates", "gamma"]),
        ):
            out = tmp_path / "fitted" / file
            printed = _fit_cli(
                data,
                "--column",
                "packing_seconds",
                "--lower",
                "0",
                "--out",
                out,
                *extra,
            )
            assert out.read_text() == (BLUEPRINTS / "fitted" / file).read_text()
            if name == "triangular":
                table = printed
            else:
                monkeypatch.setenv("PACKING_FIT", f"fitted/{file}")
            [row] = replicate(
                blueprint,
                40,
                seed=42,
                until=None,
                measures=["tasks.pack_order.wait_mean"],
            ).summary
            waits[name] = (round(row["mean"], 2), round(row["half_width"], 2))
        assert waits == {k: FIT_WAITS[k] for k in waits}
        # The 40 times as they are: the file the page shows holds the CSV's column
        import csv

        times = yaml.safe_load(
            (BLUEPRINTS / "fitted" / "packing_times.yaml").read_text()
        )
        with open(data) as handle:
            column = [float(r["packing_seconds"]) for r in csv.DictReader(handle)]
        assert times == {"dist": "empirical", "values": column}
        (tmp_path / "fitted" / "packing_times.yaml").write_text(
            (BLUEPRINTS / "fitted" / "packing_times.yaml").read_text()
        )
        monkeypatch.setenv("PACKING_FIT", "fitted/packing_times.yaml")
        [row] = replicate(
            blueprint, 40, seed=42, until=None, measures=["tasks.pack_order.wait_mean"]
        ).summary
        assert (round(row["mean"], 2), round(row["half_width"], 2)) == FIT_WAITS[
            "empirical"
        ]
    else:
        module = _load("fit_the_inputs" + ("" if api == "builder" else "_low_level"))
        result = module.fitted()
        table = capsys.readouterr().out
        assert round(result.settings("triangular")["mode"], 3) == 1.2
        waits = {
            name: (round(row["mean"], 2), round(row["half_width"], 2))
            for name, row in module.mean_waits(result).items()
        }
        assert waits == FIT_WAITS
    assert "triangular *      fitted            56.68" in table
    assert "gamma             fitted            56.79" in table
    assert "exponential       rejected" in table
    assert "(can draw below the lower bound 0)" in table


COMPLETE_FITTED = {
    "orders": ("gap_seconds", "complete_orders.yaml"),
    "picking": ("picking_seconds", "complete_picking.yaml"),
    "packing": ("packing_seconds", "complete_packing.yaml"),
}


@pytest.mark.parametrize("api", APIS)
def test_complete_model_fits(api, tmp_path):
    """Each column's best fit is the settings the blueprint includes: exponential
    gaps, gamma picking and packing times."""
    pytest.importorskip("scipy")
    import yaml

    fitted = BLUEPRINTS / "fitted"
    if api == "yaml":
        tables = {}
        for input_, (column, name) in COMPLETE_FITTED.items():
            out = tmp_path / name
            tables[input_] = _fit_cli(
                TUTORIALS / "complete_model_observed.csv",
                "--column",
                column,
                "--lower",
                "0",
                "--out",
                out,
            )
            assert out.read_text() == (fitted / name).read_text()
        # The KS p-values the page quotes for the two fits it does not show
        orders, picking = tables["orders"], tables["picking"]
        assert (
            "exponential *     fitted            355.9  0.02357          0.995"
            in orders
        )
        assert (
            "gamma *           fitted           -101.7  0.04252         0.6341"
            in picking
        )
    else:
        module = _load("complete_model" + ("" if api == "builder" else "_low_level"))
        assert module.FITS == {
            input_: yaml.safe_load((fitted / name).read_text())
            for input_, (_, name) in COMPLETE_FITTED.items()
        }
    assert yaml.safe_load((fitted / "complete_orders.yaml").read_text()) == {
        "dist": "exponential",
        "rate": 1.50719,
    }
    # The sensitivity rate: the fitted rate's standard error is rate / sqrt(n) for
    # n gaps, about 6% of it, a 95% interval reaches about 11% either side, and the
    # busier variant's rate is 10% higher
    n = len((TUTORIALS / "complete_model_observed.csv").read_text().splitlines()) - 1
    assert (n, round(100 / n**0.5), round(196 / n**0.5)) == (300, 6, 11)
    assert round(1.50719 * 1.1, 5) == 1.65791


class _Records:
    """A sink that keeps every record it receives, without its timestamp."""

    def __init__(self):
        from desflow.connectors.egress.base import BaseEgress

        records = self.records = []

        class Sink(BaseEgress):
            async def run(self, egress_queue):
                import asyncio
                import queue

                while True:
                    try:
                        batch = egress_queue.get_nowait()
                    except queue.Empty:
                        await asyncio.sleep(0.01)
                        continue
                    records.extend(
                        {k: v for k, v in r.items() if k != "timestamp"}
                        for r in batch
                        if not str(r.get("path_id", "")).startswith("system.")
                    )

        self.sink = Sink()


def _complete_records(api, monkeypatch):
    kept = _Records()
    if api == "low-level":
        _load("complete_model_low_level").run([kept.sink])
        return kept.records
    if api == "yaml":
        monkeypatch.delenv("WARMUP", raising=False)
        app = SimulationContext.from_yaml(BLUEPRINTS / "complete_model.yaml")
    else:
        app = _load("complete_model").shop()
    app.add_egress(kept.sink)
    app.run(until="1 h")
    return kept.records


def test_complete_model_same_records_in_three_apis(monkeypatch):
    """The busy hour publishes the same events and telemetry in all three tabs."""
    pytest.importorskip("scipy")
    records = {api: _complete_records(api, monkeypatch) for api in APIS}
    assert len(records["yaml"]) > 40000
    assert records["builder"] == records["yaml"]
    assert records["low-level"] == records["yaml"]


def test_complete_model_explain_is_the_same_in_yaml_and_builder(monkeypatch):
    """desflow explain --verify prints the same for the blueprint and the builder
    file, and names the packers as the bottleneck."""
    pytest.importorskip("scipy")
    from typer.testing import CliRunner

    from desflow.cli import app

    monkeypatch.delenv("WARMUP", raising=False)
    verify = ["--verify", "--replications", "10"]
    yaml_run = CliRunner().invoke(
        app, ["explain", str(BLUEPRINTS / "complete_model.yaml"), *verify]
    )
    builder_run = CliRunner().invoke(
        app,
        ["explain", str(TUTORIALS / "complete_model.py"), *verify, "--until", "1 h"],
    )
    assert yaml_run.exit_code == builder_run.exit_code == 0
    assert builder_run.stdout == yaml_run.stdout
    assert "Bottleneck: packers" in yaml_run.stdout


COMPLETE_WARMUPS = {
    0: {"pick": (0.553, 0.015), "pack": (1.718, 0.103)},
    120: {"pick": (0.553, 0.016), "pack": (1.733, 0.104)},
    600: {"pick": (0.55, 0.016), "pack": (1.699, 0.104)},
}


@pytest.mark.parametrize("api", APIS)
def test_complete_model_warmups(api, monkeypatch):
    """The mean waits after warm-ups of 0, 2 and 10 minutes differ by less than
    their half-widths."""
    pytest.importorskip("scipy")
    if api == "yaml":
        # The page's command, with each warm-up through the blueprint's ${WARMUP}
        got = {}
        for w, text in ((0, "0"), (120, "2 min"), (600, "10 min")):
            monkeypatch.setenv("WARMUP", text)
            waits = _cli_waits(
                "complete_model.yaml",
                40,
                "tasks.pick_order.wait_mean",
                "tasks.pack_order.wait_mean",
            )
            got[w] = {name.split(".")[1][:4]: value for name, value in waits.items()}
    else:
        module = _load("complete_model" + ("" if api == "builder" else "_low_level"))
        results = dict(zip(COMPLETE_WARMUPS, module.warmups().values()))
        got = {
            w: {
                r["measure"].split(".")[1][:4]: (
                    round(r["mean"], 3),
                    round(r["half_width"], 3),
                )
                for r in result.summary
            }
            for w, result in results.items()
        }
    assert got == COMPLETE_WARMUPS


def _cli_waits(blueprint, replications, *measures):
    """The mean waits `desflow run <blueprint> --replications <n> --measure ...`
    prints, as (mean, half-width) by measure."""
    import re

    from typer.testing import CliRunner

    from desflow.cli import app

    arguments = ["run", blueprint, "--replications", str(replications)]
    for measure in measures:
        arguments += ["--measure", measure]
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(BLUEPRINTS)
        run = CliRunner().invoke(app, arguments)
    assert run.exit_code == 0, run.output
    rows = re.findall(
        rf"^(tasks\.\w+\.wait_mean) +{replications} +(\S+) +(\S+)$", run.stdout, re.M
    )
    return {name: (round(float(m), 3), round(float(h), 3)) for name, m, h in rows}


def test_complete_model_outputs_copies():
    """The YAML experiment and the scripts read the same observed outputs."""
    name = "complete_model_outputs.csv"
    assert (BLUEPRINTS / name).read_text() == (TUTORIALS / name).read_text()


@pytest.mark.parametrize("api", APIS)
def test_complete_model_recommends_a_design(api, tmp_path, monkeypatch):
    """Only the fourth packer keeps both mean waits under 1 second: it cuts the
    packing wait by 1.58 +/- 0.10 s, while a second picker lengthens it; only the
    observed utilisation lies inside the model's interval, and every observed value
    lies within one standard deviation of one run; with orders 10% faster the
    fourth packer still keeps both waits under 1 second; every check passes."""
    pytest.importorskip("scipy")
    result = _experiment("complete_model", api, tmp_path, monkeypatch)
    pick, pack, use, ship = (
        "tasks.pick_order.wait_mean",
        "tasks.pack_order.wait_mean",
        "resources.packers.utilization_avg",
        "tasks.ship_order.wait_mean",
    )
    means = {
        (r["design"], r["statistic"]): (round(r["mean"], 2), round(r["half_width"], 2))
        for r in result.summary
        if r["statistic"] in (pick, pack, use, ship)
    }
    assert means == {
        ("current", pick): (0.55, 0.02),
        ("current", pack): (1.73, 0.1),
        ("current", use): (87.76, 0.39),
        ("current", ship): (0.0, 0.0),
        ("second-picker", pick): (0.04, 0.0),
        ("second-picker", pack): (1.98, 0.11),
        ("second-picker", use): (87.76, 0.39),
        ("second-picker", ship): (0.0, 0.0),
        ("fourth-packer", pick): (0.55, 0.02),
        ("fourth-packer", pack): (0.15, 0.01),
        ("fourth-packer", use): (65.83, 0.29),
        ("fourth-packer", ship): (0.0, 0.0),
        # The sensitivity variant: the fourth packer with orders 10% faster
        ("fourth-packer-busier", pick): (0.76, 0.02),
        ("fourth-packer-busier", pack): (0.23, 0.01),
        ("fourth-packer-busier", use): (72.44, 0.31),
        ("fourth-packer-busier", ship): (0.14, 0.17),
    }
    # With orders faster, some runs have orders waiting for shipping boxes
    busier_ship = [
        r["value"]
        for r in result.results
        if r["design"] == "fourth-packer-busier" and r["statistic"] == ship
    ]
    assert (len(busier_ship), sum(v > 0 for v in busier_ship)) == (40, 22)
    assert round(max(busier_ship), 2) == 3.41
    difference = {
        (r["variant"], r["measure"]): (
            round(r["difference"], 2),
            round(r["half_width"], 2),
        )
        for r in result.comparisons
        if r["measure"] in (pick, pack)
    }
    assert difference == {
        ("second-picker", pick): (-0.52, 0.01),
        ("second-picker", pack): (0.25, 0.01),
        ("fourth-packer", pick): (0.0, 0.0),
        ("fourth-packer", pack): (-1.58, 0.1),
    }
    assert [c["passed"] for c in result.checks] == [True] * 7
    assert round(result.checks[0]["value"], 2) == 0.57
    assert [round(c["value"], 2) for c in result.checks[5:]] == [0.78, 0.24]
    observed = {
        r["measure"]: (r["value"], round(r["low"], 2), round(r["high"], 2), r["inside"])
        for r in result.observed
    }
    assert observed == {
        pick: (0.57, 0.54, 0.57, False),
        pack: (1.9, 1.63, 1.84, False),
        use: (88.0, 87.37, 88.15, True),
    }
    # One observed hour varies like one run, not like a mean of 40 runs
    runs = {
        m: [
            r["value"]
            for r in result.results
            if r["design"] == "current" and r["statistic"] == m
        ]
        for m in (pick, pack, use)
    }
    spread = {m: round(statistics.stdev(v), 3) for m, v in runs.items()}
    assert spread == {pick: 0.049, pack: 0.325, use: 1.214}
    inside = {
        r["measure"]: sum(r["low"] <= v <= r["high"] for v in runs[r["measure"]])
        for r in result.observed
    }
    assert inside == {pick: 12, pack: 8, use: 7}
    # How many single-run standard deviations each observed value lies from the mean
    current = {
        r["statistic"]: r["mean"] for r in result.summary if r["design"] == "current"
    }
    away = {
        r["measure"]: round(
            abs(r["value"] - current[r["measure"]]) / spread[r["measure"]], 2
        )
        for r in result.observed
    }
    assert away == {pick: 0.34, pack: 0.51, use: 0.2}
    assert _load("complete_model").recommended(result) == ["fourth-packer"]
    assert "## Missing Inputs\n\nNone." in result.report


def _script_output(name, argv, tmp_path, monkeypatch, capsys):
    """What `python <name>.py <argv>` prints to the terminal, from an empty folder."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", [f"{name}.py", *argv])
    monkeypatch.syspath_prepend(str(TUTORIALS))
    runpy.run_path(str(TUTORIALS / f"{name}.py"), run_name="__main__")
    return capsys.readouterr().out


@pytest.mark.parametrize("name", ["flow_of_tasks", "flow_of_tasks_low_level"])
def test_flow_of_tasks_scripts_print_the_counts(name, tmp_path, monkeypatch, capsys):
    """The page's command prints the four finished counts of the table."""
    out = _script_output(name, [], tmp_path, monkeypatch, capsys)
    assert out == (
        "pick_order: 2011 finished\n"
        "backorder : 198 finished\n"
        "pack_order: 1904 finished\n"
        "ship_order: 1812 finished\n"
    )


@pytest.mark.parametrize("name", ["priorities", "priorities_low_level"])
def test_priorities_scripts_take_the_rates(name, tmp_path, monkeypatch, capsys):
    """The page's commands give the starvation waits at 30 and 33 orders a minute,
    and the default rates give 36 a minute (7.17 s)."""
    for argv, regular in (
        (["0.375", "0.125"], "2.70"),
        (["0.4125", "0.1375"], "4.12"),
        ([], "7.17"),
    ):
        out = _script_output(name, argv, tmp_path, monkeypatch, capsys)
        assert f"priority pack_order   mean wait  {regular} s" in out.split("\n")


def test_live_changes_compare_reads_two_yaml_runs(tmp_path, monkeypatch, capsys):
    """The page's YAML commands (a baseline made with sed, one run in each folder,
    then `compare`) print what the Builder and Low-level scripts print."""
    import subprocess

    text = (BLUEPRINTS / "live_changes.yaml").read_text()
    text = "".join(line for line in text.splitlines(True) if "--8<--" not in line)
    (tmp_path / "live_changes.yaml").write_text(text)
    baseline = subprocess.run(
        ["sed", "/^scenario:/,/^$/d", "live_changes.yaml"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "\nscenario:" not in baseline and "egress:" in baseline
    (tmp_path / "live_changes_baseline.yaml").write_text(baseline)
    for folder, blueprint in (
        ("jam", "live_changes.yaml"),
        ("baseline", "live_changes_baseline.yaml"),
    ):
        (tmp_path / folder).mkdir()
        monkeypatch.chdir(tmp_path / folder)
        SimulationContext.from_yaml(tmp_path / blueprint).run()
    monkeypatch.chdir(tmp_path)
    compared = _script_output(
        "live_changes", ["compare", "jam", "baseline"], tmp_path, monkeypatch, capsys
    )
    assert compared == (
        "label at 1200.0 s: printer_jam, packing 1.5 s to 3.0 s\n"
        "label at 1800.0 s: printer_fixed, packing 3.0 s to 1.5 s\n"
        "with the jam: mean wait  45.79 s from 20 to 30 min,  14.60 s from 30 to 40 min\n"
        "  without it: mean wait   1.26 s from 20 to 30 min,   1.30 s from 30 to 40 min\n"
        "the last order that waits longer is queued at 1982.3 s\n"
    )
    (tmp_path / "scripts").mkdir()
    for name in ("live_changes", "live_changes_low_level"):
        out = _script_output(name, [], tmp_path / "scripts", monkeypatch, capsys)
        assert out == compared


# What the three tabs print for the agreement with the observed values
AGREEMENT_LINES = [
    "tasks.pack_order.wait_mean: mean 2.15, sd 0.32, distance 1.38 sd, 4 of 20 runs inside",
    "resources.packers.utilization_avg: mean 89.99, sd 1.29, distance 0.78 sd, 5 of 20 runs inside",
]


def _page_command(page, marker):
    """The bash block of a docs page whose text holds `marker`, as the page writes it."""
    import re

    text = (ROOT / "docs" / page).read_text(encoding="utf-8")
    blocks = re.findall(r"^ *```bash\n(.*?)\n *```$", text, re.M | re.S)
    (block,) = [b for b in blocks if marker in b]
    return "\n".join(line.removeprefix("    ") for line in block.split("\n"))


def test_checking_a_model_yaml_command_prints_the_agreement(tmp_path, monkeypatch):
    """The YAML tab's awk command prints the standard deviations, distances and
    counts that the page's answer rests on, as the scripts print them."""
    import shutil
    import subprocess

    command = _page_command(
        "learn/studies/checking-a-model.md", "checking_a_model_observed.csv"
    )
    shutil.copy(BLUEPRINTS / "checking_a_model_observed.csv", tmp_path)
    _experiment(
        "checking_a_model", "yaml", tmp_path / "results/checking-a-model", monkeypatch
    )
    printed = subprocess.run(
        ["sh", "-c", command], cwd=tmp_path, capture_output=True, text=True, check=True
    ).stdout
    assert printed.split("\n") == AGREEMENT_LINES + [""]


@pytest.mark.parametrize("name", ["checking_a_model", "checking_a_model_low_level"])
def test_checking_a_model_scripts_print_the_extremes(
    name, tmp_path, monkeypatch, capsys
):
    """The page's command prints the one-packer and ten-packer means and Little's law."""
    out = _script_output(name, [], tmp_path, monkeypatch, capsys).split("\n")
    assert out[1:5] == [
        "one: 2318 finished, utilisation 100.00%, mean wait 1169.71 s",
        "ten: 6270 finished, utilisation 27.00%, mean wait 0.00 s",
        "three: queue 3.882, 1.8016 x 2.151 = 3.875",
        "one: queue 2101.667, 1.8016 x 1169.707 = 2107.321",
    ]
    assert out[7:] == AGREEMENT_LINES + [""]


def test_trusting_results_yaml_command_prints_the_two_final_counts(
    tmp_path, monkeypatch
):
    """The page's command, with its `tail -2`, prints the last queue average and the
    packed count of the hour (6,294 orders), not the 698 lines of every interval."""
    import shutil
    import subprocess

    command = _page_command(
        "learn/studies/trusting-results.md", 'pack_order\\.finished"|queue_length_avg'
    )
    assert command.endswith("| tail -2")
    shutil.copy(BLUEPRINTS / "trusting_results.yaml", tmp_path)
    printed = subprocess.run(
        ["sh", "-c", command],
        cwd=tmp_path,
        env={
            **os.environ,
            "PATH": f"{Path(sys.executable).parent}{os.pathsep}{os.environ['PATH']}",
        },
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    assert len(printed) == 2
    assert printed[0].endswith('queue_length_avg","value":3.7095253628163287}')
    assert printed[1].endswith('pack_order.finished","value":6294}')


def test_checking_a_model_summary_columns(tmp_path, monkeypatch):
    """The YAML tab's awk commands read these columns of summary.csv."""
    import csv

    _experiment("checking_a_model", "yaml", tmp_path, monkeypatch)
    with (tmp_path / "summary.csv").open() as handle:
        rows = list(csv.reader(handle))
    header = rows[0]
    assert (header[0], header[2], header[6]) == ("design", "statistic", "mean")
    means = {(r[0], r[2]): round(float(r[6]), 3) for r in rows[1:]}
    assert means["three", "flows.orders.started"] == 6269.5
    assert means["three", "resources.packers.queue_length_avg"] == 3.882
    assert means["one", "tasks.pack_order.wait_mean"] == 1169.707


@pytest.mark.parametrize("name", ["trusting_results", "trusting_results_low_level"])
def test_trusting_results_scripts_print_the_start_and_the_counts(
    name, tmp_path, monkeypatch, capsys, caplog
):
    """After the 2-minute warm-up 6,294 orders are packed and queue_length_avg is
    3.71; with `start` the script prints the first statistics, at 10 s, 20 s and so on."""
    module = _load(name)
    readings = (
        module.busy_hour("2 min")
        if name == "trusting_results"
        else module.run(120.0).stats.readings()
    )
    assert readings[module.PACKED] == 6294
    assert round(readings[module.QUEUE], 2) == 3.71
    with caplog.at_level(logging.INFO):
        _script_output(name, ["start"], tmp_path, monkeypatch, capsys)
    lines = [
        ast.literal_eval(line[6:])
        for r in caplog.records
        for line in r.getMessage().split("\n")
        if line.startswith("[TEL] ")
    ]
    wait = [
        (t["sim_ts"], round(t["value"], 4))
        for t in lines
        if t["path_id"].endswith("pack_order.wait_mean")
    ]
    assert wait[:2] == [(10.0, 0.0585), (20.0, 0.0328)]
    # The page's table of values at chosen times, and its 90 readings of each path
    queue = {
        t["sim_ts"]: t["value"]
        for t in lines
        if t["path_id"].endswith("packers.queue_length")
    }
    assert len(wait) == len(queue) == 90
    at = {seconds: (queue[seconds], round(value, 2)) for seconds, value in wait}
    assert [at[s] for s in (10, 30, 60, 90, 120, 300, 600, 900)] == [
        (0, 0.06),
        (0, 0.21),
        (2, 0.65),
        (1, 0.6),
        (0, 0.59),
        (0, 1.31),
        (10, 2.6),
        (0, 2.35),
    ]
    assert wait[-1][0] == 900.0

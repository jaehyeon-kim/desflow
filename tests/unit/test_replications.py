"""Replications: seeds, intervals, the three model forms, workers and the CLI."""

import asyncio
import queue
from pathlib import Path

import pytest
from typer.testing import CliRunner

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicResource,
    LowLevelModel,
    SimParameter,
    SimulationContext,
    replicate,
)
from desflow.blueprint.cli import app as cli
from desflow.connectors.egress.base import BaseEgress
from desflow.connectors.ingress.base import BaseIngress
from desflow.analysis.intervals import interval, t_quantile
from desflow.analysis.runner import level_for, spawn_seeds

BLUEPRINT = """\
simulation: {sim_id: Shop, factor: 0}
resources: {packers: {current_cap: 1, max_cap: 1}}
arrivals: {orders: {dist: exponential, rate: 0.5, spawn: pack}}
services: {packing: {dist: exponential, rate: 1.0}}
tasks: {pack: {service: packing, resource: packers, payload: {event_type: packed}}}
stats: typical
run: {until: 2000}
"""
MEASURES = ["tasks.pack.wait_mean", "resources.packers.utilization_avg"]


def shop(patience=None) -> SimulationContext:
    """The blueprint's M/M/1 shop, written with the builder."""
    app = (
        SimulationContext(sim_id="Shop", factor=0)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_arrival("orders", dist="exponential", rate=0.5)
        .add_service("packing", dist="exponential", rate=1.0)
        .with_stats("typical")
    )

    @app.task(service_id="packing", resource_id="packers", patience=patience)
    def pack(task_id):
        return {"event_type": "packed"}

    @app.arrival_loop("orders")
    def orders(context):
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack(task_id, entity=entity))
            task_id += 1

    return app


def _pack(env, sampler, packers, task_id):
    config = env.registry.get_config("Shop.service.packing")
    env.stats.task_queued("pack", task_id)
    with packers.request() as request:
        yield request
        env.stats.task_started("pack", task_id)
        yield env.timeout(sampler.sample(config))
    env.stats.task_finished("pack", task_id)
    env.stats.flow_finished("orders")


def _orders(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(_pack(env, sampler, packers, task_id))
        task_id += 1


def low_level(env, sampler):
    """The same shop, written with the low-level API."""
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
            service={"packing": DistributionConfig(dist="exponential", rate=1.0)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
        )
    )
    env.process(_orders(env, sampler, DynamicResource(env, "Shop", "packers")))


@pytest.fixture
def blueprint(tmp_path) -> Path:
    path = tmp_path / "shop.yaml"
    path.write_text(BLUEPRINT)
    return path


# Two-sided 95% (p = 0.975), one-sided 95% and two-sided 99% values from a
# published table of the t distribution
T_TABLE = {
    (0.975, 1): 12.706,
    (0.975, 2): 4.303,
    (0.975, 4): 2.776,
    (0.975, 9): 2.262,
    (0.975, 19): 2.093,
    (0.975, 29): 2.045,
    (0.975, 120): 1.980,
    (0.95, 5): 2.015,
    (0.95, 30): 1.697,
    (0.995, 10): 3.169,
    (0.995, 60): 2.660,
}


@pytest.mark.parametrize("p,df", list(T_TABLE))
def test_t_quantile_matches_the_table(p, df):
    assert round(t_quantile(p, df), 3) == T_TABLE[(p, df)]
    assert t_quantile(1 - p, df) == -t_quantile(p, df)


def test_interval_matches_a_hand_calculation():
    """1 to 5: mean 3, s = sqrt(2.5), half-width 2.776 x s / sqrt(5) = 1.963."""
    result = interval([1, 2, 3, 4, 5], 0.95)
    assert (result.n, result.mean) == (5, 3.0)
    assert result.std == pytest.approx(2.5**0.5)
    assert result.half_width == pytest.approx(2.776445 * 2.5**0.5 / 5**0.5)
    assert result.low == pytest.approx(3 - result.half_width)


def test_interval_refuses_one_value_and_a_bad_level():
    with pytest.raises(ValueError, match="at least 2"):
        interval([1.0])
    with pytest.raises(ValueError, match="level"):
        interval([1.0, 2.0], 1.0)


def test_seeds_are_plain_integers_and_repeat():
    seeds = spawn_seeds(42, 5)
    assert seeds == spawn_seeds(42, 5) and len(set(seeds)) == 5
    assert all(type(s) is int for s in seeds)
    assert spawn_seeds(42, 6)[:5] == seeds


def test_level_follows_the_measures():
    assert level_for(["tasks.pack.finished"]) == "basic"
    assert level_for(["tasks.pack.wait_mean", "tasks.pack.finished"]) == "typical"
    assert level_for(["tasks.pack.service_p95"]) == "all"
    assert level_for(None) == "typical"


def test_three_forms_repeat_and_agree(blueprint):
    """Each form repeats exactly with one seed, and the three are the same model."""
    models = [blueprint, shop, LowLevelModel(low_level, "Shop")]
    results = []
    for model in models:
        first = replicate(model, 3, seed=11, until=2000, measures=MEASURES)
        again = replicate(model, 3, seed=11, until=2000, measures=MEASURES)
        assert first.values == again.values
        results.append(first.values)
    assert results[0] == results[1] == results[2]
    assert (
        replicate(shop, 3, seed=12, until=2000, measures=MEASURES).values
        != (results[1])
    )


def test_mm1_mean_wait_lies_in_its_interval():
    """30 replications of M/M/1 with rate 0.5 and service rate 1: Wq = 1 s."""
    result = replicate(
        shop,
        30,
        seed=7,
        until=5000,
        warmup=500,
        measures=["tasks.pack.wait_mean"],
    )
    [row] = result.summary
    assert row["n"] == 30
    assert abs(row["mean"] - 1.0) <= row["half_width"]


def test_a_replication_reruns_with_its_seed(blueprint):
    """The third replication's value is what a run with its seed measures."""
    result = replicate(blueprint, 3, seed=5, until=None, measures=MEASURES)
    context = SimulationContext.from_yaml(blueprint)
    context.random_seed = result.seeds[2]
    context.run()
    readings = context.env.stats.readings()
    for measure in MEASURES:
        assert readings[f"Shop.stats.{measure}"] == result.values[measure][2]
    rows = [r for r in result.table if r["replication"] == 3]
    assert {r["seed"] for r in rows} == {result.seeds[2]}


def test_workers_give_the_same_values(blueprint):
    serial = replicate(blueprint, 4, seed=3, until=1000, measures=MEASURES)
    parallel = replicate(blueprint, 4, seed=3, until=1000, measures=MEASURES, workers=2)
    assert parallel.values == serial.values


def test_workers_need_an_importable_model():
    with pytest.raises(ValueError, match="cannot be imported"):
        replicate(lambda: shop(), 2, seed=1, until=100, workers=2)


def test_an_unknown_measure_is_refused():
    with pytest.raises(ValueError, match="no statistic called tasks.packing"):
        replicate(shop, 2, seed=1, until=100, measures=["tasks.packing.wait_mean"])


def test_a_count_not_yet_published_is_zero_and_a_missing_mean_is_refused():
    def patient() -> SimulationContext:
        return shop(patience=0.5)

    # Over 5 seconds some runs renege and some do not
    result = replicate(patient, 6, seed=2, until=5, measures=["tasks.pack.reneged"])
    assert 0 in result.values["tasks.pack.reneged"]
    with pytest.raises(ValueError, match="no value in run"):
        replicate(shop, 6, seed=2, until=2, measures=["tasks.pack.wait_mean"])


def test_overrides_apply_to_every_replication(blueprint):
    base = replicate(blueprint, 2, seed=4, until=1000, measures=MEASURES)
    two = replicate(
        blueprint,
        2,
        seed=4,
        until=1000,
        measures=MEASURES,
        overrides={
            "Shop.resources.packers.max_cap": 2,
            "Shop.resources.packers.current_cap": 2,
        },
    )
    assert max(two.values["tasks.pack.wait_mean"]) < min(
        base.values["tasks.pack.wait_mean"]
    )
    with pytest.raises(ValueError, match="not a registry path"):
        replicate(blueprint, 2, seed=4, until=100, overrides={"Shop.nope": 1})
    with pytest.raises(ValueError, match="cannot be set"):
        replicate(
            blueprint,
            2,
            seed=4,
            until=100,
            overrides={"Shop.resources.packers.current_cap": 5},
        )


class _Capture(BaseEgress):
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def test_the_sinks_receive_the_summary_once():
    capture = _Capture()

    def with_sink() -> SimulationContext:
        return shop().add_egress(capture)

    result = replicate(
        with_sink, 2, seed=6, until=500, measures=["tasks.pack.wait_mean"]
    )
    published = {r["path_id"]: r["value"] for r in capture.records}
    assert [r["stream_type"] for r in capture.records] == ["telemetry"] * 5
    assert {r["sim_ts"] for r in capture.records} == {500.0}
    [row] = result.summary
    assert published == {
        "Shop.stats.tasks.pack.wait_mean.replication.1": result.values[
            "tasks.pack.wait_mean"
        ][0],
        "Shop.stats.tasks.pack.wait_mean.replication.2": result.values[
            "tasks.pack.wait_mean"
        ][1],
        "Shop.stats.tasks.pack.wait_mean.mean": row["mean"],
        "Shop.stats.tasks.pack.wait_mean.half_width": row["half_width"],
        "Shop.stats.tasks.pack.wait_mean.n": 2,
    }


def test_replicate_refuses_bad_arguments():
    with pytest.raises(ValueError, match="2 or more"):
        replicate(shop, 1, seed=1, until=10)
    with pytest.raises(ValueError, match="end time"):
        replicate(shop, 2, seed=1, until=None)


def test_cli_prints_the_summary(blueprint):
    result = CliRunner().invoke(
        cli,
        [
            "run",
            str(blueprint),
            "--replications",
            "3",
            "--seed",
            "5",
            "--measure",
            "tasks.pack.wait_mean",
        ],
    )
    assert result.exit_code == 0, result.output
    expected = replicate(
        blueprint, 3, seed=5, until=None, measures=["tasks.pack.wait_mean"]
    )
    [row] = expected.summary
    assert f"{row['mean']:.4g}" in result.output
    assert str(expected.seeds[0]) in result.output
    bad = CliRunner().invoke(
        cli, ["run", str(blueprint), "--replications", "2", "--measure", "nope"]
    )
    assert bad.exit_code == 1 and "no statistic called nope" in bad.output


class _NeverRun(BaseIngress):
    """An ingress whose connection must never be opened."""

    def __init__(self):
        self.ran = False

    async def run(self, ingress_queue):
        self.ran = True


def test_replications_never_start_the_ingress():
    """Neither the replications nor the build that sends the result to the sinks
    connect the model's ingress."""
    for with_sink in (False, True):
        ingress = _NeverRun()

        def model() -> SimulationContext:
            app = shop().add_ingress(ingress)
            return app.add_egress(_Capture()) if with_sink else app

        replicate(model, 2, seed=1, until=100, measures=["tasks.pack.finished"])
        assert not ingress.ran


def test_run_before_runs_once_more_only_with_sinks():
    calls = []

    def model(sinks) -> SimulationContext:
        app = shop()
        app._before_run = [lambda: calls.append(1)]
        return app.add_egress(_Capture()) if sinks else app

    replicate(
        lambda: model(False), 3, seed=1, until=50, measures=["tasks.pack.finished"]
    )
    assert len(calls) == 3
    calls.clear()
    replicate(
        lambda: model(True), 3, seed=1, until=50, measures=["tasks.pack.finished"]
    )
    assert len(calls) == 4


def test_workers_refuse_a_function_from_an_interactive_session(monkeypatch):
    """A function defined in python -c or a notebook cannot reach a worker."""
    import sys
    import types

    session = types.ModuleType("__main__")  # no __file__, as in python -c
    monkeypatch.setitem(sys.modules, "__main__", session)

    def typed_in():
        return shop()

    # A top-level function of the session: it pickles by name, so only the
    # check for a session without a file refuses it
    typed_in.__module__ = "__main__"
    typed_in.__qualname__ = "typed_in"
    session.typed_in = typed_in
    with pytest.raises(ValueError, match="Put the function in a file"):
        replicate(typed_in, 2, seed=1, until=100, workers=2)
    with pytest.raises(ValueError, match="Put the function in a file"):
        replicate(LowLevelModel(typed_in, "Shop"), 2, seed=1, until=100, workers=2)


def test_a_lower_max_cap_that_would_clamp_current_cap_is_refused(blueprint):
    with pytest.raises(ValueError, match="current_cap' is 1; set current_cap first"):
        replicate(
            blueprint,
            2,
            seed=4,
            until=100,
            overrides={"Shop.resources.packers.max_cap": 0},
        )


def test_the_summary_heading_keeps_the_level():
    result = replicate(shop, 2, seed=1, until=200, measures=["tasks.pack.finished"])
    result.level = 0.975
    assert "half-width (97.5%)" in result.summary_text()

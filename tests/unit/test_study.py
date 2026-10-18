"""Local studies: designs, pairing, comparisons, checks, observed values and the report."""

from pathlib import Path
from typing import Optional

import pytest
from typer.testing import CliRunner

from desflow import (
    CapacityConfig,
    CheckFailed,
    DistributionConfig,
    DynamicResource,
    LowLevelModel,
    SimParameter,
    SimulationContext,
    run_study,
)
from desflow.blueprint.cli import app as cli
from desflow.blueprint.loader import BlueprintError
from desflow.study import results as found
from desflow.study.models import RunResult
from desflow.study.spec import StudySpec

SHOP = """\
simulation: {sim_id: Shop, factor: 0}
resources: {packers: {current_cap: 2, max_cap: 4}}
arrivals: {orders: {dist: exponential, rate: 0.4, spawn: pack}}
services: {packing: {dist: gamma, shape: 4, scale: 1}}
tasks: {pack: {service: packing, resource: packers, payload: {event_type: packed}}}
"""
STUDY = """\
study: packers
question: Does a third packer keep the mean wait under 2 seconds?
model: shop.yaml
assumptions: ["Orders arrive at random at a steady rate."]
inputs: {Shop.service.packing: "40 packing times measured at the bench"}
until: 30 min
warmup: 3 min
replications: 4
seed: 42
measures: [tasks.pack.wait_mean, resources.packers.utilization_avg]
variants: {base: {}, three: {Shop.resources.packers.current_cap: 3}}
compare: [{baseline: base, variant: three}]
checks: [{measure: tasks.pack.wait_mean, variant: three, max: 2}]
limits: ["Thirty minutes of one shop only."]
"""
MEASURES = ["tasks.pack.wait_mean", "resources.packers.utilization_avg"]


def shop(orders: Optional[int] = None) -> SimulationContext:
    """The blueprint's shop, written with the builder; `orders` stops the arrivals
    after that many."""
    app = (
        SimulationContext(sim_id="Shop", factor=0)
        .add_resource("packers", current_cap=2, max_cap=4)
        .add_arrival("orders", dist="exponential", rate=0.4)
        .add_service("packing", dist="gamma", shape=4, scale=1)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id):
        return {"event_type": "packed"}

    @app.arrival_loop("orders")
    def order_loop(context):
        task_id = 0
        while orders is None or task_id < orders:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack(task_id, entity=entity))
            task_id += 1

    return app


def _pack(env, sampler, packers, task_id, wrap):
    env.stats.task_queued("pack", task_id)
    with packers.request() as request:
        yield request
        env.stats.task_started("pack", task_id)
        yield env.timeout(
            sampler.sample(env.registry.get_config("Shop.service.packing"))
        )
        if wrap:
            config = env.registry.get_config("Shop.service.wrapping")
            yield env.timeout(sampler.sample(config))
    env.stats.task_finished("pack", task_id)
    env.stats.flow_finished("orders")


def _orders(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        wrap = env.registry.get("Shop.variables.wrap").value
        env.process(_pack(env, sampler, packers, task_id, wrap))
        task_id += 1


def low_level(env, sampler):
    """The same shop, written with the low-level API; `wrap` adds a second step."""
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
            service={
                "packing": DistributionConfig(dist="gamma", shape=4, scale=1),
                "wrapping": DistributionConfig(dist="exponential", rate=2.0),
            },
            resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
            variables={"wrap": 0},
        )
    )
    env.process(_orders(env, sampler, DynamicResource(env, "Shop", "packers")))


def _settings(**changes):
    import yaml

    settings = yaml.safe_load(STUDY)
    settings.update(changes)
    return settings


@pytest.fixture
def folder(tmp_path) -> Path:
    (tmp_path / "shop.yaml").write_text(SHOP)
    (tmp_path / "study.yaml").write_text(STUDY)
    return tmp_path


def test_a_sweep_runs_every_variant_at_every_point():
    spec = StudySpec.model_validate(
        _settings(
            model=shop,
            sweep={
                "Shop.arrival.orders.rate": [0.3, 0.4],
                "Shop.service.packing.scale": [1, 1.2],
            },
        )
    )
    names = [(d.variant, d.point) for d in found.designs(spec)]
    assert len(names) == 8
    assert (
        "three",
        {"Shop.arrival.orders.rate": 0.4, "Shop.service.packing.scale": 1.2},
    ) in names
    three = [d for d in found.designs(spec) if d.variant == "three"]
    assert all(d.overrides["Shop.resources.packers.current_cap"] == 3 for d in three)


def test_one_factor_sensitivity_changes_only_that_factor(tmp_path):
    result = run_study(
        _settings(
            model=shop,
            variants={"base": {}},
            sweep={"Shop.arrival.orders.rate": [0.3, 0.4, 0.5]},
            compare=[],
            checks=[],
            output=str(tmp_path / "out"),
        )
    )
    overrides = [d.overrides for d in result.designs]
    assert overrides == [{"Shop.arrival.orders.rate": r} for r in (0.3, 0.4, 0.5)]
    waits = [
        row["mean"]
        for row in result.summary
        if row["statistic"] == "tasks.pack.wait_mean"
    ]
    assert waits == sorted(waits)  # more orders, longer waits
    assert all(
        len(set(d.seeds)) == 4 and d.seeds == result.designs[0].seeds
        for d in result.designs
    )


def test_a_study_repeats_and_the_three_forms_agree(folder, tmp_path):
    first = run_study(folder / "study.yaml")
    again = run_study(folder / "study.yaml")
    assert first.results == again.results
    keep = (folder / "results" / "packers" / "results.csv").read_text()
    run_study(_settings(model=shop, output=str(tmp_path / "builder")))
    low = run_study(
        _settings(
            model=LowLevelModel(low_level, "Shop"),
            output=str(tmp_path / "low"),
        )
    )
    assert (tmp_path / "builder" / "results.csv").read_text() == keep
    measures = [r for r in low.results if r["kind"] == "measure"]
    assert measures == [r for r in first.results if r["kind"] == "measure"]


def test_a_paired_interval_matches_a_hand_calculation(tmp_path):
    """Differences 1 to 5 seed by seed: mean 3, half-width 2.776 x sqrt(2.5 / 5)."""
    spec = StudySpec.model_validate(
        _settings(
            model=shop, replications=5, measures=["tasks.pack.wait_mean"], checks=[]
        )
    )
    base, three = found.designs(spec)
    for design, values in ((base, [10, 20, 30, 40, 50]), (three, [11, 22, 33, 44, 55])):
        design.runs = [
            RunResult({"Shop.stats.tasks.pack.wait_mean": v}, ["s"], "Shop")
            for v in values
        ]
    rows = found.result_rows([base, three], spec.measures, [])
    [row] = found.comparison_rows(spec, [base, three], rows)
    assert row["difference"] == pytest.approx(3.0)
    assert row["half_width"] == pytest.approx(2.776445 * (2.5 / 5) ** 0.5)
    assert row["streams_both"] == "s" and row["streams_variant_only"] == ""


def test_stream_alignment_is_reported(tmp_path):
    """A variant that draws from a stream the baseline never uses names it."""
    result = run_study(
        _settings(
            model=LowLevelModel(low_level, "Shop"),
            variants={"base": {}, "wrap": {"Shop.variables.wrap": 1}},
            compare=[{"baseline": "base", "variant": "wrap"}],
            checks=[],
            output=str(tmp_path / "o"),
        ),
    )
    row = result.comparisons[0]
    assert row["streams_variant_only"] == "service.wrapping"
    assert row["streams_both"] == "arrival.orders;service.packing"
    assert "only the variant: service.wrapping" in result.report


def test_independent_pairing_spawns_seeds_per_design():
    spec = StudySpec.model_validate(
        _settings(model=shop, pairing="independent", compare=[])
    )
    base, three = found.designs(spec)
    assert base.seeds != three.seeds and len(set(base.seeds + three.seeds)) == 8


def test_a_failed_check_writes_everything_then_fails(folder):
    study = folder / "study.yaml"
    study.write_text(STUDY.replace("variant: three, max: 2", "variant: base, max: 2"))
    with pytest.raises(CheckFailed, match="base: tasks.pack.wait_mean max 2") as caught:
        run_study(study)
    assert caught.value.result.failed[0]["design"] == "base"
    out = folder / "results" / "packers"
    assert sorted(p.name for p in out.iterdir()) == [
        "checks.csv",
        "comparisons.csv",
        "report.md",
        "results.csv",
        "study.json",
        "summary.csv",
    ]
    assert (
        "| base | `tasks.pack.wait_mean` | the upper end | max 2 |"
        in (out / "report.md").read_text()
    )
    cli_run = CliRunner().invoke(cli, ["study", str(study)])
    assert cli_run.exit_code == 1 and "FAILED: base" in cli_run.output
    assert not run_study(study, raise_on_failure=False).failed == []


def test_a_check_on_the_interval_is_stricter_than_on_the_mean(tmp_path):
    checks = [
        {"measure": "tasks.pack.wait_mean", "variant": "base", "max": m, "on": on}
        for m in (6.0,)
        for on in ("mean", "interval")
    ]
    result = run_study(
        _settings(model=shop, checks=checks, output=str(tmp_path / "o")),
        raise_on_failure=False,
    )
    mean, upper = result.checks
    assert mean["value"] < upper["value"]
    assert mean["passed"] and not upper["passed"]


OBSERVED = "measure,value,unit,population,horizon\n{}\n"


@pytest.mark.parametrize(
    "row,error",
    [
        ("tasks.pack.wait_mean,4.5,min,started instances,27 min", "is in s"),
        ("tasks.pack.wait_mean,4.5,s,all orders,27 min", "is measured on"),
        ("tasks.pack.wait_mean,4.5,s,started instances,30 min", "horizon is 1800"),
        ("tasks.pack.service_mean,4,s,finished instances,27 min", "not one of"),
    ],
)
def test_observed_values_must_match_the_measure(folder, row, error):
    (folder / "observed.csv").write_text(OBSERVED.format(row))
    (folder / "study.yaml").write_text(STUDY + "observed: {file: observed.csv}\n")
    with pytest.raises(ValueError, match=error):
        run_study(folder / "study.yaml")


def test_an_observed_value_is_placed_against_the_interval(folder):
    rows = (
        "tasks.pack.wait_mean,4.5,s,started instances,27 min\n"
        "resources.packers.utilization_avg,95,percent,time average,1620"
    )
    (folder / "observed.csv").write_text(OBSERVED.format(rows))
    (folder / "study.yaml").write_text(STUDY + "observed: {file: observed.csv}\n")
    result = run_study(folder / "study.yaml")
    inside = {row["measure"]: row["inside"] for row in result.observed}
    assert inside == {
        "tasks.pack.wait_mean": True,
        "resources.packers.utilization_avg": False,
    }
    assert "No observed values are given" not in result.report
    row = next(r for r in result.summary if r["statistic"] == "tasks.pack.wait_mean")
    assert (row["unit"], row["population"]) == ("s", "started instances")
    assert "| base | `tasks.pack.wait_mean` | started instances |" in result.report
    header = (folder / "results" / "packers" / "results.csv").read_text().split("\n")[0]
    assert (
        header == "design,variant,replication,seed,statistic,kind,unit,population,value"
    )


def test_the_report_names_missing_inputs_and_rebuilds_the_same(folder):
    (folder / "study.yaml").write_text(
        "\n".join(
            line
            for line in STUDY.split("\n")
            if not line.startswith(("question", "assumptions", "limits"))
        )
    )
    result = run_study(folder / "study.yaml")
    missing = result.report[result.report.index("## Missing Inputs") :]
    for text in (
        "The question the study answers is not stated.",
        "No assumptions are stated.",
        "The provenance of `Shop.arrival.orders` is not stated.",
        "No observed values are given",
        "No limits are stated.",
    ):
        assert text in missing
    assert "provenance of `Shop.service.packing`" not in missing
    assert "## Question\n\nNot stated." in result.report
    out = folder / "results" / "packers"
    (out / "report.md").unlink()
    rebuilt = CliRunner().invoke(cli, ["report", str(out)])
    assert rebuilt.exit_code == 0
    assert (out / "report.md").read_text() == result.report


def test_a_fixed_horizon_leaves_orders_unfinished_and_a_drained_run_does_not(tmp_path):
    """The shop's blueprint with run.drain: every replication stops the arrivals
    at the until and packs every order already in the shop, so finished equals
    started and nothing is left; the same blueprint without the drain ends with
    orders still in the shop. The report counts both populations."""
    for name, run in (
        ("fixed", "run: {until: 120}\n"),
        ("drained", "run: {until: 120, drain: true}\n"),
    ):
        (tmp_path / f"{name}.yaml").write_text(SHOP + run)

    def study(name):
        return run_study(
            _settings(
                model=str(tmp_path / f"{name}.yaml"),
                until=120,
                warmup=None,
                measures=["tasks.pack.wait_mean"],
                variants={"base": {}},
                compare=[],
                checks=[],
                output=str(tmp_path / f"out-{name}"),
            )
        )

    fixed, drained = study("fixed"), study("drained")

    def counts(result, statistic):
        return [r["value"] for r in result.results if r["statistic"] == statistic]

    started = counts(drained, "flows.orders.started")
    assert counts(drained, "flows.orders.finished") == started
    assert counts(drained, "flows.orders.in_system") == [0] * 4
    assert all(n > 0 for n in counts(fixed, "flows.orders.in_system"))
    for result in (fixed, drained):
        assert "`flows.orders.started`" in result.report
        assert "`flows.orders.in_system`" in result.report
    rows = [r for r in drained.results if r["statistic"] == "flows.orders.in_system"]
    assert {r["population"] for r in rows} == {"end of run"}


def test_bad_settings_are_refused_with_their_line(folder):
    study = folder / "study.yaml"
    study.write_text(STUDY + "replication: 3\n")
    with pytest.raises(BlueprintError, match=r"study.yaml:15: replication"):
        run_study(study)
    study.write_text(STUDY.replace("current_cap: 3", "capacity: 3"))
    with pytest.raises(
        BlueprintError,
        match=r"study.yaml:11: three: 'Shop.resources.packers.capacity' is not a registry path",
    ):
        run_study(study)
    study.write_text(STUDY.replace("Shop.service.packing:", "Shop.service.pack:"))
    with pytest.raises(
        BlueprintError, match="study.yaml:5: inputs names Shop.service.pack"
    ):
        run_study(study)
    study.write_text(STUDY.replace("current_cap: 3", "current_cap: 9"))
    with pytest.raises(BlueprintError, match="cannot be set to 9"):
        run_study(study)
    with pytest.raises(ValueError, match="pairing: common"):
        StudySpec.model_validate(_settings(model=shop, pairing="independent"))
    with pytest.raises(ValueError, match="add it to measures"):
        StudySpec.model_validate(
            _settings(model=shop, checks=[{"measure": "tasks.pack.wait_max", "max": 1}])
        )
    with pytest.raises(ValueError, match="sweep also sets"):
        StudySpec.model_validate(
            _settings(model=shop, sweep={"Shop.resources.packers.current_cap": [2, 3]})
        )
    cli_run = CliRunner().invoke(cli, ["study", str(study)])
    assert cli_run.exit_code == 2 and "cannot be set to 9" in cli_run.output


def test_a_python_reference_names_a_builder_factory(folder):
    (folder / "shop_model.py").write_text(
        "from tests.unit.test_study import shop\n\ndef make_shop():\n    return shop()\n"
    )
    study = folder / "study.yaml"
    study.write_text(
        STUDY.replace("model: shop.yaml", "model: !python shop_model.make_shop")
    )
    result = run_study(study)
    evidence = found.read_json(result.output / "study.json")
    assert evidence["model"]["form"] == "builder"
    assert evidence["model"]["source"] == "shop_model:make_shop"
    assert len(evidence["model"]["sha256"]) == 64
    assert evidence["designs"][1]["overrides"] == {
        "Shop.resources.packers.current_cap": 3
    }
    assert evidence["parameters"]["Shop.service.packing"] == {
        "dist": "gamma",
        "shape": 4.0,
        "scale": 1.0,
    }


def test_report_needs_a_study_folder(tmp_path):
    run = CliRunner().invoke(cli, ["report", str(tmp_path)])
    assert run.exit_code == 2 and "study.json" in run.output


def test_a_missing_model_or_observed_file_is_an_invalid_study(folder):
    """Both exit 2, the status of an invalid study, naming the line of the key."""
    study = folder / "study.yaml"
    study.write_text(STUDY.replace("model: shop.yaml", "model: shops.yaml"))
    run = CliRunner().invoke(cli, ["study", str(study)])
    assert run.exit_code == 2
    assert "study.yaml:3: model shops.yaml: no such file" in run.output
    study.write_text(STUDY + "observed: {file: observed.csv}\n")
    run = CliRunner().invoke(cli, ["study", str(study)])
    assert run.exit_code == 2
    assert "study.yaml:15: observed file observed.csv: no such file" in run.output


SHARED = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={
        "packing": DistributionConfig(dist="gamma", shape=4, scale=1),
        "wrapping": DistributionConfig(dist="exponential", rate=2.0),
    },
    resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
    variables={"wrap": 0},
)


def shared_model(env, sampler):
    """The low-level shop, registering parameters that live in this module."""
    env.registry.register_sim_parameter(SHARED)
    env.process(_orders(env, sampler, DynamicResource(env, "Shop", "packers")))


def test_a_low_level_model_keeps_its_own_parameters(tmp_path):
    """A variant's value is undone after each run, so the baseline is not changed."""
    result = run_study(
        _settings(
            model=LowLevelModel(shared_model, "Shop"),
            variants={"three": {"Shop.resources.packers.current_cap": 3}, "base": {}},
            compare=[{"baseline": "base", "variant": "three"}],
            checks=[],
            output=str(tmp_path / "o"),
        )
    )
    assert result.comparisons[0]["difference"] < 0
    assert SHARED.resources["packers"].current_cap == 2
    assert SHARED.variables == {"wrap": 0}

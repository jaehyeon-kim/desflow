"""Every example written three ways publishes the same records.

Each example exists as YAML, builder and low-level code. Each version is run unpaced
into a capturing sink in place of its real ones, and its events and telemetry are
compared with the builder version's: the same keys and paths, the same values, at the
same simulation times. Nothing here contacts a database, a broker or a catalog.
"""

import asyncio
import importlib.util
import queue
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from desflow import (
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimulationContext,
)
from desflow.connectors.egress.base import BaseEgress

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"


class _Capture(BaseEgress):
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _load(path):
    spec = importlib.util.spec_from_file_location(path.stem + "_twin", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _records(records):
    """Every record without its timestamp, and without the lag metric.

    Timestamps depend on the moment the run started, and the lag metric on how fast
    the machine is, so both are left out.
    """
    return [
        {k: v for k, v in r.items() if k != "timestamp"}
        for r in records
        if not str(r.get("path_id", "")).startswith("system.")
    ]


def _run_builder(app, until, **run):
    """Runs a builder or YAML version into two captures: data, then ground truth.

    `run` holds `stop` and `drain` for a version whose run ends by its own rules.
    """
    app.factor = 0.0
    app.go_live_at = None  # the backfill example stays unpaced for the comparison
    capture, labels = _Capture(), _Capture()
    app._ingress_providers = []
    app._egress_providers = [capture, labels]
    app._egress_predicates = [None, None]
    app._egress_batch_sizes = [None, None]
    app._egress_flush_intervals = [None, None]
    app._egress_labels = [False, True]
    app.run(until=until, **run)
    return _records(capture.records + labels.records)


def _run_low_level(module, until, processes):
    """Runs the low-level version's processes, built by `processes(env, sampler)`.

    A module's `WARMUP` is passed to the egress, as its own `run` does. A module
    whose run ends by its own rules, `run_to_end(env, shop, source)`, runs that
    instead of a fixed `until`.
    """
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(module.PARAMS)
    sampler = Sampler(rng=np.random.default_rng(42))
    capture, labels = _Capture(), _Capture()
    env.setup_egress(
        [capture, labels],
        labels=[False, True],
        warmup=getattr(module, "WARMUP", None),
    )
    started = [env.process(process) for process in processes(env, sampler)]
    try:
        if hasattr(module, "run_to_end"):
            module.run_to_end(env, env.example_shop, started[0])
        else:
            env.run(until=until)
    finally:
        env.teardown()
    return _records(capture.records + labels.records)


def _packers(env):
    return DynamicResource(env, "Shop", "packers")


def _from_yaml(path):
    """Builds a blueprint, with its folder importable for a `!python` reference."""
    sys.path.insert(0, str(path.parent))
    try:
        sys.modules.pop("postgres_orders_logic", None)
        return SimulationContext.from_yaml(path)
    finally:
        sys.path.remove(str(path.parent))


def _local(m, env, sampler):
    env.setup_stats("Shop", "minimal", interval=2.0)
    packers = _packers(env)
    return [m.arrival_generator(env, sampler, packers), m.capacity_changes(env)]


def _tasks_and_stats(interval):
    """The processes of an example that publishes the minimal statistics."""

    def processes(m, env, sampler):
        env.setup_stats("Shop", "minimal", interval=interval)
        return [m.arrival_generator(env, sampler, _packers(env))]

    return processes


def _tasks(m, env, sampler):
    return [m.arrival_generator(env, sampler, _packers(env))]


def _orders(m, env, sampler):
    return [m.order_generator(env, sampler)]


def _orders_with_items(m, env, sampler):
    return [m.order_generator(env, sampler, max_items=5)]


def _stock(m, env, sampler):
    env.setup_stats("Shop", "minimal", interval=10.0)
    return m.processes(env, sampler, m.build(env))


def _built(m, env, sampler):
    return m.processes(env, sampler, m.build(env))


def _patience(m, env, sampler):
    """An example that publishes the basic statistics every 30 seconds."""
    env.setup_stats("Shop", "basic", interval=30.0)
    return m.processes(env, sampler, m.build(env))


def _interaction(m, env, sampler):
    env.setup_stats("Shop", "basic", interval=60.0)
    return m.processes(env, sampler, m.build(env, sampler))


def _competing(m, env, sampler):
    env.setup_stats("Shop", "basic", interval=30.0)
    return m.processes(env, sampler, m.build(env, sampler))


def _ending(m, env, sampler):
    env.setup_stats("Shop", "basic", interval=30.0)
    env.example_shop = m.build(env)
    return m.processes(env, sampler, env.example_shop)


# How the builder version of an example that ends by its own rules is run
RUN = {
    "end_conditions": {
        "drain": {"limit": 60},
        "stop": {"condition": "level.boxes == 0"},
    }
}


def _every_minute(m, env, sampler):
    env.setup_stats("Shop", "basic", interval=60.0)
    return m.processes(env, sampler, m.build(env))


def _every_minute_typical(m, env, sampler):
    env.setup_stats("Shop", "typical", interval=60.0)
    return m.processes(env, sampler, m.build(env))


def _measuring(m, env, sampler):
    env.setup_stats("Shop", "all", interval=300)
    return _built(m, env, sampler)


def _typical(m, env, sampler):
    env.setup_stats("Shop", "typical", interval=600)
    return _built(m, env, sampler)


# Example name, its YAML file, the extra its connectors import, the simulated seconds
# to compare, and the processes its low-level version starts.
EXAMPLE_CASES = [
    ("local", "local.yaml", None, 30, _local),
    ("kafka", "kafka.yaml", "aiokafka", 60, _tasks_and_stats(2.0)),
    ("parquet", "parquet.yaml", "pyarrow", 600, _tasks),
    ("iceberg", "iceberg.yaml", "pyiceberg", 600, _tasks),
    ("postgres", "postgres.yaml", "asyncpg", 60, _orders),
    ("redis", "redis.yaml", "redis", 60, _orders),
    ("backfill_live", "backfill_live.yaml", "aiokafka", 300, _tasks_and_stats(30.0)),
    (
        "postgres_orders",
        "advanced/postgres_orders.yaml",
        "asyncpg",
        120,
        _orders_with_items,
    ),
    ("stock", "stock.yaml", None, 60, _stock),
    ("flow", "flow.yaml", None, 120, _built),
    ("measuring", "measuring.yaml", None, 1200, _measuring),
    ("patience", "patience.yaml", None, 120, _patience),
    ("filtered_inventory", "filtered_inventory.yaml", None, 120, _patience),
    ("process_interaction", "process_interaction.yaml", None, 300, _interaction),
    ("interruptions", "interruptions.yaml", None, 120, _patience),
    ("competing_resources", "competing_resources.yaml", None, 120, _competing),
    ("end_conditions", "end_conditions.yaml", None, 120, _ending),
    ("breakdowns", "breakdowns.yaml", None, 300, _every_minute),
    ("changing_demand", "changing_demand.yaml", None, 300, _every_minute_typical),
    ("local_replay", "local_replay.yaml", None, 120, _built),
    ("replications", "replications.yaml", None, 1200, _typical),
    ("study", "study_shop.yaml", None, 600, _built),
    ("fitting", "fitting.yaml", "scipy", 1200, _typical),
]


@pytest.fixture
def no_catalog():
    """The builder Iceberg example connects to its catalog when it is imported."""
    with patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()):
        yield


def _builder_records(name, until):
    """An example's `app`, or a new context from its `shop` factory."""
    builder = _load(EXAMPLES / "builder" / f"{name}_example.py")
    app = builder.app if hasattr(builder, "app") else builder.shop()
    return _run_builder(app, until, **RUN.get(name, {}))


@pytest.mark.parametrize(
    "name,yaml_file,extra,until,processes",
    EXAMPLE_CASES,
    ids=[case[0] for case in EXAMPLE_CASES],
)
def test_low_level_matches_builder(
    name, yaml_file, extra, until, processes, no_catalog
):
    if extra:
        pytest.importorskip(extra)
    expected = _builder_records(name, until)
    low = _load(EXAMPLES / "low-level" / f"{name}_example.py")
    got = _run_low_level(low, until, lambda env, s: processes(low, env, s))
    assert got and got == expected


@pytest.mark.parametrize(
    "name,yaml_file,extra,until,processes",
    EXAMPLE_CASES,
    ids=[case[0] for case in EXAMPLE_CASES],
)
def test_yaml_matches_builder(name, yaml_file, extra, until, processes, no_catalog):
    if extra:
        pytest.importorskip(extra)
    expected = _builder_records(name, until)
    yaml_app = _from_yaml(EXAMPLES / "yaml" / yaml_file)
    assert _run_builder(yaml_app, until) == expected


def test_every_example_is_compared():
    """A new example must join the comparison, in all three APIs."""
    names = {
        p.stem.removesuffix("_example") for p in (EXAMPLES / "builder").glob("*.py")
    }
    assert names == {case[0] for case in EXAMPLE_CASES}


def test_study_example_gives_the_same_results_in_every_api(tmp_path):
    """The study's results and checks are the same whichever way its model is written."""
    from desflow import run_study

    results = {}
    for api in ("builder", "low-level"):
        module = _load(EXAMPLES / api / "study_example.py")
        result = module.run(output=tmp_path / api)
        results[api] = (tmp_path / api / "results.csv").read_text()
        assert [row["passed"] for row in result.checks] == [True]
    run_study(EXAMPLES / "yaml" / "study.yaml", output=tmp_path / "yaml")
    results["yaml"] = (tmp_path / "yaml" / "results.csv").read_text()
    assert results["yaml"] == results["builder"] == results["low-level"]


def test_fitting_example_writes_the_file_its_blueprint_loads(tmp_path):
    """desflow fit on the example's data writes packing_fit.yaml byte for byte, so
    the blueprint loads what the fit gives today."""
    pytest.importorskip("scipy")
    from typer.testing import CliRunner

    from desflow.blueprint.cli import app as cli

    out = tmp_path / "packing_fit.yaml"
    run = CliRunner().invoke(
        cli,
        [
            "fit",
            str(EXAMPLES / "packing_times.csv"),
            "--column",
            "packing_seconds",
            "--lower",
            "0",
            "--out",
            str(out),
        ],
    )
    assert run.exit_code == 0, run.output
    assert out.read_text() == (EXAMPLES / "yaml" / "packing_fit.yaml").read_text()

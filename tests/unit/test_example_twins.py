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

from dynamic_des import (
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimulationContext,
)
from dynamic_des.connectors.egress.base import BaseEgress

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
    """Events by key and telemetry by path, with their values and simulation times.

    Timestamps depend on the moment the run started, and the lag metric on how fast
    the machine is, so both are left out.
    """
    return [
        (r["stream_type"], r.get("key") or r.get("path_id"), r["value"], r["sim_ts"])
        for r in records
        if not str(r.get("path_id", "")).startswith("system.")
    ]


def _run_builder(app, until):
    app.factor = 0.0
    app.go_live_at = None  # the backfill example stays unpaced for the comparison
    capture = _Capture()
    app._ingress_providers = []
    app._egress_providers = [capture]
    app._egress_predicates = [None]
    app._egress_batch_sizes = [None]
    app._egress_flush_intervals = [None]
    app.run(until=until)
    return _records(capture.records)


def _run_low_level(module, until, processes):
    """Runs the low-level version's processes, built by `processes(env, sampler)`."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(module.PARAMS)
    sampler = Sampler(rng=np.random.default_rng(42))
    capture = _Capture()
    env.setup_egress([capture])
    for process in processes(env, sampler):
        env.process(process)
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return _records(capture.records)


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
    packers = _packers(env)
    return [
        m.arrival_generator(env, sampler, packers),
        m.telemetry_generator(env, packers),
        m.capacity_changes(env),
    ]


def _tasks_and_telemetry(m, env, sampler):
    packers = _packers(env)
    return [
        m.arrival_generator(env, sampler, packers),
        m.telemetry_generator(env, packers),
    ]


def _tasks(m, env, sampler):
    return [m.arrival_generator(env, sampler, _packers(env))]


def _orders(m, env, sampler):
    return [m.order_generator(env, sampler)]


def _orders_with_items(m, env, sampler):
    return [m.order_generator(env, sampler, max_items=5)]


# Example name, its YAML file, the extra its connectors import, the simulated seconds
# to compare, and the processes its low-level version starts.
EXAMPLE_CASES = [
    ("local", "local.yaml", None, 30, _local),
    ("kafka", "kafka.yaml", "aiokafka", 60, _tasks_and_telemetry),
    ("parquet", "parquet.yaml", "pyarrow", 600, _tasks),
    ("iceberg", "iceberg.yaml", "pyiceberg", 600, _tasks),
    ("postgres", "postgres.yaml", "asyncpg", 60, _orders),
    ("redis", "redis.yaml", "redis", 60, _orders),
    ("backfill_live", "backfill_live.yaml", "aiokafka", 300, _tasks_and_telemetry),
    (
        "postgres_orders",
        "advanced/postgres_orders.yaml",
        "asyncpg",
        120,
        _orders_with_items,
    ),
]


@pytest.fixture
def no_catalog():
    """The builder Iceberg example connects to its catalog when it is imported."""
    with patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()):
        yield


def _builder_records(name, until):
    builder = _load(EXAMPLES / "builder" / f"{name}_example.py")
    return _run_builder(builder.app, until)


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

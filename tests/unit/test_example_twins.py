"""The examples written three ways publish the same records.

The orders example exists as YAML, builder and low-level code; the backfill example as
YAML, builder and low-level code. Each Python version is run unpaced into a capturing
sink in place of its real ones, and its records are compared with the builder
version's. Nothing here contacts a database or a broker.
"""

import asyncio
import importlib.util
import queue
import sys
from pathlib import Path

import pytest

from dynamic_des import DynamicRealtimeEnvironment, DynamicResource, Sampler, SimulationContext
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


def _events(records):
    """Events by key and value; timestamps depend on the moment the run started."""
    return [(r["key"], r["value"]) for r in records if r["stream_type"] == "event"]


def _run_builder(app, until):
    app.factor = 0.0
    capture = _Capture()
    app._ingress_providers = []
    app._egress_providers = [capture]
    app._egress_predicates = [None]
    app._egress_batch_sizes = [None]
    app._egress_flush_intervals = [None]
    app.run(until=until)
    return capture.records


def _run_env(env, processes, until):
    capture = _Capture()
    env.setup_egress([capture])
    for p in processes:
        env.process(p)
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return capture.records


@pytest.mark.parametrize("until", [120])
def test_orders_low_level_matches_builder(until):
    pytest.importorskip("asyncpg")
    import numpy as np

    builder = _load(EXAMPLES / "declarative" / "postgres_orders_example.py")
    low = _load(EXAMPLES / "imperative" / "postgres_orders_example.py")
    expected = _events(_run_builder(builder.app, until))
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(low.PARAMS)
    sampler = Sampler(rng=np.random.default_rng(42))
    got = _events(_run_env(env, [low.order_generator(env, sampler, max_items=5)], until))
    assert got and got == expected


def test_orders_yaml_matches_builder():
    pytest.importorskip("asyncpg")
    builder = _load(EXAMPLES / "declarative" / "postgres_orders_example.py")
    expected = _events(_run_builder(builder.app, 120))
    sys.path.insert(0, str(EXAMPLES / "yaml" / "advanced"))
    try:
        sys.modules.pop("postgres_orders_logic", None)
        yaml_app = SimulationContext.from_yaml(EXAMPLES / "yaml" / "advanced" / "postgres_orders.yaml")
    finally:
        sys.path.remove(str(EXAMPLES / "yaml" / "advanced"))
    assert _events(_run_builder(yaml_app, 120)) == expected


def test_backfill_low_level_matches_builder():
    pytest.importorskip("aiokafka")
    import numpy as np

    builder = _load(EXAMPLES / "declarative" / "backfill_live_example.py")
    low = _load(EXAMPLES / "imperative" / "backfill_live_example.py")
    builder.app.go_live_at = None  # both stay unpaced for the comparison
    expected = _events(_run_builder(builder.app, 300))
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(low.PARAMS)
    lathe = DynamicResource(env, "Line_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(42))
    got = _events(
        _run_env(env, [low.arrival_generator(env, sampler, lathe), low.telemetry_generator(env, lathe)], 300)
    )
    assert got and got == expected


def test_backfill_yaml_matches_builder():
    pytest.importorskip("aiokafka")
    builder = _load(EXAMPLES / "declarative" / "backfill_live_example.py")
    expected = _events(_run_builder(builder.app, 300))
    yaml_app = SimulationContext.from_yaml(EXAMPLES / "yaml" / "backfill_live.yaml")
    assert _events(_run_builder(yaml_app, 300)) == expected

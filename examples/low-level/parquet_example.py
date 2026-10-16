"""Historical data generation, low-level API.

The low-level twin of `builder/parquet_example.py`. It wires
`DynamicRealtimeEnvironment`, the registry and the connectors by hand rather than
through the builder, which shows what `SimulationContext` does for you.

`factor=0.0` detaches the clock from real time, so seven days of shop orders are
generated as fast as the machine allows and written to Parquet. A router keeps the
lifecycle events and flattens them into rows. Writes to `data/`, or to S3 with the
variables `yaml/parquet.yaml` reads: `PARQUET_FILESYSTEM=s3` and `S3_ENDPOINT`,
`S3_ACCESS_KEY`, `S3_SECRET_KEY`.
"""

import logging
import os
from datetime import datetime, timedelta

import numpy as np

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    ParquetStorageEgress,
    Sampler,
    SimParameter,
)
from desflow.utils import time_to_seconds

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
logger = logging.getLogger("parquet_example")


def create_history_router(base_path: str):
    """
    Router Factory: Generates a router function injected with the correct
    base path, and flattens nested event payloads for Parquet.
    """

    def history_router(data: dict) -> str | None:
        if data.get("path_id") == "system.simulation.lag_seconds":
            return None

        stream_type = data.get("stream_type")

        if stream_type == "telemetry":
            return None

        # FLATTEN EVENT FOR PARQUET
        # If it's an event and has a nested 'value' dictionary, flatten it
        if (
            stream_type == "event"
            and "value" in data
            and isinstance(data["value"], dict)
        ):
            # Extract and remove the nested 'value' object
            nested_value = data.pop("value")
            # Merge the nested keys (path_id, status) directly into the root dict
            data.update(nested_value)

        return f"{base_path}/events.parquet"

    return history_router


# The shop starts with 4 packers and has room for 10.
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=2.0)},
    service={"packing": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
    resources={"packers": CapacityConfig(current_cap=4, max_cap=10)},
)


def pack_order(env, sampler, packers, key, task_id):
    """What @app.task does: queued, started, the service time, then finished."""
    path_id = "Shop.service.packing"
    env.publish_event(
        key,
        {"path_id": path_id, "status": "queued"},
        task="pack_order",
        instance=task_id,
    )
    with packers.request() as req:
        yield req
        env.publish_event(
            key,
            {"path_id": path_id, "status": "started"},
            task="pack_order",
            instance=task_id,
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(
            key,
            {"path_id": path_id, "status": "finished"},
            task="pack_order",
            instance=task_id,
        )


def arrival_generator(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def run():
    # ---------------------------------------------------------
    # 1. DUAL-MODE STORAGE CONFIGURATION
    # ---------------------------------------------------------
    # The same variables as yaml/parquet.yaml. Unset, the run writes to data/ on the local
    # disk. PARQUET_FILESYSTEM=s3 with the S3_* variables writes to S3-compatible storage,
    # such as SeaweedFS from `odctl up storage`. The egress builds the filesystem from this
    # mapping and creates the destination folder on its first write; an empty value is
    # left out, so the PyArrow default applies.
    base_path = os.getenv("DEST_PATH", "data")
    filesystem = {
        "type": os.getenv("PARQUET_FILESYSTEM", "local"),
        "endpoint_override": os.getenv("S3_ENDPOINT", ""),
        "access_key": os.getenv("S3_ACCESS_KEY", ""),
        "secret_key": os.getenv("S3_SECRET_KEY", ""),
    }

    # ---------------------------------------------------------
    # 2. SIMULATION SETUP
    # ---------------------------------------------------------
    start_time = datetime.now() - timedelta(days=7)
    env = DynamicRealtimeEnvironment(factor=0.0, logical_start_time=start_time)
    env.registry.register_sim_parameter(PARAMS)

    # Initialize Egress with the dynamic router and conditional filesystem
    router = create_history_router(base_path)
    egress = ParquetStorageEgress(path_router=router, filesystem=filesystem)

    # batch_size=5000 for compression, flush_interval=86400 (1 day in sim time)
    env.setup_egress([egress], batch_size=5000, flush_interval=86400)

    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(rng=np.random.default_rng(42))

    env.process(arrival_generator(env, sampler, packers))

    run_duration_str = "1 week"
    run_duration_sec = time_to_seconds(run_duration_str)

    logger.info(
        f"Generating historical data from {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("Fast-forwarding (factor=0.0)...")

    try:
        env.run(until=run_duration_sec)
    finally:
        env.teardown()
        logger.info(f"Data generation complete. Check '{base_path}/' for chunks.")


if __name__ == "__main__":
    run()

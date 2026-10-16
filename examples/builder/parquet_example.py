"""
Historical Data Generation Example.

Demonstrates using SimulationContext as a fast-forward data engine.
By setting `factor=0.0`, the SimPy clock detaches from wall-clock time,
executing the same shop logic instantly to generate a week of
historical orders for machine learning models, written to Parquet on disk or S3.
"""

import logging
import os
from datetime import datetime, timedelta

from desflow import ParquetStorageEgress, SimulationContext

# Logging is configured here rather than in a wrapper, because this script is run
# directly. Without it the run produces no output at all.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)


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
        if (
            stream_type == "event"
            and "value" in data
            and isinstance(data["value"], dict)
        ):
            nested_value = data.pop("value")
            data.update(nested_value)

        return f"{base_path}/events.parquet"

    return history_router


# ==========================================
# 1. DUAL-MODE STORAGE CONFIGURATION
# ==========================================
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


router = create_history_router(base_path)

# The week of backdating this example exists to demonstrate. It sits at module scope
# because the builder below needs it, and the builder has to stay at module scope for
# the decorators further down to attach to it. The low-level twin computes the same
# value inside run(), where it has no such constraint.
LOGICAL_START_TIME = datetime.now() - timedelta(days=7)

# ==========================================
# 2. Infrastructure Builder
# ==========================================
app = (
    SimulationContext(
        sim_id="Shop",
        factor=0.0,
        random_seed=42,
        # Without this the example logs that it is backdating a week and then timestamps
        # every record from the current clock, which is the opposite of what it claims.
        logical_start_time=LOGICAL_START_TIME,
    )
    .add_egress(ParquetStorageEgress(path_router=router, filesystem=filesystem))
    .with_batching(batch_size=5000, flush_interval=86400)
    .add_resource("packers", current_cap=4, max_cap=10)
    .add_service("packing", dist="normal", mean=2.0, std=0.2)
    .add_arrival("orders", dist="exponential", rate=2.0)
)


# ==========================================
# 3. Simulation Logic
# ==========================================
@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, context):
    """
    Returns the exact flat dictionary expected by the parquet router
    to represent the 'finished' state of the lifecycle.
    """
    return {"path_id": "Shop.service.packing", "status": "finished"}


@app.arrival_loop("orders")
def arrival_generator(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, context, entity=entity))
        task_id += 1


# ==========================================
# 4. Execution
# ==========================================
def run():
    """Generates 1 week of shop orders instantly."""

    logger.info(
        "Generating historical data mimicking start from "
        f"{LOGICAL_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    logger.info("Fast-forwarding (factor=0.0)...")

    # The clock detaches and executes 1 week of operations instantaneously
    app.run(until="1 week")

    logger.info(f"Data generation complete. Check '{base_path}/' for chunks.")


if __name__ == "__main__":
    run()

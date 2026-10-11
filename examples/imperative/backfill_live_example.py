"""
Backfill-then-live Example, imperative API.

The low-level twin of `declarative/backfill_live_example.py`. One run produces both
halves of a tiered dataset. Until `go_live_at` the clock is detached from the wall
clock, so ten minutes of backdated history are written to Parquet as fast as the
machine allows. From `go_live_at` the same run is paced at one simulated second per
real second, and the same events are published to Kafka as they happen.
"""

import logging
import os
import time
from datetime import datetime, timedelta

import numpy as np

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaAdminConnector,
    KafkaEgress,
    ParquetStorageEgress,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENT_TOPIC = "sim-events"
TELEMETRY_TOPIC = "sim-telemetry"

HISTORY = timedelta(minutes=float(os.getenv("HISTORY_MINUTES", "10")))
LIVE_SECONDS = float(os.getenv("LIVE_SECONDS", "60"))
base_path = os.getenv("DEST_PATH", "data/backfill")

GO_LIVE_AT = datetime.now()
LOGICAL_START_TIME = GO_LIVE_AT - HISTORY
GO_LIVE_ISO = GO_LIVE_AT.isoformat(timespec="milliseconds")

PARAMS = SimParameter(
    sim_id="Line_A",
    arrival={"standard": DistributionConfig(dist="exponential", rate=0.5)},
    service={"milling": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
    resources={"lathe": CapacityConfig(current_cap=4, max_cap=10)},
)


def is_history(record: dict) -> bool:
    """True for records stamped before the go-live instant."""
    return record["timestamp"] < GO_LIVE_ISO


def is_live(record: dict) -> bool:
    """True for records stamped at or after the go-live instant."""
    return record["timestamp"] >= GO_LIVE_ISO


def history_router(data: dict) -> str | None:
    """Drops telemetry and flattens the event payload, as Parquet needs flat rows."""
    if data.get("stream_type") != "event":
        return None
    if isinstance(data.get("value"), dict):
        data.update(data.pop("value"))
    return f"{base_path}/events.parquet"


def process_part(env, sampler, lathe, task_id):
    """What @app.task does: queued, started, the service time, then finished."""
    key, path_id = f"task-{task_id}", "Line_A.service.milling"
    env.publish_event(key, {"path_id": path_id, "status": "queued"})
    with lathe.request() as request:
        yield request
        env.publish_event(key, {"path_id": path_id, "status": "started"})
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"path_id": path_id, "status": "finished"})


def arrival_generator(env, sampler, lathe):
    gap = env.registry.get_config("Line_A.arrival.standard")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(process_part(env, sampler, lathe, task_id))
        task_id += 1


def telemetry_generator(env, lathe):
    """Samples resource use every 30 simulation seconds."""
    while True:
        env.publish_telemetry("Line_A.lathe.in_use", lathe.in_use)
        env.publish_telemetry("Line_A.lathe.queue_length", len(lathe.queue.items))
        yield env.timeout(30.0)


def run():
    """Generates the history instantly, then tails live for LIVE_SECONDS."""
    os.makedirs(base_path, exist_ok=True)
    try:
        admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
        admin.create_topics(
            topics_config=[
                {"name": EVENT_TOPIC, "partitions": 1},
                {"name": TELEMETRY_TOPIC, "partitions": 1},
            ]
        )
        time.sleep(2)
    except Exception as e:
        logger.warning(f"Could not explicitly create topics: {e}")

    env = DynamicRealtimeEnvironment(
        factor=0.0,  # unpaced, so the history costs no real time
        logical_start_time=LOGICAL_START_TIME,
        go_live_at=GO_LIVE_AT,  # from here, one simulated second per real second
    )
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(
        [
            ParquetStorageEgress(path_router=history_router),
            KafkaEgress(
                event_topic=EVENT_TOPIC,
                telemetry_topic=TELEMETRY_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
            ),
        ],
        batch_size=2000,
        flush_interval=10.0,
        predicates=[is_history, is_live],  # matched to the providers by position
    )
    lathe = DynamicResource(env, "Line_A", "lathe")
    sampler = Sampler(rng=np.random.default_rng(42))
    env.process(arrival_generator(env, sampler, lathe))
    env.process(telemetry_generator(env, lathe))

    logger.info(
        "Backfilling from %s to %s into '%s/', then tailing live to Kafka for %.0fs.",
        LOGICAL_START_TIME.strftime("%Y-%m-%d %H:%M:%S"),
        GO_LIVE_AT.strftime("%Y-%m-%d %H:%M:%S"),
        base_path,
        LIVE_SECONDS,
    )
    try:
        env.run(until=HISTORY.total_seconds() + LIVE_SECONDS)
    finally:
        env.teardown()
    logger.info("Run complete. History is in '%s/', the tail is in Kafka.", base_path)


if __name__ == "__main__":
    run()

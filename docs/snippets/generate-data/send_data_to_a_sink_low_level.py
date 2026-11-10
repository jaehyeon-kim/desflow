"""Send data to a sink: one arrival, one generated payload, low-level API.

The low-level version of `send_data_to_a_sink.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
draw is made with `Sampler`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's. The four sinks are functions, and `run`
takes one of them.

Needs: the kafka and postgres extras, because the file imports every sink, and the
parquet extra for the file sink. The console needs no service. The Kafka sink needs a
broker (odctl up kafka-lite) and the PostgreSQL sink a database (odctl up postgres).
Run it with: python docs/snippets/generate-data/send_data_to_a_sink_low_level.py
SINK sets the sink: console (the default), kafka, postgres or file.
"""

import logging
import os
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    JsonlStorageEgress,
    KafkaEgress,
    PostgresEgress,
    Sampler,
)

UNTIL = 10
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"


# --8<-- [start:sinks]
def console_sink() -> ConsoleEgress:
    """Prints each record."""
    return ConsoleEgress()


def kafka_sink() -> KafkaEgress:
    """Writes the records to the topic `orders`."""
    return KafkaEgress(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        event_topic="orders",
    )


def postgres_sink() -> PostgresEgress:
    """Writes each record as a row of the table `orders`."""
    return PostgresEgress(
        DSN,
        tables={
            "orders": {
                "columns": {"order_id": "INT", "customer_id": "INT", "total": "REAL"},
                "primary_key": "order_id",
            }
        },
    )


def file_sink() -> JsonlStorageEgress:
    """Writes the records to a JSON Lines file."""
    return JsonlStorageEgress(
        default_path=f"{os.environ.get('DEST_PATH', 'data')}/orders.jsonl"
    )


# --8<-- [end:sinks]


START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals on `env`."""
    # --8<-- [start:arrival]
    sampler = Sampler(seed=42)
    customer = sampler.generator("tasks.order.payload.customer_id")
    total = sampler.generator("tasks.order.payload.total")
    orders_gap = DistributionConfig(dist="exponential", rate=1)
    # The random sequence the blueprint's draw uses
    orders_gap.stream = "arrival.orders"
    # --8<-- [end:arrival]

    # --8<-- [start:payload]
    def orders_source():
        number = 0
        while True:
            yield env.timeout(sampler.sample(orders_gap))
            number += 1
            payload = {
                "order_id": number,
                "customer_id": int(customer.integers(1, 100 + 1)),
                "total": round(float(total.uniform(5, 200)), 2),
            }
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=number - 1
            )

    env.process(orders_source())
    # --8<-- [end:payload]


SINKS = {
    "console": console_sink,
    "kafka": kafka_sink,
    "postgres": postgres_sink,
    "file": file_sink,
}


def run(sink=console_sink) -> None:
    """Runs the model unpaced into the egress that `sink` makes."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([sink()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run(SINKS[os.environ.get("SINK", "console")])

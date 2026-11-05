"""Send data to a sink: one arrival, one generated payload, builder API.

The builder version of `send_data_to_a_sink.yaml`: an order about every second,
written to the console, and to Kafka, PostgreSQL or a JSON Lines file when `sink` is
another function of the file.

Needs: the kafka and postgres extras, because the file imports every sink, and the
parquet extra for the file sink. The console needs no service. The Kafka sink needs a
broker (odctl up kafka-lite) and the PostgreSQL sink a database (odctl up postgres).
Run it with: python docs/snippets/generate-data/send_data_to_a_sink_builder.py
"""

import logging
import os
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    JsonlStorageEgress,
    KafkaEgress,
    PostgresEgress,
    SimulationContext,
)
from desflow.values import Template

UNTIL = 10
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"


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


def build(sink=console_sink) -> SimulationContext:
    """Builds the model, with the egress that `sink` makes."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(sink())
    app.add_arrival("orders", "exponential", rate=1, spawn="order")
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "customer_id": {"gen": "integer", "bounds": [1, 100]},
            "total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
        },
        name="tasks.order",
    )

    @app.task(name="order")
    def order(task_id):
        """Gives one order record."""
        return order_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

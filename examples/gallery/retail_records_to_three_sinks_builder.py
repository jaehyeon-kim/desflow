"""Retail records go to PostgreSQL, Kafka and Parquet, builder API.

The builder version of `retail_records_to_three_sinks.yaml`: 20 customers and 10
products at the start, an order about every two seconds and an invoice for each order.
The customers and products go to PostgreSQL tables, the orders to a Kafka topic and the
invoices to a Parquet file.

Needs: a broker and a database, with odctl up kafka-lite and odctl up postgres, and the
kafka, postgres, parquet and fake extras. KAFKA_BOOTSTRAP_SERVERS sets the broker
address, and DEST_PATH the folder of the Parquet file (data by default).
Run it with: python examples/gallery/retail_records_to_three_sinks_builder.py
"""

import logging
import os
from datetime import datetime, timezone

from desflow import (
    KafkaEgress,
    ParquetStorageEgress,
    PostgresEgress,
    SimulationContext,
)
from desflow.values import Template

UNTIL = 600
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"


def build() -> SimulationContext:
    """Builds the shop, with a sink for each kind of record."""
    app = SimulationContext(
        sim_id="Retail",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(
        PostgresEgress(
            DSN,
            tables={
                "customers": {
                    "columns": {
                        "customer_id": "INT",
                        "name": "TEXT",
                        "country": "TEXT",
                    },
                    "primary_key": "customer_id",
                }
            },
        ),
        when=lambda record: record.get("task") == "customer",
    )
    app.add_egress(
        PostgresEgress(
            DSN,
            tables={
                "products": {
                    "columns": {"product_id": "INT", "title": "TEXT", "price": "REAL"},
                    "primary_key": "product_id",
                }
            },
        ),
        when=lambda record: record.get("task") == "product",
    )
    app.add_egress(
        KafkaEgress(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            event_topic="retail-orders",
        ),
        when=lambda record: record.get("task") == "order",
    )
    app.add_egress(
        ParquetStorageEgress(
            default_path=f"{os.environ.get('DEST_PATH', 'data')}/invoices.parquet"
        ),
        when=lambda record: record.get("task") == "invoice",
    )
    app.add_initial_arrival("customers", 20, spawn="customer")
    app.add_initial_arrival("products", 10, spawn="product")
    app.add_arrival("orders", "exponential", rate=0.5, spawn="order")
    customer_payload = Template(
        {
            "customer_id": {"gen": "sequence", "start": 1},
            "name": {"gen": "fake", "method": "name"},
            "country": {"gen": "weighted", "options": {"AU": 5, "US": 3, "GB": 2}},
        },
        name="tasks.customer",
    )
    product_payload = Template(
        {
            "product_id": {"gen": "sequence", "start": 1},
            "title": {
                "gen": "one_of",
                "choices": [
                    "Desk lamp",
                    "Notebook",
                    "Water bottle",
                    "Backpack",
                    "Headphones",
                    "Keyboard",
                ],
            },
            "price": {"gen": "uniform", "low": 2, "high": 200, "decimals": 2},
        },
        name="tasks.product",
    )
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"},
            "product_id": {"gen": "var", "name": "product", "path": "product_id"},
            "quantity": {"gen": "var", "name": "quantity"},
            "total": {"gen": "expr", "expr": "round(price * quantity, 2)"},
        },
        name="tasks.order",
        vars={
            "product": {"gen": "lookup", "task": "product"},
            "price": {"gen": "var", "name": "product", "path": "price"},
            "quantity": {"gen": "integer", "bounds": [1, 5]},
        },
    )
    invoice_payload = Template(
        {
            "order_id": {"gen": "upstream", "path": "order_id"},
            "amount": {"gen": "upstream", "path": "total"},
        },
        name="tasks.invoice",
    )

    @app.task(name="customer", history=1_000_000)
    def customer(task_id):
        """Gives one customer."""
        return customer_payload

    @app.task(name="product", history=1_000_000)
    def product(task_id):
        """Gives one product."""
        return product_payload

    @app.task(name="order", next="invoice")
    def order(task_id):
        """Takes one order."""
        return order_payload

    @app.task(name="invoice")
    def invoice(task_id):
        """Bills the order."""
        return invoice_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

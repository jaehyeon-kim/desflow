"""A customer table's changes are written to Kafka, and each delete is a tombstone.

The Builder form of customer_deletes_become_kafka_tombstones.yaml, with the same
records. Every message is keyed by its customer id, and a delete has no value, so with
`envelope=False` the Kafka sink sends it as a tombstone.

Needs: odctl up kafka-lite and the kafka extra
Run it with: python examples/gallery/customer_deletes_become_kafka_tombstones_builder.py
"""

import os
from datetime import datetime

from desflow import NO_RECORD, KafkaEgress, SimulationContext
from desflow.values import Template, generator

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

UNTIL = 120


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Customers",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        # envelope=False sends each event's value alone, and a null value as a tombstone
        .add_egress(
            KafkaEgress(
                bootstrap_servers=BOOTSTRAP_SERVERS, event_topic="customers-cdc"
            ),
            envelope=False,
        )
        .add_initial_arrival("existing", 10, spawn="insert_customer")
        .add_arrival("changes", "exponential", rate=1, spawn="change")
    )

    insert_payload = Template(
        {
            "op": "insert",
            "customer_id": {"gen": "sequence"},
            "plan": {"gen": "weighted", "options": {"free": 60, "pro": 40}},
        },
        name="tasks.insert_customer",
    )
    update_payload = Template(
        {
            "op": "update",
            "customer_id": {
                "gen": "lookup",
                "task": "insert_customer",
                "path": "customer_id",
                "strategy": "last",
            },
            "plan": {"gen": "weighted", "options": {"free": 40, "pro": 60}},
        },
        name="tasks.update_customer",
    )
    # One generator that gives null: the task publishes a record whose value is null
    delete_payload = Template(
        {"gen": "literal", "value": None}, name="tasks.delete_customer"
    )

    @app.task(
        name="change",
        next={
            "weights": {
                "insert_customer": 40,
                "update_customer": 45,
                "delete_customer": 15,
            }
        },
    )
    def change(task_id):
        return NO_RECORD

    # history is the number of rows a blueprint keeps (1_000_000 here, as in the YAML);
    # update_customer looks up the insert rows
    @app.task(name="insert_customer", key_field="customer_id", history=1_000_000)
    def insert_customer(task_id):
        return insert_payload

    @app.task(name="update_customer", key_field="customer_id")
    def update_customer(task_id):
        return update_payload

    @app.task(name="delete_customer", key=generator({"gen": "sequence"}))
    def delete_customer(task_id):
        return delete_payload

    return app


if __name__ == "__main__":
    build().run(until=UNTIL)

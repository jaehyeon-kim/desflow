"""BT-OS1 to BT-OS10: the order-streams JSON producer.

Builder form of `model.yaml`.
"""

from datetime import datetime, timezone
from pathlib import Path

from desflow import KafkaEgress, SimulationContext
from desflow.values import Template

ADJECTIVES = ["Small", "Ergonomic", "Rustic", "Intelligent", "Gorgeous", "Incredible", "Fantastic", "Practical", "Sleek", "Awesome", "Enormous", "Mediocre", "Synergistic", "Heavy Duty", "Lightweight", "Aerodynamic", "Durable"]  # fmt: skip
MATERIALS = ["Steel", "Wooden", "Concrete", "Plastic", "Cotton", "Granite", "Rubber", "Leather", "Silk", "Wool", "Linen", "Marble", "Iron", "Bronze", "Copper", "Aluminum", "Paper"]  # fmt: skip
PRODUCTS = ["Chair", "Car", "Computer", "Gloves", "Pants", "Shirt", "Table", "Shoes", "Hat", "Plate", "Knife", "Bottle", "Coat", "Lamp", "Keyboard", "Bag", "Bench", "Clock", "Watch", "Wallet"]  # fmt: skip
DELAY_SECONDS = 5
PAYLOAD = {
    "order_id": {"gen": "uuid"},
    "bid_time": {
        "gen": "format_time",
        "format": "%Y-%m-%d %H:%M:%S",
        "time": {
            "gen": "now",
            "offset": {
                "gen": "expr",
                "expr": "-d",
                "names": {
                    "d": {
                        "gen": "uniform",
                        "low": 0,
                        "high": {
                            "gen": "param",
                            "path": "Orders.variables.delay_seconds",
                        },
                    }
                },
            },
        },
    },
    "price": {"gen": "uniform", "low": 1, "high": 150, "decimals": 2},
    "item": {
        "gen": "format",
        "template": "{a} {m} {p}",
        "names": {
            "a": {"gen": "one_of", "choices": ADJECTIVES},
            "m": {"gen": "one_of", "choices": MATERIALS},
            "p": {"gen": "one_of", "choices": PRODUCTS},
        },
    },
    "supplier": {
        "gen": "weighted",
        "options": {
            "Alice": 3,
            "Alex": 3,
            "Bob": 6,
            "Carol": 6,
            "Joe": 3,
            "James": 1,
            "Jane": 1,
            "Jack": 1,
        },
    },
}


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="Orders",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_egress(
        KafkaEgress(
            bootstrap_servers="localhost:9092",
            event_topic="orders-json",
            topic_policy={"partitions": 3},
        ),
        envelope=False,
    )
    app.add_egress(
        KafkaEgress(
            bootstrap_servers="localhost:9092",
            event_topic="orders-avro",
            topic_policy={"partitions": 3},
            serializer={
                "type": "confluent_avro",
                "registry_url": "http://localhost:8081",
                "schema_file": str(Path(__file__).parent / "order.avsc"),
                "auth": ["admin", "admin"],
            },
        ),
        envelope=False,
    )
    app.add_variable("delay_seconds", DELAY_SECONDS)
    app.add_arrival("orders", "constant", value=1, spawn="order")
    order = Template(PAYLOAD, name="tasks.order")

    @app.task(name="order", key_field="order_id")
    def order_task(task_id):
        return order

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=2000)

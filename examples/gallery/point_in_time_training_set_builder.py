"""A point-in-time training set: feature records and the label that follows each,
builder API.

The builder version of `point_in_time_training_set.yaml`: five customers, each with a
feature record every day and, 7 days after each, a label record.

Needs: nothing
Run it with: python examples/gallery/point_in_time_training_set_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 40 * 86400
CUSTOMERS = 5


def build() -> SimulationContext:
    """Builds the customers. A blueprint's `count: 5` is five arrivals here, each with
    the key of its customer."""
    app = SimulationContext(
        sim_id="Churn",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    for number in range(1, CUSTOMERS + 1):
        app.add_arrival(
            f"customers{number}",
            "constant",
            value=86400,  # 1 day
            spawn="features",
            key=lambda n, number=number: f"customer-{number}",
        )
    features_payload = Template(
        {
            "customer_id": {"gen": "entity"},
            "as_of": {"gen": "now"},
            "sessions_30d": {"gen": "integer", "bounds": [0, 30]},
            "spend_30d": {"gen": "uniform", "low": 0, "high": 500, "decimals": 2},
        },
        name="tasks.features",
    )
    label_payload = Template(
        {
            "customer_id": {"gen": "entity"},
            "as_of": {"gen": "upstream", "path": "as_of"},
            "churned": {
                "gen": "when",
                "condition": "upstream.sessions_30d < 5",
                "then": {"gen": "boolean", "p": 0.6},
                "else": {"gen": "boolean", "p": 0.05},
            },
        },
        name="tasks.label",
    )

    @app.task(name="features", next="label")
    def features(task_id):
        """Gives the features of a customer."""
        return features_payload

    @app.task(name="label", wait={"timeout": "7 days"})
    def label(task_id):
        """Says, 7 days later, whether the customer left."""
        return label_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

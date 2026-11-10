"""Realistic fields: names, emails and places from Faker, and products from a CSV file.

The builder version of `realistic_fields.yaml`: a customer signs up about every 2
seconds, with fields from Faker and a product from a CSV file.

Needs: the fake extra.
Run it with: python docs/snippets/generate-data/realistic_fields_builder.py
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

CSV = str(Path(__file__).with_name("realistic_fields_products.csv"))
UNTIL = 20


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    # --8<-- [start:arrival]
    app.add_arrival("signups", "exponential", rate=0.5, spawn="signup")
    # --8<-- [end:arrival]
    # --8<-- [start:payload]
    signup_payload = Template(
        {
            "customer_id": {"gen": "sequence", "start": 1},
            "name": {"gen": "fake", "method": "name"},
            "email": {"gen": "fake", "method": "email"},
            "city": {"gen": "fake", "method": "city"},
            "german_city": {"gen": "fake", "method": "city", "locale": "de_DE"},
            "job": {"gen": "fake", "method": "job"},
            "favourite": {"gen": "row", "file": CSV, "weight": "popularity"},
        },
        name="tasks.signup",
    )

    @app.task(name="signup")
    def signup(task_id):
        """Gives one signup record."""
        return signup_payload

    # --8<-- [end:payload]

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

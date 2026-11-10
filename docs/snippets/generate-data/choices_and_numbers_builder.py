"""Choices and numbers: weighted choices and drawn numbers, builder API.

The builder version of `choices_and_numbers.yaml`: a payment every second, 60 percent by
card, and purchases with drawn numbers.

Needs: nothing.
Run it with: python docs/snippets/generate-data/choices_and_numbers_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 30


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
    app.add_arrival("payments", "constant", value=1, spawn="payment")
    app.add_arrival("purchases", "exponential", rate=1, spawn="purchase")
    # --8<-- [end:arrival]
    # --8<-- [start:payload]
    payment_payload = Template(
        {"method": {"gen": "weighted", "options": {"card": 60, "cash": 40}}},
        name="tasks.payment",
    )

    @app.task(name="payment")
    def payment(task_id):
        """Gives one payment record."""
        return payment_payload

    purchase_payload = Template(
        {
            "price": {"gen": "uniform", "low": 1, "high": 250, "decimals": 2},
            "items": {"gen": "integer", "bounds": [1, 5]},
            "delivery_minutes": {
                "gen": "normal",
                "mean": 120,
                "std": 15,
                "decimals": 1,
            },
            "channel": {"gen": "one_of", "choices": ["web", "app", "store"]},
            "gift": {"gen": "boolean", "p": 0.1},
        },
        name="tasks.purchase",
    )

    @app.task(name="purchase")
    def purchase(task_id):
        """Gives one purchase record."""
        return purchase_payload

    # --8<-- [end:payload]

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

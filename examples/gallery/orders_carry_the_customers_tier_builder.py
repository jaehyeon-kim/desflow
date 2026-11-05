"""Each order carries the tier of the customer who placed it.

The Builder form of orders_carry_the_customers_tier.yaml, with the same records.
Customers sign up with a tier, and each order reads one sign-up row, so its customer id
and tier agree. An order placed before any customer has signed up is dropped.

Needs: nothing
Run it with: python examples/gallery/orders_carry_the_customers_tier_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 300


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Shop",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_arrival("signups", "exponential", rate=0.1, spawn="sign_up")
        .add_arrival("orders", "exponential", rate=1, spawn="place_order")
    )

    sign_up_payload = Template(
        {
            "customer_id": {"gen": "sequence"},
            "tier": {
                "gen": "weighted",
                "options": {"basic": 70, "plus": 25, "pro": 5},
            },
        },
        name="tasks.sign_up",
    )
    place_order_payload = Template(
        {
            "order_id": {"gen": "sequence"},
            "customer_id": {"gen": "var", "name": "customer", "path": "customer_id"},
            "tier": {"gen": "var", "name": "customer", "path": "tier"},
            "amount": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
        },
        name="tasks.place_order",
        vars={"customer": {"gen": "lookup", "task": "sign_up"}},
    )

    # history is the number of rows a blueprint keeps (1_000_000 here, as in the YAML);
    # place_order looks up the sign-up rows
    @app.task(name="sign_up", history=1_000_000)
    def sign_up(task_id):
        return sign_up_payload

    @app.task(name="place_order")
    def place_order(task_id):
        return place_order_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

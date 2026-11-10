"""A shopper keeps adding items to a cart, then pays or abandons it.

The Builder form of shoppers_add_items_then_pay_or_abandon.yaml, with the same records.
A shopper adds an item every 5 to 30 seconds, and after each item 60 percent add
another, 25 percent pay and 15 percent leave. Two tills serve the payments, each for
about 25 seconds.

Needs: nothing
Run it with: python examples/gallery/shoppers_add_items_then_pay_or_abandon_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 3600


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
        .add_resource("tills", current_cap=2, max_cap=2)
        .add_service("checkout", dist="normal", mean=25, std=5)
        .add_arrival(
            "shoppers",
            "exponential",
            rate=0.1,
            attributes={"items": 0},
            spawn="add_item",
        )
    )

    item_added_payload = Template(
        {
            "event_type": "item_added",
            "items": {"gen": "expr", "expr": "attr.items + 1"},
        },
        name="tasks.add_item",
    )
    paid_payload = Template(
        {"event_type": "paid", "items": {"gen": "attr", "name": "items"}},
        name="tasks.pay",
    )
    abandoned_payload = Template(
        {"event_type": "abandoned", "items": {"gen": "attr", "name": "items"}},
        name="tasks.abandon",
    )

    @app.task(
        name="add_item",
        service_time=generator({"gen": "uniform", "low": 5, "high": 30}),
        set={"items": generator({"gen": "expr", "expr": "attr.items + 1"})},
        next={"weights": {"add_item": 60, "pay": 25, "abandon": 15}},
    )
    def add_item(task_id):
        return item_added_payload

    @app.task(name="pay", resource_id="tills", service_id="checkout")
    def pay(task_id):
        return paid_payload

    @app.task(name="abandon")
    def abandon(task_id):
        return abandoned_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

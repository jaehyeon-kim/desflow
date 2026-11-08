"""Attr: an attribute of the entity, builder API.

The builder version of `attr.yaml`. The arrival's `attributes` take a typed generator
made with `generator`, which gives it the place `arrivals.orders.attributes.size`.

Run it with: python examples/generators/attr_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 200


def build() -> SimulationContext:
    """Builds the model: an order every 60 seconds, each with a size."""
    app = SimulationContext(
        sim_id="Gen",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival(
        "orders",
        "constant",
        value=60,
        spawn="order",
        attributes={"size": generator({"gen": "integer", "bounds": [1, 50]})},
    )
    payload = Template({"size": {"gen": "attr", "name": "size"}}, name="tasks.order")

    @app.task(name="order")
    def order(task_id):
        """Gives one order."""
        return payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

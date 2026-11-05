"""Many entities, by name: one entity for each key in a list, builder API.

The builder version of `many_entities_named.yaml`: three warehouses, each with its own
name, report the orders waiting every 2 seconds. Each warehouse is its own arrival,
because `add_arrival` has no `keys`.

Needs: nothing.
Run it with: python docs/snippets/generate-data/many_entities_named_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 10


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    # A blueprint's `keys` is one arrival for each name here, each with its own key
    for name in ["north", "south", "east"]:
        app.add_arrival(
            f"warehouses-{name}",
            "constant",
            value=2,
            spawn="report",
            key=lambda n, name=name: name,
        )
    report_payload = Template(
        {
            "warehouse": {"gen": "entity"},
            "orders_waiting": {"gen": "integer", "bounds": [0, 40]},
        },
        name="tasks.report",
    )

    @app.task(name="report")
    def report(task_id):
        """Gives one report record."""
        return report_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

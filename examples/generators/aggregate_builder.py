"""Aggregate: a mean, sum, minimum, maximum or count over the finished records of another
task, builder API.

The builder version of `aggregate.yaml`. A task that something aggregates keeps its
finished rows, so the reading task sets `history`.

Run it with: python examples/generators/aggregate_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 200


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Gen",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("readings", "constant", value=3600, spawn="reading", initial=3)
    app.add_arrival("orders", "constant", value=60, spawn="order")
    reading_payload = Template(
        {"level": {"gen": "integer", "bounds": [10, 20]}}, name="tasks.reading"
    )
    order_payload = Template(
        {
            "average": {
                "gen": "aggregate",
                "task": "reading",
                "path": "level",
                "last": 3,
                "function": "mean",
            }
        },
        name="tasks.order",
    )

    @app.task(name="reading", history=1_000_000)
    def reading(task_id):
        """Gives one reading."""
        return reading_payload

    @app.task(name="order")
    def order(task_id):
        """Gives one order."""
        return order_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

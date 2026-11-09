"""Timestamps and gaps: generated times and random gaps between records, builder API.

The builder version of `timestamps_and_gaps.yaml`: an order on average every 0.4 s with
a random gap, each with generated times.

Needs: nothing.
Run it with: python docs/snippets/generate-data/timestamps_and_gaps_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

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
    # --8<-- [start:arrival]
    app.add_arrival(
        "orders",
        gap=generator({"gen": "normal", "mean": 0.4, "std": 0.03}),
        spawn="order",
    )
    # --8<-- [end:arrival]
    # --8<-- [start:payload]
    order_payload = Template(
        {
            "placed_at": {"gen": "now"},
            "deliver_by": {
                "gen": "now",
                "offset": {"gen": "uniform", "low": 0, "high": 172800},
            },
            "epoch_ms": {"gen": "format_time", "format": "epoch_ms"},
        },
        name="tasks.order",
    )

    @app.task(name="order")
    def order(task_id):
        """Gives one order record."""
        return order_payload

    # --8<-- [end:payload]

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

"""Call: what a Python function returns, builder API.

The builder version of `call.yaml`. The function is passed as it is, where a blueprint
names it with `!python`.

Run it with: python examples/generators/call_builder.py
"""

import logging
from datetime import datetime, timezone

from call_helpers import with_tax

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
    app.add_arrival("orders", "constant", value=60, spawn="order")
    payload = Template(
        {
            "total": {
                "gen": "call",
                "function": with_tax,
                "kwargs": {"price": {"gen": "integer", "bounds": [10, 50]}},
            }
        },
        name="tasks.order",
    )

    @app.task(name="order")
    def order(task_id):
        """Gives one order."""
        return payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

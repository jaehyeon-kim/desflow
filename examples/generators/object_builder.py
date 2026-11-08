"""Object: a mapping that a modifier can apply to as a whole, builder API.

The builder version of `object.yaml`: the same task, written with `Template`, which
gives the payload's generators their place and so their random streams.

Run it with: python examples/generators/object_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 200


def build() -> SimulationContext:
    """Builds the model: an order every 60 seconds."""
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
            "address": {
                "gen": "object",
                "null_rate": 0.5,
                "fields": {
                    "city": {"gen": "one_of", "choices": ["Perth", "Hobart"]},
                    "postcode": {"gen": "integer", "bounds": [1000, 9999]},
                },
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

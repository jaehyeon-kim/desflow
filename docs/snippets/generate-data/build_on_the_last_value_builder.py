"""Build on the last value: prices that drift from the previous price, builder API.

The builder version of `build_on_the_last_value.yaml`: five products, each with the key
of its product, are priced once a second for 30 seconds. `previous` reads the last value
of the field that the same product published.

Needs: nothing.
Run it with: python docs/snippets/generate-data/build_on_the_last_value_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 30


def build() -> SimulationContext:
    """Builds the products."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    # A blueprint's `count: 5` is five arrivals here, each with the key of its product
    for number in range(5):
        app.add_arrival(
            f"prices{number}",
            "constant",
            value=1,
            spawn="price",
            key=lambda n, number=number: f"product-{number}",
        )
    price_payload = Template(
        {
            "product": {"gen": "entity"},
            "price": {
                "gen": "add",
                "args": [
                    {"gen": "previous", "path": "price", "default": 20},
                    {"gen": "uniform", "low": -1, "high": 1},
                ],
                "decimals": 2,
            },
            "stock": {
                "gen": "max",
                "args": [
                    0,
                    {
                        "gen": "min",
                        "args": [
                            500,
                            {
                                "gen": "add",
                                "args": [
                                    {
                                        "gen": "previous",
                                        "path": "stock",
                                        "default": 250,
                                    },
                                    {"gen": "normal", "mean": 0, "std": 40},
                                ],
                            },
                        ],
                    },
                ],
                "decimals": 0,
            },
        },
        name="tasks.price",
    )

    @app.task(name="price")
    def price(task_id):
        """Gives one price record."""
        return price_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

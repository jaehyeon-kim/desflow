"""Stock queries read the latest level of each product.

The Builder form of stock_queries_read_the_latest_level.yaml, with the same records.
A blueprint's `count: 5` is five arrivals here, one for each product, each with the key
of its product. Queries look up the latest update of one product.

Needs: nothing
Run it with: python examples/gallery/stock_queries_read_the_latest_level_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 200


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Warehouse",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1),
    ).add_egress(ConsoleEgress())

    for number in range(1, 6):
        app.add_arrival(
            f"updates{number}",
            "constant",
            value=5,
            first_at=0,
            spawn="update_stock",
            key=lambda n, number=number: f"sku-{number}",
        )
    app.add_arrival("queries", "exponential", rate=0.5, spawn="read_stock")

    update_payload = Template(
        {
            "sku": {"gen": "entity"},
            "level": {
                "gen": "max",
                "args": [
                    0,
                    {
                        "gen": "add",
                        "args": [
                            {"gen": "previous", "path": "level", "default": 100},
                            {"gen": "integer", "bounds": [-12, 10]},
                        ],
                    },
                ],
            },
        },
        name="tasks.update_stock",
    )
    read_payload = Template(
        {
            "sku": {"gen": "var", "name": "row", "path": "sku"},
            "level": {"gen": "var", "name": "row", "path": "level"},
        },
        name="tasks.read_stock",
        vars={"row": {"gen": "lookup", "task": "update_stock", "latest_by": "sku"}},
    )

    # history is the number of rows a blueprint keeps (1_000_000 here, as in the YAML);
    # read_stock looks up the update rows
    @app.task(name="update_stock", history=1_000_000)
    def update_stock(task_id):
        return update_payload

    @app.task(name="read_stock")
    def read_stock(task_id):
        return read_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

"""Change the schema during a run, builder API.

The builder version of `change_the_schema_during_a_run.yaml`: an order every 10 seconds
for 4 minutes. A field is added at 60 s, renamed at 120 s and retyped at 180 s, and a
scenario step at each change writes a label record to the labels sink.

Needs: nothing.
Run it with: python docs/snippets/generate-data/change_the_schema_during_a_run_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 240


def build() -> SimulationContext:
    """Builds the shop."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    # --8<-- [start:egress]
    app.add_egress(ConsoleEgress())
    app.add_egress(ConsoleEgress(), when="labels")
    # --8<-- [end:egress]
    # --8<-- [start:arrival]
    app.add_arrival("orders", "constant", value=10, spawn="order")
    # --8<-- [end:arrival]
    # --8<-- [start:scenario]
    app.add_scenario(
        [
            {"at": 60, "record": {"change": "field_added", "field": "currency"}},
            {
                "at": 120,
                "record": {
                    "change": "field_renamed",
                    "field": "total",
                    "to": "amount",
                },
            },
            {
                "at": 180,
                "record": {
                    "change": "field_retyped",
                    "field": "amount",
                    "to": "text",
                },
            },
        ]
    )
    # --8<-- [end:scenario]
    # --8<-- [start:payload]
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "currency": {"gen": "literal", "value": "EUR", "omit_when": "t < 60"},
            "total": {"gen": "var", "name": "value", "omit_when": "t >= 120"},
            "amount": {
                "gen": "when",
                "condition": "t >= 180",
                "then": {"gen": "format", "template": "{value}"},
                "else": {"gen": "var", "name": "value"},
                "omit_when": "t < 120",
            },
        },
        name="tasks.order",
        vars={"value": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2}},
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

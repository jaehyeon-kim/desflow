"""A stolen card makes three large payments seconds after a normal one.

The Builder form of stolen_card_makes_three_large_payments.yaml, with the same records.
Payments arrive from twenty cards, and 3 percent of them come from a card that has just
been stolen. Two seconds later the same card makes three large payments.

Needs: nothing
Run it with: python examples/gallery/stolen_card_makes_three_large_payments_builder.py
"""

import logging
from datetime import datetime

from desflow import NO_RECORD, ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 600


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Bank",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_arrival(
            "payments",
            "exponential",
            rate=1,
            vars={"card": generator({"gen": "integer", "bounds": [1, 20]})},
            key=generator({"gen": "format", "template": "card-{card}"}),
            spawn="payment",
        )
    )

    payment_payload = Template(
        {
            "card": {"gen": "expr", "expr": "process.key"},
            "amount": {"gen": "uniform", "low": 5, "high": 120, "decimals": 2},
            "stolen": {"gen": "boolean", "p": 0.03},
        },
        name="tasks.payment",
    )
    large_payment_payload = Template(
        {
            "card": {"gen": "expr", "expr": "arg.card"},
            "amount": {"gen": "uniform", "low": 500, "high": 2000, "decimals": 2},
            "fraud": True,
        },
        name="tasks.large_payment",
    )

    @app.task(
        name="payment",
        next={"when": [{"condition": "payload.stolen", "task": "burst"}]},
    )
    def payment(task_id):
        return payment_payload

    @app.task(
        name="burst",
        spawn={
            "task": "large_payment",
            "count": 3,
            "after": 2,
            "args": {"card": generator({"gen": "upstream", "path": "card"})},
        },
    )
    def burst(task_id):
        return NO_RECORD

    @app.task(name="large_payment", key=generator({"gen": "expr", "expr": "arg.card"}))
    def large_payment(task_id):
        return large_payment_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

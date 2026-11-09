"""Look up other records: an order names a customer, a payment reads its order, and a
report summarises the recent orders, builder API.

The builder version of `look_up_other_records.yaml`: customers are written at the start
and about every 10 seconds. `lookup` gives each order the id of a customer already
written. `next` routes each order to a payment, and `upstream` gives the payment the
order's id and total. A report every 10 seconds uses `aggregate` to give the mean and
the largest total of the last 10 orders.

Needs: nothing.
Run it with: python docs/snippets/generate-data/look_up_other_records_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 60


def build() -> SimulationContext:
    """Builds the shop."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    # --8<-- [start:arrival]
    app.add_arrival("customers", "exponential", rate=0.1, spawn="customer", initial=5)
    app.add_arrival("orders", "exponential", rate=1, spawn="order")
    app.add_arrival("reports", "constant", value=10, spawn="report")
    # --8<-- [end:arrival]
    # --8<-- [start:payload]
    customer_payload = Template(
        {
            "customer_id": {"gen": "sequence", "start": 1},
            "country": {"gen": "one_of", "choices": ["AU", "NZ", "SG"]},
        },
        name="tasks.customer",
    )
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"},
            "total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
        },
        name="tasks.order",
    )
    payment_payload = Template(
        {
            "order_id": {"gen": "upstream", "path": "order_id"},
            "amount": {"gen": "upstream", "path": "total"},
        },
        name="tasks.payment",
    )
    report_payload = Template(
        {
            "mean_total": {
                "gen": "aggregate",
                "task": "order",
                "path": "total",
                "last": 10,
                "function": "mean",
            },
            "largest_total": {
                "gen": "aggregate",
                "task": "order",
                "path": "total",
                "last": 10,
                "function": "max",
            },
        },
        name="tasks.report",
    )

    # A task that something looks up or aggregates keeps its rows: `history` is how
    # many. A builder task needs it because a `Template` payload is not searched for
    # them.
    @app.task(name="customer", history=1_000_000)
    def customer(task_id):
        """Gives one customer record."""
        return customer_payload

    @app.task(name="order", next="payment", history=10)
    def order(task_id):
        """Gives one order record."""
        return order_payload

    @app.task(name="payment")
    def payment(task_id):
        """Gives one payment record."""
        return payment_payload

    @app.task(name="report")
    def report(task_id):
        """Gives one report record."""
        return report_payload

    # --8<-- [end:payload]

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

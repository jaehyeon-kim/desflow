"""DX-1: An order with 1 to 5 random items and a total.

Builder form of `model.yaml`. The two Postgres sinks of the model only route the records
by `__table__`, so this form leaves them out: the records are the same.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.add_arrival(
        "orders",
        "exponential",
        rate=1.0,
        spawn="order",
        attributes={"order_id": generator({"gen": "sequence", "start": 1})},
    )
    items_payload = Template(
        {
            "__table__": "orders",
            "order_id": {"gen": "attr", "name": "order_id"},
            "customer_id": {"gen": "integer", "bounds": [1, 100]},
            "total_amount": {
                "gen": "expr",
                "expr": "sum([l.price * l.quantity for l in attr.lines])",
                "decimals": 2,
            },
            "status": "pending",
        },
        name="tasks.items",
    )
    item_payload = Template(
        {
            "__table__": "order_items",
            "order_item_id": {"gen": "sequence", "start": 1},
            "order_id": {"gen": "attr", "name": "order_id"},
            "product_id": {"gen": "integer", "bounds": [1, 50]},
            "quantity": {"gen": "expr", "expr": "arg.line.quantity"},
            "unit_price": {"gen": "expr", "expr": "arg.line.price"},
        },
        name="tasks.item",
    )

    @app.task(
        name="order",
        set={
            "lines": generator(
                {
                    "gen": "repeat",
                    "times": {"gen": "integer", "bounds": [1, 5]},
                    "value": {
                        "price": {
                            "gen": "uniform",
                            "low": 10.0,
                            "high": 50.0,
                            "decimals": 2,
                        },
                        "quantity": {"gen": "integer", "bounds": [1, 3]},
                    },
                }
            )
        },
        next="items",
    )
    def order(task_id):
        return {"event_type": "drawn"}

    @app.task(
        name="items",
        wait={
            "each": "attr.lines",
            "branch": {
                "task": "item",
                "args": {"line": generator({"gen": "expr", "expr": "each.value"})},
            },
        },
    )
    def items(task_id):
        return items_payload

    @app.task(name="item")
    def item(task_id):
        return item_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=6)

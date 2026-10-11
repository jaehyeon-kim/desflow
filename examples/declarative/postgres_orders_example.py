"""
Orders with line items, declarative API.

The builder twin of `yaml/advanced/postgres_orders.yaml`. Each customer_order arrival
builds an order with 1 to 5 items, random prices and quantities, and a total computed
from them. Two PostgresEgress instances are attached, one per table, and each keeps
only the records whose `__table__` key names its table. Each creates its table at
start.

Needs a database: odctl up postgres. Runs until interrupted with Ctrl + C.
"""

import logging

from dynamic_des import PostgresEgress, PostgresIngress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

DSN = "postgresql://user:password@localhost:5432/odctl"

ORDERS = {
    "columns": {
        "order_id": "INT",
        "customer_id": "INT",
        "total_amount": "REAL",
        "status": "TEXT",
        "timestamp": "TIMESTAMP",
    },
    "primary_key": "order_id",
}
ORDER_ITEMS = {
    "columns": {
        "order_item_id": "INT",
        "order_id": "INT",
        "product_id": "INT",
        "quantity": "INT",
        "unit_price": "REAL",
    },
    "primary_key": "order_item_id",
}


def order_generator(context: SimulationContext, arrival: str, max_items: int):
    """Publishes an order and its items on every arrival.

    Draws from `context.sampler.rng`, the generator seeded by `random_seed`, so a
    seeded run repeats the same orders. The `__table__` key names the table each
    record is written to.
    """
    rng = context.sampler.rng
    order_id = 1
    item_id = 1
    while True:
        yield context.wait_for_arrival(arrival)

        total = 0.0
        for _ in range(int(rng.integers(1, max_items + 1))):
            price = round(float(rng.uniform(10.0, 50.0)), 2)
            quantity = int(rng.integers(1, 4))
            total += price * quantity
            context.env.publish_event(
                f"order-{order_id}",
                {
                    "__table__": "order_items",
                    "order_item_id": item_id,
                    "order_id": order_id,
                    "product_id": int(rng.integers(1, 51)),
                    "quantity": quantity,
                    "unit_price": price,
                },
            )
            item_id += 1

        context.env.publish_event(
            f"order-{order_id}",
            {
                "__table__": "orders",
                "order_id": order_id,
                "customer_id": int(rng.integers(1, 101)),
                "total_amount": round(total, 2),
                "status": "pending",
            },
        )
        order_id += 1


app = (
    SimulationContext(sim_id="Store", factor=1.0, random_seed=42)
    .add_ingress(PostgresIngress(DSN, table_name="simulation_params"))
    .add_egress(PostgresEgress(DSN, tables={"orders": ORDERS}))
    .add_egress(PostgresEgress(DSN, tables={"order_items": ORDER_ITEMS}))
    .add_arrival("customer_order", dist="exponential", rate=1.0)
    .add_process(order_generator, arrival="customer_order", max_items=5)
)

if __name__ == "__main__":
    app.run()

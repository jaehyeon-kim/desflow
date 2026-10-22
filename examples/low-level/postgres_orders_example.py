"""
Orders with line items, low-level API.

The low-level twin of `builder/postgres_orders_example.py` and
`yaml/advanced/postgres_orders.yaml`. It wires `DynamicRealtimeEnvironment`, the
registry and the connectors by hand. With the same seed it publishes the same orders
as the other two.

Needs a database: odctl up postgres. Runs until interrupted with Ctrl + C.
"""

import logging

import numpy as np

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    PostgresEgress,
    PostgresIngress,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

DSN = "postgresql://user:password@localhost:5432/odctl"

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
)

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


def order_generator(env, sampler: Sampler, max_items: int):
    """Publishes an order and its items on every arrival.

    The gaps come from the arrival's own random sequence and the order contents from
    `sampler.rng`, as in the builder version, so the two publish the same records.
    """
    rng = sampler.rng
    arrival = env.registry.get_config("Shop.arrival.orders")
    order_id = 1
    item_id = 1
    while True:
        yield env.timeout(sampler.sample(arrival))

        total = 0.0
        for _ in range(int(rng.integers(1, max_items + 1))):
            price = round(float(rng.uniform(10.0, 50.0)), 2)
            quantity = int(rng.integers(1, 4))
            total += price * quantity
            env.publish_event(
                f"orders-{order_id}",
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

        env.publish_event(
            f"orders-{order_id}",
            {
                "__table__": "orders",
                "order_id": order_id,
                "customer_id": int(rng.integers(1, 101)),
                "total_amount": round(total, 2),
                "status": "pending",
            },
        )
        order_id += 1


def run(until=None, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor, strict=False)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_ingress([PostgresIngress(DSN, table_name="simulation_params")])
    env.setup_egress(
        [
            PostgresEgress(DSN, tables={"orders": ORDERS}),
            PostgresEgress(DSN, tables={"order_items": ORDER_ITEMS}),
        ]
    )
    sampler = Sampler(rng=np.random.default_rng(42))
    env.process(order_generator(env, sampler, max_items=5))
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()

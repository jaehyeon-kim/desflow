"""Relational output, low-level API.

The low-level version of `builder/postgres_example.py`. The simulation writes order
records to the PostgreSQL table `orders`. `PostgresEgress` creates the table at start,
and `PostgresIngress` creates `simulation_params` and polls it for parameter updates.

Needs a database: `odctl up postgres`. Runs until interrupted with Ctrl + C.
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

logger = logging.getLogger("postgres_example")

# Connection string matching the odctl `postgres` profile
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"

# The table PostgresEgress creates when it does not exist. timestamp comes from the
# record, and the egress converts its ISO string for the TIMESTAMP column.
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

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
)


def order_generator(env, sampler):
    """Publishes one order per arrival, as the builder's task with no service does."""
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))

        # --8<-- [start:records]
        order = {
            "order_id": task_id,
            "customer_id": 42,
            "total_amount": 59.97,
            "status": "pending",
        }
        # The order key, and the task name and instance id the builder adds
        env.publish_event(
            f"orders-{task_id + 1}", order, task="place_order", instance=task_id
        )
        # --8<-- [end:records]
        task_id += 1


def run():
    env = DynamicRealtimeEnvironment(factor=1.0, strict=False)
    env.registry.register_sim_parameter(PARAMS)

    # --8<-- [start:connectors]
    # PostgresEgress creates the orders table, and PostgresIngress creates the
    # simulation_params table it polls for parameter updates.
    env.setup_egress([PostgresEgress(DSN, tables={"orders": ORDERS})])
    env.setup_ingress([PostgresIngress(DSN, table_name="simulation_params")])
    # --8<-- [end:connectors]

    sampler = Sampler(rng=np.random.default_rng(42))
    env.process(order_generator(env, sampler))

    logger.info("Starting Low-level Postgres Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running: INSERT INTO simulation_params (path, "
        "value) VALUES ('Shop.arrival.orders.rate', '5.0');"
    )

    try:
        env.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()

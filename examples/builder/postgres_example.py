"""Relational output, builder API.

`Shop`, an online shop, writes its orders to the PostgreSQL table `orders`.
`PostgresEgress` creates the table at start and writes one row per order, and
`PostgresIngress` creates `simulation_params` and polls it for parameter updates, so
inserting a row there changes the arrival rate of a running simulation.

Needs a database: `odctl up postgres`. Runs until interrupted with Ctrl + C.
"""

import logging

from desflow import PostgresEgress, PostgresIngress, SimulationContext

logger = logging.getLogger(__name__)

# Connection string matching the odctl `postgres` profile
DSN = "postgresql://user:password@localhost:5432/odctl"

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

# --8<-- [start:app]
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_ingress(PostgresIngress(DSN, table_name="simulation_params"))
    .add_egress(PostgresEgress(DSN, tables={"orders": ORDERS}))
    .add_arrival("orders", dist="exponential", rate=1.0)
)
# --8<-- [end:app]


# --8<-- [start:records]
@app.task()
def place_order(task_id: int, context):
    """A task with no service or resource: its payload is published at once."""
    return {
        "order_id": task_id,
        "customer_id": 42,
        "total_amount": 59.97,
        "status": "pending",
    }


@app.arrival_loop("orders")
def order_generator(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(place_order(task_id, context, entity=entity))
        task_id += 1
        # --8<-- [end:records]


def run():
    logger.info("Starting Builder Postgres Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running: INSERT INTO simulation_params (path, "
        "value) VALUES ('Shop.arrival.orders.rate', '5.0');"
    )
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()

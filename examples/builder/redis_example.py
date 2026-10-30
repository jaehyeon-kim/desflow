"""Redis Streams output with live parameter updates, builder API.

The simulation records each order as it is placed. `RedisEgress` writes the
records to the `order_events` Redis Stream, named by each record's `__stream__` key,
while `RedisIngress` subscribes to the `simulation_params` channel, so publishing a
message to that channel changes the arrival rate of a running simulation.

Needs Valkey: `odctl up valkey`. Runs until interrupted with Ctrl + C.
Set EXAMPLE_UNTIL to a number of simulation seconds to end the run at that time instead.
"""

import logging
import os

from desflow import RedisEgress, RedisIngress, SimulationContext

logger = logging.getLogger(__name__)

# The odctl `valkey` profile disables the unauthenticated default user, so the
# URL carries credentials. A plain Redis without auth takes redis://127.0.0.1:6379/0.
REDIS_URL = "redis://user:password@127.0.0.1:6379/0"

# --8<-- [start:app]
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
    .add_egress(RedisEgress(REDIS_URL, stream_name="events"))
    .add_arrival("orders", dist="exponential", rate=2.0)
)
# --8<-- [end:app]


# --8<-- [start:records]
@app.task()
def record_order(task_id: int, context):
    """A task with no service or resource: its payload is published at once."""
    return {
        "__stream__": "order_events",
        "order_id": task_id,
        "channel": "web",
        "status": "placed",
    }


@app.arrival_loop("orders")
def order_generator(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(record_order(task_id, context, entity=entity))
        task_id += 1
        # --8<-- [end:records]


# The simulation time, in seconds, at which the run ends.
# When EXAMPLE_UNTIL is not set, the run goes on until Ctrl + C.
EXAMPLE_UNTIL = float(os.getenv("EXAMPLE_UNTIL") or 0) or None


def run():
    logger.info("Starting Builder Redis Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running: docker exec -it valkey valkey-cli --user user "
        "--pass password PUBLISH simulation_params "
        '\'{"path": "Shop.arrival.orders.rate", "value": 10.0}\''
    )
    try:
        app.run(until=EXAMPLE_UNTIL)
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()

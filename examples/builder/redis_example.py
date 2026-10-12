"""Redis Streams output with live parameter updates, builder API.

`Factory` writes part records to the `part_events` Redis Stream through `RedisEgress`,
named by each record's `__stream__` key, while `RedisIngress` subscribes to the `simulation_params` channel, so publishing a message to
that channel changes the arrival rate of a running simulation.

Needs Valkey: `odctl up valkey`. Runs until interrupted with Ctrl + C.
"""

import logging
import random
from datetime import datetime

from dynamic_des import RedisEgress, RedisIngress, SimulationContext

logger = logging.getLogger(__name__)

# The odctl `valkey` profile disables the unauthenticated default user, so the
# URL carries credentials. A plain Redis without auth takes redis://localhost:6379/0.
REDIS_URL = "redis://user:password@localhost:6379/0"

# --8<-- [start:app]
app = (
    SimulationContext(sim_id="Factory", factor=1.0)
    .add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
    .add_egress(RedisEgress(REDIS_URL, stream_name="events"))
    .add_arrival("part_arrival", dist="exponential", rate=2.0)
)
# --8<-- [end:app]


@app.arrival_loop("part_arrival")
def part_generator(context):
    part_id = 1
    while True:
        yield context.wait_for_arrival("part_arrival")

        # --8<-- [start:records]
        part_event = {
            "__stream__": "part_events",
            "part_id": part_id,
            "type": random.choice(["A", "B", "C"]),
            "timestamp": datetime.utcnow().isoformat(),
            "status": "arrived",
        }

        context.publish("factory_event", part_event)
        # --8<-- [end:records]
        part_id += 1


def run():
    logger.info("Starting Builder Redis Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running: docker exec -it valkey valkey-cli --user user "
        "--pass password PUBLISH simulation_params "
        '\'{"param_path": "Factory.arrival.part_arrival.rate", "param_value": 10.0}\''
    )
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()

"""Redis Streams output with live parameter updates, low-level API.

The low-level version of `builder/redis_example.py`. The simulation records each
order as it is placed. `RedisEgress` writes the records to the `order_events` stream,
named by each record's `__stream__` key, and `RedisIngress` subscribes to the
`simulation_params` channel.

Needs Valkey: `odctl up valkey`. Runs until interrupted with Ctrl + C.
Set EXAMPLE_UNTIL to a number of simulation seconds to end the run at that time instead.
"""

import logging
import os

import numpy as np

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    RedisEgress,
    RedisIngress,
    Sampler,
    SimParameter,
)

logger = logging.getLogger("redis_example")

# The odctl `valkey` profile disables the unauthenticated default user, so the
# URL carries credentials. A plain Redis without auth takes redis://127.0.0.1:6379/0.
REDIS_URL = "redis://user:password@127.0.0.1:6379/0"

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=2.0)},
)


def order_generator(env, sampler):
    """Publishes one record per order, as the builder's task with no service does."""
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        order_event = {
            "__stream__": "order_events",
            "order_id": task_id,
            "channel": "web",
            "status": "placed",
        }
        # The order key, and the task name and instance id the builder adds
        env.publish_event(
            f"orders-{task_id + 1}", order_event, task="record_order", instance=task_id
        )
        task_id += 1


# The simulation time, in seconds, at which the run ends.
# When EXAMPLE_UNTIL is not set, the run goes on until Ctrl + C.
EXAMPLE_UNTIL = float(os.getenv("EXAMPLE_UNTIL") or 0) or None


def run():
    env = DynamicRealtimeEnvironment(factor=1.0, strict=False)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([RedisEgress(REDIS_URL, stream_name="events")])
    env.setup_ingress([RedisIngress(REDIS_URL, channel_name="simulation_params")])

    sampler = Sampler(rng=np.random.default_rng(42))
    env.process(order_generator(env, sampler))

    logger.info("Starting Low-level Redis Demo. Press Ctrl+C to stop...")
    logger.info(
        "Test Ingress by running: docker exec -it valkey valkey-cli --user user "
        "--pass password PUBLISH simulation_params "
        '\'{"path": "Shop.arrival.orders.rate", "value": 10.0}\''
    )

    try:
        env.run(until=EXAMPLE_UNTIL)
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user.")
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()

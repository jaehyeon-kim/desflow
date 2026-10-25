"""The Patience tutorial: orders leave after waiting too long, and none join near closing.

One packer, an order every 1.43 seconds on average (rate 0.7) and 1.5 seconds of
packing. Every record is printed to the terminal, and the script prints what happened
to the orders with a patience of 30 seconds and with none at the end. To see only those
lines, run:
python patience_low_level.py 2>/dev/null
"""

import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    # An order every 1.43 s on average (0.7 a second): more than one packer packs
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.7)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
)


# --8<-- [start:task]
def pack_order(env, sampler, packers, key, task_id, patience):
    path_id = "Shop.service.packing"

    def publish(status):
        value = {"path_id": path_id, "status": status}
        env.publish_event(key, value, task="pack_order", instance=task_id)

    # --8<-- [start:balk]
    # The courier leaves at the hour, so the last 5 minutes take no new orders
    if env.now >= 3300:
        publish("balked")
        env.stats.task_balked("pack_order", task_id)
        return
    # --8<-- [end:balk]
    publish("queued")
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        # --8<-- [start:patience]
        # A customer cancels an order not started within 30 seconds
        yield request | env.timeout(patience)
        if not request.triggered:
            # Leaving the with block takes the request out of the queue
            publish("reneged")
            env.stats.task_reneged("pack_order", task_id)
            return
        # --8<-- [end:patience]
        publish("started")
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        value = {"event_type": "order_packed"}
        env.publish_event(key, value, task="pack_order", instance=task_id)
    env.stats.task_finished("pack_order", task_id)


# --8<-- [end:task]


def order_generator(env, sampler, packers, patience):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        key = f"orders-{task_id + 1}"
        env.process(pack_order(env, sampler, packers, key, task_id, patience))
        task_id += 1


def run(patience=30, until=3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # --8<-- [start:watch]
    # Counts of finished, reneged and balked orders, and the mean wait, at the end
    env.setup_stats("Shop", "typical", interval=3600)
    # --8<-- [end:watch]
    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=42)
    env.process(order_generator(env, sampler, packers, patience))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def outcomes(patience: float) -> dict:
    """Runs one hour and returns what happened to the orders, and the mean wait."""
    readings = run(patience).stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        outcome: readings.get(name(outcome), 0)
        for outcome in ("finished", "reneged", "balked", "wait_mean")
    }


if __name__ == "__main__":
    for patience, label in ((30, "30 s"), (3600, "none")):
        result = outcomes(patience)
        print(
            f"patience {label:>4}: {result['finished']} packed, "
            f"{result['reneged']} reneged, {result['balked']} balked, "
            f"mean wait {result['wait_mean']:5.2f} s"
        )

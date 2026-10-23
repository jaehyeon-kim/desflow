"""A complete low-level simulation, as the Low-level API page explains it step by step.

One packing team with a stock of boxes and a store of packed orders. Run it with
`python low_level.py`: it runs unpaced for 10 simulation minutes and prints every record
to the terminal.
"""

# --8<-- [start:imports]
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    DynamicStore,
    Sampler,
    SimParameter,
    time_to_seconds,
)
# --8<-- [end:imports]

# --8<-- [start:environment]
env = DynamicRealtimeEnvironment(factor=0.0)  # unpaced; 1.0 follows the wall clock
# --8<-- [end:environment]

# --8<-- [start:parameters]
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop",
        # An order every 2 s on average (rate 0.5)
        arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
        service={"packing": DistributionConfig(dist="normal", mean=1.5, std=0.3)},
        resources={"packers": CapacityConfig(current_cap=1, max_cap=3)},
        containers={"boxes": ContainerConfig(current_cap=100, max_cap=100, level=100)},
        stores={"finished": CapacityConfig(current_cap=50, max_cap=50)},
        variables={"boxes_per_order": 1.0},
    )
)
# --8<-- [end:parameters]

# --8<-- [start:connectors]
env.setup_egress([ConsoleEgress()])  # starts the egress thread
# env.setup_ingress([KafkaIngress(...)]) would start the ingress thread
env.setup_stats("Shop", "basic", interval=60)  # readings and counts every minute
# --8<-- [end:connectors]

# --8<-- [start:resources]
packers = DynamicResource(env, "Shop", "packers")  # reads Shop.resources.packers.*
boxes = DynamicContainer(env, "Shop", "boxes")  # starts at its level, 100
finished = DynamicStore(env, "Shop", "finished")
sampler = Sampler(seed=42)  # a random stream per distribution, from one seed
# --8<-- [end:resources]


# --8<-- [start:order]
def order(env, key, task_id):
    """One order: queue for a packer, take a box, pack, then store the result."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)  # what @app.task reports for you
    with packers.request() as request:
        yield request
        count = env.registry.get("Shop.variables.boxes_per_order").value
        yield boxes.get(count)  # waits if the stock is too low
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        yield finished.put({"order": key})
        publish({"event_type": "order_packed"})
    env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")


# --8<-- [end:order]


# --8<-- [start:arrivals]
def arrivals(env):
    gap = env.registry.get_config("Shop.arrival.orders")  # the live config object
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))  # each draw reads the current rate
        env.process(order(env, f"orders-{task_id + 1}", task_id))  # the entity key
        task_id += 1


# --8<-- [end:arrivals]


# --8<-- [start:telemetry]
def telemetry(env):
    """A reading of your own, beside the built-in statistics."""
    while True:
        env.publish_telemetry("Shop.boxes_per_packer", boxes.level / packers.capacity)
        yield env.timeout(10.0)


# --8<-- [end:telemetry]


# --8<-- [start:control]
def refill_and_rush(env):
    """Refills boxes every minute, and doubles arrivals after five minutes."""
    while True:
        yield env.timeout(60.0)
        missing = boxes.capacity - boxes.level
        if missing > 0:  # SimPy refuses a put of 0
            yield boxes.put(missing)
        if env.now >= 300 and env.registry.get("Shop.arrival.orders.rate").value < 1.0:
            env.registry.update("Shop.arrival.orders.rate", 1.0)
            env.registry.update("Shop.resources.packers.current_cap", 2)


# --8<-- [end:control]


# --8<-- [start:start]
env.process(arrivals(env))
env.process(telemetry(env))
env.process(refill_and_rush(env))
# --8<-- [end:start]

if __name__ == "__main__":
    # --8<-- [start:run]
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    try:
        env.run(until=time_to_seconds("10 min"))  # until is in simulation seconds
    finally:
        env.teardown()  # flushes the buffers and stops the threads
    # --8<-- [end:run]
    assert (
        packers.capacity == 2
        and env.registry.get("Shop.arrival.orders.rate").value == 1.0
    )
    assert len(finished.items) > 0

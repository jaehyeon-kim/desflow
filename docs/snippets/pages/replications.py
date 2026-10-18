"""Replications: one shop run with many seeds, in the builder and the low-level API."""

import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicResource,
    LowLevelModel,
    SimParameter,
    SimulationContext,
    replicate,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
# Each replication would log its build and teardown; the sink still logs
logging.getLogger("desflow.core").setLevel(logging.WARNING)

MEASURES = ["tasks.pack.wait_mean", "resources.packers.utilization_avg"]


def shop() -> SimulationContext:
    """A new shop each time it is called, with a Console sink."""
    app = (
        SimulationContext(sim_id="Shop", factor=0)
        .add_egress(ConsoleEgress())
        .add_resource("packers", current_cap=2, max_cap=4)
        .add_service("packing", dist="gamma", shape=4, scale=1)
        .add_arrival("orders", dist="exponential", rate=0.4)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_loop(context):
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack(task_id, entity=entity))
            task_id += 1

    return app


def builder():
    # --8<-- [start:builder]
    # shop is a function returning a new SimulationContext, called once per run
    result = replicate(shop, 3, seed=42, until="1 h", warmup="3 min", measures=MEASURES)
    # --8<-- [end:builder]
    return result


def pack(env, sampler, packers, task_id):
    config = env.registry.get_config("Shop.service.packing")
    env.stats.task_queued("pack", task_id)
    with packers.request() as request:
        yield request
        env.stats.task_started("pack", task_id)
        yield env.timeout(sampler.sample(config))
    env.stats.task_finished("pack", task_id)


def orders(env, sampler, packers):
    gap = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        env.process(pack(env, sampler, packers, task_id))
        task_id += 1


PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={"packing": DistributionConfig(dist="gamma", shape=4, scale=1)},
    resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
)


# --8<-- [start:low-level]
def model(env, sampler):
    """Builds the shop in the environment it is given, drawing from sampler."""
    env.registry.register_sim_parameter(PARAMS)
    env.process(orders(env, sampler, DynamicResource(env, "Shop", "packers")))


def low_level():
    shop = LowLevelModel(model, sim_id="Shop")
    return replicate(shop, 3, seed=42, until=3600, warmup=180, measures=MEASURES)


# --8<-- [end:low-level]
if __name__ == "__main__":
    result = builder()
    # --8<-- [start:read]
    print(result.summary_text())  # each measure's n, mean and half-width
    print(result.seeds)  # replication i ran with seeds[i - 1]
    for row in result.table:  # one row per replication and measure
        print(row["replication"], row["seed"], row["measure"], row["value"])
    # --8<-- [end:read]
    assert low_level().values == result.values  # the two forms are one model

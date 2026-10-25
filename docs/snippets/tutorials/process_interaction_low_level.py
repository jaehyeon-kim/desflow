"""The Process Interaction tutorial: packed orders wait for the courier, with a limit.

One packer, an order every 2 seconds on average, and a courier every 10 minutes. Every
record is printed to the terminal, and the script prints how many orders went by courier
and by post, with limits of 5 and 8 minutes, at the end. To see only those lines, run:
python process_interaction_low_level.py 2>/dev/null
"""

import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    Signal,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        "orders": DistributionConfig(dist="exponential", rate=0.5),
        # --8<-- [start:courier]
        # A courier every 10 minutes
        "couriers": DistributionConfig(dist="constant", value=600),
        # --8<-- [end:courier]
    },
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
)


def publish(env, key, task, task_id, value):
    env.publish_event(key, value, task=task, instance=task_id)


def pack_order(env, sampler, shop, key, ids):
    task_id, path_id = next(ids["pack_order"]), "Shop.service.packing"
    publish(env, key, "pack_order", task_id, {"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)
    with shop["packers"].request() as request:
        yield request
        started = {"path_id": path_id, "status": "started"}
        publish(env, key, "pack_order", task_id, started)
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish(env, key, "pack_order", task_id, {"event_type": "order_packed"})
        env.stats.task_finished("pack_order", task_id)
    env.process(dispatch(env, shop, key, ids))


# --8<-- [start:dispatch]
def dispatch(env, shop, key, ids):
    task_id = next(ids["dispatch"])
    publish(env, key, "dispatch", task_id, {"status": "queued"})
    env.stats.task_queued("dispatch", task_id)
    # The courier's departure, or the limit, whichever comes first
    courier = shop["courier_left"].wait()
    done = yield env.any_of([courier, env.timeout(shop["limit"])])
    publish(env, key, "dispatch", task_id, {"status": "started"})
    env.stats.task_started("dispatch", task_id)
    publish(env, key, "dispatch", task_id, {"event_type": "dispatched"})
    env.stats.task_finished("dispatch", task_id)
    # The courier came before the limit when its event is in the AnyOf's value
    task = "by_courier" if courier in done else "by_post"
    env.process(send(env, key, task, ids))


# --8<-- [end:dispatch]


def send(env, key, task, ids):
    task_id = next(ids[task])
    publish(env, key, task, task_id, {"event_type": f"sent_{task}"})
    env.stats.task_finished(task, task_id)
    env.stats.flow_finished("orders")
    yield env.timeout(0)


# --8<-- [start:collect]
def collect(env, shop, key, ids):
    """Fires courier_left with the payload, so every waiting order resumes."""
    task_id = next(ids["collect"])
    payload = {"event_type": "courier_left"}
    publish(env, key, "collect", task_id, payload)
    env.stats.task_finished("collect", task_id)
    shop["courier_left"].fire(payload)
    env.stats.flow_finished("couriers")
    yield env.timeout(0)


# --8<-- [end:collect]


def generator(env, sampler, shop, arrival, ids):
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        key = f"{arrival}-{number}"
        if arrival == "orders":
            env.process(pack_order(env, sampler, shop, key, ids))
        else:
            env.process(collect(env, shop, key, ids))


def run(limit=300, until=8 * 3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # --8<-- [start:watch]
    # How many orders went each way, and the mean wait for dispatch, at the end
    env.setup_stats("Shop", "typical", interval=8 * 3600)
    # --8<-- [end:watch]
    shop = {
        "packers": DynamicResource(env, "Shop", "packers"),
        # --8<-- [start:signal]
        # Fired by the courier as it leaves with every order that waits for it
        "courier_left": Signal(env, "courier_left"),
        # --8<-- [end:signal]
        "limit": limit,
    }
    names = ("pack_order", "dispatch", "by_courier", "by_post", "collect")
    ids = {name: itertools.count() for name in names}
    sampler = Sampler(seed=42)
    env.process(generator(env, sampler, shop, "orders", ids))
    env.process(generator(env, sampler, shop, "couriers", ids))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def outcomes(limit: float) -> dict:
    """Runs eight hours and returns how orders went, and the mean wait for dispatch."""
    readings = run(limit).stats.readings()
    name = "Shop.stats.tasks.{}".format
    return {
        "by_courier": readings[name("by_courier.finished")],
        "by_post": readings[name("by_post.finished")],
        "wait_mean": readings[name("dispatch.wait_mean")],
    }


if __name__ == "__main__":
    for limit in (300, 480):
        result = outcomes(limit)
        print(
            f"limit {limit / 60:.0f} min: {result['by_courier']} by courier, "
            f"{result['by_post']} by post, mean wait {result['wait_mean']:6.1f} s"
        )

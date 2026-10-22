"""Live parameters: an arrival rate, a service time and a capacity changed on the
way, and a run that ends by its end conditions, low-level API.

The low-level twin of `builder/live_parameters_example.py`, a regression scenario
(see scripts/regression.py), not an example. A process updates registry paths at
fixed times, as an ingress connector would. The 500 boxes are taken at about 740
seconds, which ends the run by its stop condition; 900 seconds and a drain of at most
120 seconds are the limits if they were not.
"""

import itertools

import simpy

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="normal", mean=3, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=2, max_cap=6)},
    containers={"boxes": ContainerConfig(current_cap=500, max_cap=500, level=500)},
)

# Each change: the simulation time, the registry path and the new value. No time is a
# multiple of the 30 s statistics interval, so no change ties with a reading.
CHANGES = [
    (145, "Shop.arrival.orders.rate", 0.9),
    (145, "Shop.resources.packers.current_cap", 4),
    (305, "Shop.service.packing.mean", 4.5),
    (455, "Shop.arrival.orders.rate", 0.7),
    (455, "Shop.resources.packers.current_cap", 2),
    (605, "Shop.service.packing.mean", 3.5),
    (605, "Shop.service.packing.std", 0.8),
]


def pack(env, sampler, shop, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack", instance=task_id)

    env.stats.flow_started("orders")
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack", task_id)
    with shop["packers"].request() as req:
        yield req
        yield shop["boxes"].get(1)
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack", task_id)
    env.stats.flow_finished("orders")


def order_generator(env, sampler, shop):
    gap = env.registry.get_config("Shop.arrival.orders")
    try:
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            process = env.process(
                pack(env, sampler, shop, f"orders-{number}", number - 1)
            )
            shop["orders"].append(process)
    except simpy.Interrupt:
        pass  # closing time: no new orders


def steer(env):
    """Applies each change at its time, as an ingress connector would."""
    for at, path, value in CHANGES:
        yield env.timeout(at - env.now)
        env.registry.update(path, value)


def build(env):
    return {
        "packers": DynamicResource(env, "Shop", "packers"),
        "boxes": DynamicContainer(env, "Shop", "boxes"),
        "orders": [],
    }


def processes(env, sampler, shop):
    shop["source"] = order_generator(env, sampler, shop)
    return [shop["source"], steer(env)]


def run_to_end(env, shop, source):
    """Runs to 900 s, then drains for at most 120 s, or until no boxes are left."""
    out_of_boxes = env.event()

    def check():
        if shop["boxes"].level == 0 and not out_of_boxes.triggered:
            out_of_boxes.succeed()

    env.watch_changes(check)
    env.run(until=env.timeout(900) | out_of_boxes)
    if out_of_boxes.triggered:
        return
    source.interrupt()  # no new orders after closing time
    packed = env.all_of([p for p in shop["orders"] if p.is_alive])
    env.run(until=packed | env.timeout(120) | out_of_boxes)


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    env.setup_stats("Shop", "basic", interval=30.0)
    sampler = Sampler(seed=42)
    shop = build(env)
    started = [env.process(process) for process in processes(env, sampler, shop)]
    try:
        run_to_end(env, shop, started[0])
    finally:
        env.teardown()


if __name__ == "__main__":
    run()

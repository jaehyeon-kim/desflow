"""The tasks and processes of the Tasks and Processes page: builder and low-level.

tests/docs runs this file: both versions run unpaced for 3 simulation minutes.
"""

import itertools

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    SimulationContext,
)

app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=1)
    .add_resource("packers", current_cap=1, max_cap=3)
    .add_service("packing", dist="normal", mean=4.0, std=1.0)
    .add_service("labelling", dist="constant", value=2.0)
    .add_egress(ConsoleEgress())
)


# --8<-- [start:builder-task]
@app.task(service_id="packing", resource_id="packers", next="print_label")
def pack_order(task_id: int):
    # Runs when the order gets a packer, before the packing time.
    # The returned dict is the finished event's value.
    return {"event_type": "order_packed"}


@app.task()  # no service and no resource: publishes at once
def return_logged(task_id: int):
    return {"event_type": "return_logged"}


# --8<-- [end:builder-task]


# --8<-- [start:builder-timed-step]
@app.task(service_id="labelling")  # a service and no resource: starts at once
def print_label(task_id: int):
    return {"event_type": "label_printed"}


# --8<-- [end:builder-timed-step]


# --8<-- [start:builder-arrival]
# Each arrival starts one pack_order
# An order every 5 s on average (rate 0.2)
app.add_arrival("orders", dist="exponential", rate=0.2, spawn="pack_order")
# --8<-- [end:builder-arrival]

# --8<-- [start:builder-arrival-loop]
# A return every 10 s on average (rate 0.1)
app.add_arrival("returns", dist="exponential", rate=0.1)


@app.arrival_loop("returns")
def returns(context: SimulationContext):
    while True:
        # Draws the next gap, waits for it, and returns the new entity, keyed returns-1, ...
        parcel = yield context.wait_for_arrival("returns")
        task_id = context.next_instance("return_logged")  # the task's one counter
        context.spawn(return_logged(task_id, entity=parcel))  # the loop goes on at once


# --8<-- [end:builder-arrival-loop]


# --8<-- [start:builder-process]
def breaks(context: SimulationContext, resource: str, every: float, duration: float):
    """Takes every packer off for a break of `duration` seconds every `every` seconds."""
    path = f"{context.sim_id}.resources.{resource}.current_cap"
    registry = context.env.registry
    while True:
        yield context.env.timeout(every)
        normal = registry.get(path).value
        registry.update(path, 0)
        yield context.env.timeout(duration)
        registry.update(path, normal)


app.add_process(breaks, resource="packers", every=50, duration=5)
# --8<-- [end:builder-process]


# --8<-- [start:low-level-task]
def pack_order_process(env, sampler, packers, key, task_id, label_ids):
    """What @app.task does, by hand."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        payload = {"event_type": "order_packed"}
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish(payload)
    # next: print_label, a new instance for the same order
    env.process(print_label_process(env, sampler, key, next(label_ids)))


# --8<-- [end:low-level-task]


# --8<-- [start:low-level-timed-step]
def print_label_process(env, sampler, key, task_id):
    """A timed step: started at once, with no queued event."""
    path_id = "Shop.service.labelling"

    def publish(value):
        env.publish_event(key, value, task="print_label", instance=task_id)

    publish({"path_id": path_id, "status": "started"})
    yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    publish({"event_type": "label_printed"})


# --8<-- [end:low-level-timed-step]


# --8<-- [start:low-level-process]
def breaks_process(env, resource: str, every: float, duration: float):
    """What add_process starts, by hand: a plain SimPy generator."""
    path = f"Shop.resources.{resource}.current_cap"
    while True:
        yield env.timeout(every)
        normal = env.registry.get(path).value
        env.registry.update(path, 0)
        yield env.timeout(duration)
        env.registry.update(path, normal)


# --8<-- [end:low-level-process]


# --8<-- [start:low-level-arrival]
def orders_process(env, sampler, packers):
    """What an arrival with spawn does, by hand."""
    gap = env.registry.get_config("Shop.arrival.orders")
    label_ids = itertools.count()
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(gap))
        key = f"orders-{task_id + 1}"
        env.process(pack_order_process(env, sampler, packers, key, task_id, label_ids))
        task_id += 1


# --8<-- [end:low-level-arrival]


def run_low_level(until):
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            # An order every 5 s on average (rate 0.2)
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.2)},
            service={
                "packing": DistributionConfig(dist="normal", mean=4.0, std=1.0),
                "labelling": DistributionConfig(dist="constant", value=2.0),
            },
            resources={"packers": CapacityConfig(current_cap=1, max_cap=3)},
        )
    )
    env.setup_egress([ConsoleEgress()])
    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=1)
    # --8<-- [start:low-level-start]
    env.process(orders_process(env, sampler, packers))  # start each loop once
    env.process(breaks_process(env, "packers", every=50, duration=5))
    # --8<-- [end:low-level-start]
    try:
        env.run(until=until)
    finally:
        env.teardown()


if __name__ == "__main__":
    app.run(until="3 min")
    run_low_level(until=180)

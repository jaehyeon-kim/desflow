"""Live parameters: an arrival rate, a service time and a capacity changed on the
way, and a run that ends by its end conditions, builder API.

A regression scenario (see scripts/regression.py), not an example. A process updates
registry paths at fixed times, as an ingress connector would; the twin runners remove
ingress, so the process stands in for it. The 500 boxes are taken at about 740
seconds, which ends the run by its stop condition; 900 seconds and a drain of at most
120 seconds are the limits if they were not.
"""

from desflow import ConsoleEgress, SimulationContext

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

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=2, max_cap=6)
    .add_container("boxes", current_cap=500, max_cap=500, level=500)
    .add_service("packing", dist="normal", mean=3, std=0.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .with_stats("basic", interval=30.0)
)


@app.task(
    service_id="packing",
    resource_id="packers",
    get={"container": "boxes", "amount": 1},
)
def pack(task_id, context):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_loop(context):
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), context, entity=entity))


def steer(context):
    """Applies each change at its time, as an ingress connector would."""
    env = context.env
    for at, path, value in CHANGES:
        yield env.timeout(at - env.now)
        env.registry.update(path, value)


app.add_process(steer)


def run():
    app.run(
        until=900,
        drain={"limit": 120},
        stop={"condition": "level.boxes == 0"},
    )


if __name__ == "__main__":
    run()

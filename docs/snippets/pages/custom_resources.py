"""A resource built from a class of your own, for the Resources page.

tests/docs runs this file unpaced: the low-level run and the builder model each use
a `LoggedPackers`, and the asserts check what it kept.
"""

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    ResourceBase,
    SimParameter,
    SimulationContext,
)


# --8<-- [start:class]
class LoggedPackers(ResourceBase):
    """A resource that keeps how long each request waited before it was granted."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.waits = []

    def _do_put(self, request):
        super()._do_put(request)
        if request.triggered:  # the request was granted just now
            self.waits.append(self.env.now - request.time)


# --8<-- [end:class]

# --8<-- [start:builder]
app = SimulationContext(sim_id="Shop", factor=0.0)
app.add_resource("packers", current_cap=1, max_cap=4, type=LoggedPackers)
# --8<-- [end:builder]
app.add_service("packing", dist="constant", value=1.5)
app.add_arrival("orders", dist="constant", value=1.0)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def orders(context):
    task_id = 0
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(task_id, entity=entity))
        task_id += 1


# --8<-- [start:low-level]
env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop", resources={"packers": CapacityConfig(current_cap=1, max_cap=4)}
    )
)
packers = LoggedPackers(env, "Shop", "packers")


def order(env):
    with packers.request() as request:
        yield request
        yield env.timeout(2.0)


# --8<-- [end:low-level]
for _ in range(3):
    env.process(order(env))

if __name__ == "__main__":
    try:
        env.run(until=10.0)
    finally:
        env.teardown()
    assert packers.waits == [0.0, 2.0, 4.0]
    app.run(until=20)
    resource = app.get_resource("packers")
    assert isinstance(resource, LoggedPackers) and len(resource.waits) > 3

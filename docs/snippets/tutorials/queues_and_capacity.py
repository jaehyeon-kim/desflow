"""The Queues and Capacity tutorial: the shop's busy hour, with the builder.

Orders arrive at 1.8 a second and each takes 1.5 seconds to pack. The script runs the
hour once for each number of packers, unpaced, and prints the mean wait of each run.
"""

from desflow import SimulationContext

UNTIL = "1 h"  # the busy hour


def shop(packers: int) -> SimulationContext:
    """The shop in its busy hour, with `packers` packers."""
    # --8<-- [start:load]
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)  # unpaced
    app.add_service("packing", dist="constant", value=1.5)
    # The busy hour: 1.8 orders a second, more than one packer can pack
    app.add_arrival("orders", dist="exponential", rate=1.8)
    # --8<-- [end:load]
    # --8<-- [start:packers]
    app.add_resource("packers", current_cap=packers, max_cap=10)
    # --8<-- [end:packers]
    # --8<-- [start:stats]
    # The mean wait of pack_order, kept by the run and read at the end
    app.with_stats("typical", interval=3600)
    # --8<-- [end:stats]

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


# --8<-- [start:table]
def mean_wait(packers: int) -> float:
    """Runs the busy hour with `packers` packers and returns the mean wait."""
    app = shop(packers)
    app.run(until=UNTIL)
    return app.env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


if __name__ == "__main__":
    for packers in range(1, 5):
        print(f"packers {packers}: mean wait {mean_wait(packers):8.2f} s")
# --8<-- [end:table]

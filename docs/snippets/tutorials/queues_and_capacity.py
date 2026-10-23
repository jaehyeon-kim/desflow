"""The Queues and Capacity tutorial: the shop's busy hour, with the builder.

Orders arrive at 1.8 a second and each takes 1.5 seconds to pack. `explain` prints
what `desflow explain` prints for the shop. The script runs the hour once for each
number of packers, unpaced, and prints the mean wait of each run. `network` is the
shop with a checker and rework, and `rush` the half hour that a rush overloads.
"""

from desflow import SimulationContext
from desflow.explain.render import render_terminal

UNTIL = "1 h"  # the busy hour


def shop(packers: int) -> SimulationContext:
    """The shop in its busy hour, with `packers` packers."""
    # --8<-- [start:load]
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)  # unpaced
    app.add_service("packing", dist="constant", value=1.5)
    # The busy hour: 1.8 orders a second, more than one packer can pack. Each
    # order starts the task pack_order.
    app.add_arrival("orders", dist="exponential", rate=1.8, spawn="pack_order")
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

    return app


# --8<-- [start:explain]
def explain(packers: int) -> str:
    """The busy hour with `packers` packers, as `desflow explain` prints it."""
    return render_terminal(shop(packers).analyze())


# --8<-- [end:explain]


def network() -> SimulationContext:
    """The busy shop as a network: packed orders are checked, one in ten packed again."""
    # --8<-- [start:network]
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.add_arrival("orders", dist="exponential", rate=1.6, spawn="pack_order")
    app.add_service("packing", dist="constant", value=1.5)
    app.add_service("checking", dist="exponential", rate=2.5)  # 0.4 s on average
    app.add_resource("packers", current_cap=4, max_cap=10)
    app.add_resource("checkers", current_cap=1, max_cap=4)

    @app.task(service_id="packing", resource_id="packers", next="check_order")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    # Nine in ten pass; the tenth is packed again
    @app.task(
        service_id="checking",
        resource_id="checkers",
        next={"weights": {"ship_order": 9, "pack_order": 1}},
    )
    def check_order(task_id):
        return {"event_type": "order_checked"}

    @app.task()
    def ship_order(task_id):
        return {"event_type": "order_shipped"}

    # print(render_terminal(app.analyze())) prints what desflow explain prints
    # --8<-- [end:network]
    return app


def rush() -> SimulationContext:
    """Three packers keep up with 1.8 orders a second until a rush at 10 minutes."""
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.add_service("packing", dist="constant", value=1.5)
    app.add_arrival("orders", dist="exponential", rate=1.8, spawn="pack_order")
    app.add_resource("packers", current_cap=3, max_cap=10)
    # --8<-- [start:rush]
    # The rush: 2.4 orders a second from 10 minutes, utilisation 2.4 x 1.5 / 3 = 1.2
    app.add_step(at=600, path="Shop.arrival.orders.rate", value=2.4)
    # The queue length, published every 5 minutes
    app.with_stats("minimal", interval=300)
    # --8<-- [end:rush]

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

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

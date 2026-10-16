"""The Priorities tutorial: express orders go before regular ones, builder.

One packer, regular orders at 0.45 a second and express orders at 0.15, and 1.5
seconds of packing. Every record is printed to the terminal, and the script prints
the mean wait of each kind of order under each queue order at the end:
python priorities.py 2>/dev/null shows only those lines.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(queue: str = "priority") -> SimulationContext:
    """The shop with the packer's queue in `queue` order."""

    def rank(value):
        # A priority counts only under the priority order, and the other orders
        # refuse one, so under fifo the tasks have none
        return value if queue == "priority" else None

    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_service("packing", dist="constant", value=1.5)
        # 0.6 orders a second in all, one in four of them express
        .add_arrival("orders", dist="exponential", rate=0.45)
        .add_arrival("express", dist="exponential", rate=0.15)
        .with_stats("typical", interval=3600)  # the mean wait of each task
        .add_egress(ConsoleEgress())
    )
    # --8<-- [start:queue]
    # The packer serves the lowest priority first, the oldest first among equals
    app.add_resource("packers", current_cap=1, max_cap=4, queue=queue)
    # --8<-- [end:queue]

    # --8<-- [start:priority]
    @app.task(service_id="packing", resource_id="packers", priority=rank(1))
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    # Served before any regular order that is waiting
    @app.task(service_id="packing", resource_id="packers", priority=rank(0))
    def pack_express(task_id: int):
        return {"event_type": "express_packed"}

    # --8<-- [end:priority]

    def loop(arrival, task):
        def generator(context: SimulationContext):
            task_id = 0
            while True:
                order = yield context.wait_for_arrival(arrival)
                context.spawn(task(task_id, entity=order))
                task_id += 1

        return generator

    app.arrival_loop("orders")(loop("orders", pack_order))
    app.arrival_loop("express")(loop("express", pack_express))
    return app


def mean_waits(queue: str) -> dict:
    """Runs one hour and returns the mean wait of each task."""
    app = shop(queue)
    app.run(until="1 h")
    readings = app.env.stats.readings()
    return {
        task: readings[f"Shop.stats.tasks.{task}.wait_mean"]
        for task in ("pack_express", "pack_order")
    }


if __name__ == "__main__":
    for queue in ("fifo", "priority"):
        for task, wait in mean_waits(queue).items():
            print(f"{queue:<8} {task:<12} mean wait {wait:5.2f} s")

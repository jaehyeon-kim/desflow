"""The Priorities tutorial: express orders go before regular ones, builder.

One packer and 1.5 seconds of packing. A regular order arrives every 2.22 seconds on
average (rate 0.45), and an express order every 6.67 seconds (rate 0.15). Every record
is printed to the terminal, and the script prints the mean wait of each kind of order
under each queue order at the end. To see only those lines, run:
python priorities.py 2>/dev/null
To use other rates, give the two as arguments: python priorities.py 0.375 0.125
"""

import logging
import sys

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(
    queue: str = "priority", orders_rate: float = 0.45, express_rate: float = 0.15
) -> SimulationContext:
    """The shop with the packer's queue in `queue` order and the two arrival rates."""

    def rank(value):
        # A priority counts only under the priority order, and the other orders
        # refuse one, so under fifo the tasks have none
        return value if queue == "priority" else None

    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_service("packing", dist="constant", value=1.5)
        # An order every 1.67 s on average in all (0.6 a second), one in four express
        .add_arrival("orders", dist="exponential", rate=orders_rate)
        .add_arrival("express", dist="exponential", rate=express_rate)
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


def mean_waits(
    queue: str, orders_rate: float = 0.45, express_rate: float = 0.15
) -> dict:
    """Runs one hour and returns the mean wait of each task."""
    app = shop(queue, orders_rate, express_rate)
    app.run(until="1 h")
    readings = app.env.stats.readings()
    return {
        task: readings[f"Shop.stats.tasks.{task}.wait_mean"]
        for task in ("pack_express", "pack_order")
    }


if __name__ == "__main__":
    rates = [float(rate) for rate in sys.argv[1:3]]
    for queue in ("fifo", "priority"):
        for task, wait in mean_waits(queue, *rates).items():
            print(f"{queue:<8} {task:<12} mean wait {wait:5.2f} s")

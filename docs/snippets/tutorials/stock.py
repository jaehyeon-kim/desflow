"""The Stock tutorial: packing takes a box from stock, and deliveries restock it.

One packer, an order every 2 seconds on average, and a delivery of boxes every 100
seconds. Every record is printed to the terminal, and the script prints the mean wait
for 60-box and 40-box deliveries at the end. To see only those lines, run:
python stock.py 2>/dev/null
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(delivery: float = 60) -> SimulationContext:
    """The shop with deliveries of `delivery` boxes every 100 seconds."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=4)
        .add_service("packing", dist="constant", value=1.5)
        # One order every 2 seconds on average: 50 boxes every 100 seconds
        .add_arrival("orders", dist="exponential", rate=0.5)
        # --8<-- [start:watch]
        # The stock's level and waiting tasks every 10 seconds, and the mean wait
        .with_stats("typical")
        # --8<-- [end:watch]
        .add_egress(ConsoleEgress())
    )
    # --8<-- [start:container]
    # Boxes on the shelf: room for 200, and 50 when the run starts
    app.add_container("stock", current_cap=200, max_cap=200, level=50)
    # --8<-- [end:container]

    # --8<-- [start:take]
    # One box per order, taken once the order has a packer; waits while stock is 0
    @app.task(
        service_id="packing",
        resource_id="packers",
        get={"container": "stock", "amount": 1},
    )
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    # --8<-- [end:take]

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    # --8<-- [start:restock]
    # A delivery every 100 seconds
    app.add_arrival("deliveries", dist="constant", value=100)

    # Adds the delivery to the shelf; waits while there is no room for it all
    @app.task(put={"container": "stock", "amount": delivery})
    def restock(task_id: int):
        return {"event_type": "stock_delivered"}

    @app.arrival_loop("deliveries")
    def delivery_generator(context: SimulationContext):
        task_id = 0
        while True:
            delivery = yield context.wait_for_arrival("deliveries")
            context.spawn(restock(task_id, entity=delivery))
            task_id += 1

    # --8<-- [end:restock]
    return app


def mean_wait(delivery: float) -> float:
    """Runs one hour and returns the mean wait for packing."""
    app = shop(delivery)
    app.run(until="1 h")
    return app.env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


if __name__ == "__main__":
    for delivery in (60, 40):
        print(f"deliveries of {delivery}: mean wait {mean_wait(delivery):6.2f} s")

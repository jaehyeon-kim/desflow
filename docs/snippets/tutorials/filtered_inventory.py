"""The Filtered Inventory tutorial: each order waits for a shirt of its own size.

Shirts of three sizes arrive on one shelf, and orders of three sizes take them.
Every record is printed to the terminal. The script then prints, for each size, how
many orders started, were filled, were still waiting and were cancelled, and the shirts left
on the shelf. To see only those lines, run:
python filtered_inventory.py 2>/dev/null
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# A shirt every 10 seconds on average: 3 in 10 small, 4 medium, 3 large
SHIRTS = {"S": 0.03, "M": 0.04, "L": 0.03}
# An order every 10 seconds on average: a quarter small, half medium, a quarter large
ORDERS = {"S": 0.025, "M": 0.05, "L": 0.025}


def shop() -> SimulationContext:
    """The shop with one shelf of shirts and orders of three sizes."""
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42).add_egress(
        ConsoleEgress()
    )
    # --8<-- [start:stats]
    # The counts of each arrival's orders, published once, at the end of the 8 hours
    app.with_stats("basic", interval=8 * 3600)
    # --8<-- [end:stats]
    # --8<-- [start:shelf]
    # Shirts of every size on one shelf, each item a mapping such as {"size": "M"}
    app.add_store("shelf", current_cap=1000, max_cap=1000)
    # --8<-- [end:shelf]
    # --8<-- [start:arrivals]
    for size, rate in SHIRTS.items():
        name = f"shirts_{size.lower()}"
        app.add_arrival(name, dist="exponential", rate=rate, attributes={"size": size})
    for size, rate in ORDERS.items():
        name = f"orders_{size.lower()}"
        app.add_arrival(name, dist="exponential", rate=rate, attributes={"size": size})
    # --8<-- [end:arrivals]

    # --8<-- [start:restock]
    # The shirt goes on the shelf, as a mapping with its size
    @app.task(put={"store": "shelf"})
    def restock(task_id: int, entity):
        return {"size": entity.attributes["size"]}

    # --8<-- [end:restock]

    # --8<-- [start:take]
    @app.task(
        # Takes the oldest shirt of the order's own size; waits while there is none
        get={"store": "shelf", "filter": "item.size == attr.size"},
        # A customer cancels an order not filled within 10 minutes
        patience="10 min",
    )
    def take_shirt(task_id: int, item):
        # `item` is the shirt the get took
        return {"event_type": "order_filled", "shirt": item}

    # --8<-- [end:take]

    def spawn(arrival: str, task, name: str):
        def loop(context: SimulationContext):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)

    for size in SHIRTS:
        spawn(f"shirts_{size.lower()}", restock, "restock")
    for size in ORDERS:
        spawn(f"orders_{size.lower()}", take_shirt, "take_shirt")
    return app


def outcomes() -> dict:
    """Runs eight hours; returns each size's order counts and the shirts left."""
    app = shop()
    app.run(until="8 h")
    readings = app.env.stats.readings()
    result: dict = {}
    for size in ORDERS:
        flow = f"Shop.stats.flows.orders_{size.lower()}"
        started = readings[f"{flow}.started"]
        filled = readings[f"{flow}.finished"]
        waiting = readings[f"{flow}.in_system"]
        # A cancelled order leaves its flow without finishing it
        result[size] = (started, filled, waiting, started - filled - waiting)
    result["left"] = readings["Shop.stats.stores.shelf.level"]
    return result


if __name__ == "__main__":
    result = outcomes()
    for size in ORDERS:
        started, filled, waiting, cancelled = result[size]
        print(
            f"size {size}: {started:4} started, {filled:4} filled, "
            f"{waiting:2} waiting, {cancelled:3} cancelled"
        )
    print(f"shirts left on the shelf: {result['left']}")

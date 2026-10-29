"""The Filtered Inventory tutorial: each order waits for a shirt of its own size.

Shirts of three sizes arrive on one shelf, and orders of three sizes take them.
Every record is printed to the terminal. The script then prints, for each size, how
many orders were filled and how many were cancelled. To see only those lines, run:
python filtered_inventory.py 2>/dev/null
"""

import logging
from collections import Counter

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

    # The shirt goes on the shelf, as a mapping with its size
    @app.task(put={"store": "shelf"})
    def restock(task_id: int, entity):
        return {"size": entity.attributes["size"]}

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


class _Outcomes:
    """An egress that counts each order's outcome, by size, from its events."""

    def __init__(self):
        self.counts = Counter()

    async def run(self, egress_queue):
        import asyncio
        import queue

        try:
            while True:
                try:
                    for record in egress_queue.get_nowait():
                        count(self.counts, record)
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


def count(counts: Counter, record: dict) -> None:
    """Counts a filled or cancelled order, by the size in its key."""
    if record["stream_type"] != "event" or record["task"] != "take_shirt":
        return
    size = record["key"].split("-")[0][-1].upper()
    value = record["value"]
    if value.get("event_type") == "order_filled":
        counts[(size, "filled")] += 1
    elif value.get("status") == "reneged":
        counts[(size, "cancelled")] += 1


def outcomes() -> Counter:
    """Runs eight hours and counts filled and cancelled orders by size."""
    app = shop()
    outcome = _Outcomes()
    app.add_egress(outcome)
    app.run(until="8 h")
    return outcome.counts


if __name__ == "__main__":
    counts = outcomes()
    for size in ORDERS:
        print(
            f"size {size}: {counts[(size, 'filled')]:4} filled, "
            f"{counts[(size, 'cancelled')]:4} cancelled"
        )

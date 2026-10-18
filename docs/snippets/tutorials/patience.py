"""The Patience tutorial: orders leave after waiting too long, and none join near closing.

One packer, 0.7 orders a second and 1.5 seconds of packing. Every record is printed
to the terminal, and the script prints what happened to the orders with a patience
of 30 seconds and with none at the end: python patience.py 2>/dev/null shows only
those lines.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(patience: float = 30) -> SimulationContext:
    """The shop, where a customer waits at most `patience` seconds."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=4)
        .add_service("packing", dist="constant", value=1.5)
        # 0.7 orders a second: more than one packer can pack
        .add_arrival("orders", dist="exponential", rate=0.7)
        # --8<-- [start:watch]
        # Counts of finished, reneged and balked orders, and the mean wait, at the end
        .with_stats("typical", interval=3600)
        # --8<-- [end:watch]
        .add_egress(ConsoleEgress())
    )

    # --8<-- [start:task]
    @app.task(
        service_id="packing",
        resource_id="packers",
        # --8<-- [start:patience]
        # A customer cancels an order not started within 30 seconds
        patience=patience,
        # --8<-- [end:patience]
        # --8<-- [start:balk]
        # The courier leaves at the hour, so the last 5 minutes take no new orders
        balk="t >= 3300",
        # --8<-- [end:balk]
    )
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    # --8<-- [end:task]

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def outcomes(patience: float) -> dict:
    """Runs one hour and returns what happened to the orders, and the mean wait."""
    app = shop(patience)
    app.run(until="1 h")
    readings = app.env.stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        outcome: readings.get(name(outcome), 0)
        for outcome in ("finished", "reneged", "balked", "wait_mean")
    }


if __name__ == "__main__":
    for patience, label in ((30, "30 s"), (3600, "none")):
        result = outcomes(patience)
        print(
            f"patience {label:>4}: {result['finished']} packed, "
            f"{result['reneged']} reneged, {result['balked']} balked, "
            f"mean wait {result['wait_mean']:5.2f} s"
        )

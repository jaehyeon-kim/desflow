"""The End Conditions tutorial: a fixed horizon against a drain.

One packer, 0.7 orders a second and 1.5 seconds of packing, for an hour. Every
record is printed to the terminal, and the script prints the counts and the mean
wait with a fixed horizon and with a drain at the end:
python end_conditions.py 2>/dev/null shows only those lines.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop() -> SimulationContext:
    """The shop of First Simulation, busier: more orders than one packer can pack."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=4)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.7)
        # --8<-- [start:watch]
        # Orders started, finished and still in the shop, and the mean wait, at the end
        .with_stats("typical", interval=7200)
        # --8<-- [end:watch]
        .add_egress(ConsoleEgress())
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def outcomes(drain: bool) -> dict:
    """Runs the hour, with or without a drain, and returns the counts and the wait."""
    app = shop()
    # --8<-- [start:run]
    # No new orders after one hour; with drain, pack every order already in the shop
    app.run(until="1 h", drain=drain or None)
    # --8<-- [end:run]
    r = app.env.stats.readings()
    name = "Shop.stats.{}".format
    return {
        "started": r[name("flows.orders.started")],
        "finished": r[name("flows.orders.finished")],
        "in_system": r[name("flows.orders.in_system")],
        "wait_mean": r[name("tasks.pack_order.wait_mean")],
        "end": app.env.now,
    }


if __name__ == "__main__":
    for drain in (False, True):
        r = outcomes(drain)
        label = "drain  " if drain else "horizon"
        print(
            f"{label}: ends at {r['end']:7.1f} s, {r['started']} started, "
            f"{r['finished']} finished, {r['in_system']} left, "
            f"mean wait {r['wait_mean']:5.2f} s"
        )

"""The Changing Demand tutorial: a 20-minute rush, with one packer or two.

One packer and 1.5 seconds of packing; orders arrive at 0.4 a second, and at 0.9
from 20 to 40 minutes. Every record is printed to the terminal, and the script
prints the waits with one packer through the rush and with a second packer for it:
python changing_demand.py 2>/dev/null shows only those lines.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(rush_packers: int) -> SimulationContext:
    """The shop of First Simulation, with a rush from 20 to 40 minutes."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=2)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.4)
        # --8<-- [start:schedule]
        # The rush: 0.9 orders a second from 20 to 40 minutes, and rush_packers for it
        .add_scenario(
            [
                {"at": "20 min", "path": "Shop.arrival.orders.rate", "value": 0.9},
                {
                    "at": "20 min",
                    "path": "Shop.resources.packers.current_cap",
                    "value": rush_packers,
                },
                {"at": "40 min", "path": "Shop.arrival.orders.rate", "value": 0.4},
                {
                    "at": "40 min",
                    "path": "Shop.resources.packers.current_cap",
                    "value": 1,
                },
            ]
        )
        # --8<-- [end:schedule]
        # Orders packed, and the mean and longest wait, at the end
        .with_stats("typical", interval=7200)
        .add_egress(ConsoleEgress())
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            # The gap is drawn from the rate in force now
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def outcomes(rush_packers: int) -> dict:
    """Runs the hour and returns the orders packed and their waits."""
    app = shop(rush_packers)
    app.run(until="1 h")
    r = app.env.stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "finished": r[name("finished")],
        "wait_mean": r[name("wait_mean")],
        "wait_max": r[name("wait_max")],
    }


if __name__ == "__main__":
    for packers in (1, 2):
        r = outcomes(packers)
        print(
            f"{packers} in the rush: {r['finished']} packed, "
            f"mean wait {r['wait_mean']:5.2f} s, longest {r['wait_max']:6.2f} s"
        )

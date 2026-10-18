"""The Local Replay tutorial: a recorded morning of orders, against a model of it.

One packer and 1.5 seconds of packing. The orders of 09:00 to 10:00 on 1 October
2026 arrive at their recorded times from local_replay_orders.csv; a second run
draws exponential gaps at the same mean rate, 0.545 orders a second. Every record
is printed to the terminal, and the script prints the waits of both runs:
python local_replay.py 2>/dev/null shows only those lines.
"""

import logging
from datetime import datetime
from pathlib import Path

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

TRACE = Path(__file__).resolve().parent / "local_replay_orders.csv"


def shop(replay: bool) -> SimulationContext:
    """One packer, with the recorded orders or with a model of them."""
    app = (
        SimulationContext(
            sim_id="Shop",
            factor=0,
            random_seed=42,
            # Simulation time 0 is 09:00 on the recorded day
            logical_start_time=datetime(2026, 10, 1, 9, 0),
        )
        .add_resource("packers", current_cap=1, max_cap=2)
        .add_service("packing", dist="constant", value=1.5)
        # Orders packed, and the mean and longest wait, at the end
        .with_stats("typical", interval=7200)
        .add_egress(ConsoleEgress())
    )
    if replay:
        # --8<-- [start:replay]
        # Each row is one order, at its recorded time, carrying its order_id and items
        app.add_arrival_replay(
            "orders",
            TRACE,
            time="ordered_at",
            unit="timestamp",
            fields=["order_id", "items"],
            # A row without a valid time is left out, with a warning naming it
            on_invalid="skip",
        )
        # --8<-- [end:replay]
    else:
        # The same mean rate: 1,963 orders in the hour
        app.add_arrival("orders", dist="exponential", rate=0.545)

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    # --8<-- [start:loop]
    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            # The wait for the next row, as for any other arrival
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    # --8<-- [end:loop]

    return app


def outcomes(replay: bool) -> dict:
    """Runs the hour and returns the orders packed and their waits."""
    app = shop(replay)
    app.run(until="1 h")
    r = app.env.stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "finished": r[name("finished")],
        "wait_mean": r[name("wait_mean")],
        "wait_max": r[name("wait_max")],
    }


if __name__ == "__main__":
    for replay in (True, False):
        r = outcomes(replay)
        label = "recorded" if replay else "modelled"
        print(
            f"{label}: {r['finished']} packed, "
            f"mean wait {r['wait_mean']:5.2f} s, longest {r['wait_max']:6.2f} s"
        )

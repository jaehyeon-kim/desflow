"""The Trusting Results tutorial: the busy hour, measured after a warm-up, builder.

Three packers, orders at 1.8 a second and the measured packing times. Every record is
printed to the terminal, and the script prints the mean wait without and with the
warm-up at the end: python trusting_results.py 2>/dev/null shows only those lines.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# The 40 packing times measured at the bench, in seconds
MEASURED = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip


def shop(warmup="2 min") -> SimulationContext:
    """The busy hour; records and statistics start after `warmup`."""
    app = (
        # --8<-- [start:warmup]
        # Records and statistics start after the first 2 minutes; None keeps everything
        SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup=warmup)
        # --8<-- [end:warmup]
        .add_resource("packers", current_cap=3, max_cap=10)
        .add_arrival("orders", dist="exponential", rate=1.8)
        .add_service("packing", dist="empirical", values=MEASURED)
        # --8<-- [start:plot]
        # wait_mean every 10 seconds is the mean wait so far: plot it against sim_ts
        .with_stats("typical", interval=10)
        .add_egress(ConsoleEgress())
        # --8<-- [end:plot]
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


def mean_wait(warmup) -> float:
    """Runs the busy hour and returns the mean wait it measured."""
    app = shop(warmup)
    app.run(until="1 h")
    return app.env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


if __name__ == "__main__":
    print(f"without a warm-up: mean wait {mean_wait(None):.3f} s")
    print(f"with a 2 min warm-up: mean wait {mean_wait('2 min'):.3f} s")

"""Replications, builder API: one hour of the shop, run 20 times with intervals.

Two packers, orders every 2.5 seconds on average and a packing time of 4 seconds on
average. `replicate` calls `shop` once per replication, so each starts from a new
context, and runs it unpaced with no sinks, after a 3-minute warm-up, with a seed
spawned from 42. It reads every statistic up to the typical level at the end of each
run, and the script prints the mean and the 95% half-width of each. The Console sink
receives them once, as telemetry.

Needs no containers.
"""

import logging

from desflow import ConsoleEgress, SimulationContext, replicate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
# Each replication would log its build and teardown; the sink still logs
logging.getLogger("desflow.core").setLevel(logging.WARNING)


def shop() -> SimulationContext:
    """A new shop: replicate calls this once per replication."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup="3 min")
        # Receives each replication's value and the mean, half-width and count
        .add_egress(ConsoleEgress())
        .add_resource("packers", current_cap=2, max_cap=4)
        # A mean of 4 seconds (shape times scale)
        .add_service("packing", dist="gamma", shape=4, scale=1)
        .add_arrival("orders", dist="exponential", rate=0.4)
        # Wait and service means need the typical level
        .with_stats("typical", interval=600)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id, context):
        """Packs one order."""
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_loop(context):
        """Spawns a pack task for every order."""
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack(task_id, context, entity=entity))
            task_id += 1

    return app


def run():
    """Runs 20 replications of one hour and prints their summary."""
    # --8<-- [start:replicate]
    # 20 seeds spawned from 42; each replication runs unpaced, with no sinks
    result = replicate(shop, 20, seed=42, until="1 h")
    print(result.summary_text())
    # Any replication repeats on its own: run shop() with its seed
    print("replication seeds:", result.seeds)
    # --8<-- [end:replicate]


if __name__ == "__main__":
    run()

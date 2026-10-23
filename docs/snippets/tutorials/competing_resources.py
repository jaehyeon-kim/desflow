"""The Competing Resources tutorial: an order keeps the picker until it is on the bench.

One picker, one packing bench, and an order every 2.5 seconds on average. Every record
is printed to the terminal, and the script prints the waits and the picker's busy share
with and without the handoff at the end. To see only those lines, run:
python competing_resources.py 2>/dev/null
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(keep: bool = True) -> SimulationContext:
    """The shop; with `keep`, an order keeps the picker until it is on the bench."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("picker", current_cap=1, max_cap=2)
        .add_resource("bench", current_cap=1, max_cap=2)
        .add_service("picking", dist="constant", value=1)
        .add_service("packing", dist="exponential", rate=0.5)
        .add_arrival("orders", dist="exponential", rate=0.4)
        # --8<-- [start:watch]
        # The mean wait of each task and the picker's busy share, at the end
        .with_stats("typical", interval=3600)
        # --8<-- [end:watch]
        .add_egress(ConsoleEgress())
    )

    # --8<-- [start:handoff]
    @app.task(
        service_id="picking",
        resource_id="picker",
        # The picker carries the order and stays with it until it is on the bench
        keep=["picker"] if keep else None,
        next="pack_order",
    )
    def pick_order(task_id: int):
        return {"event_type": "order_picked"}

    @app.task(service_id="packing", resource_id="bench")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    # --8<-- [end:handoff]

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pick_order(task_id, entity=order))
            task_id += 1

    return app


def outcomes(keep: bool) -> dict:
    """Runs one hour and returns each task's mean wait and the picker's busy share."""
    app = shop(keep)
    app.run(until="1 h")
    r = app.env.stats.readings()
    return {
        "pick_wait": r["Shop.stats.tasks.pick_order.wait_mean"],
        "pack_wait": r["Shop.stats.tasks.pack_order.wait_mean"],
        "picker_busy": r["Shop.stats.resources.picker.utilization_avg"],
    }


if __name__ == "__main__":
    for keep in (False, True):
        r = outcomes(keep)
        label = "keeps the picker" if keep else "frees the picker"
        print(
            f"{label}: wait to pick {r['pick_wait']:6.2f} s, to pack "
            f"{r['pack_wait']:5.2f} s, picker busy {r['picker_busy']:4.1f}%"
        )

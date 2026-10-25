"""The Interruptions tutorial: express orders take the packer from regular ones.

One packer and 1.5 seconds of packing. A regular order arrives every 2.22 seconds on
average (rate 0.45), and an express order every 6.67 seconds (rate 0.15). Every record
is printed to the terminal, and the script prints what preemption did to regular orders
under each policy at the end. To see only those lines, run:
python interruptions.py 2>/dev/null
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(policy: str = "resume") -> SimulationContext:
    """The shop, where a regular order follows `policy` when it loses the packer."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        # --8<-- [start:packers]
        # An express order takes the packer from a regular one being packed
        .add_resource(
            "packers", current_cap=1, max_cap=4, queue="priority", preempt=True
        )
        # --8<-- [end:packers]
        .add_service("packing", dist="constant", value=1.5)
        # An order every 1.67 s on average in all (0.6 a second), one in four express
        .add_arrival("orders", dist="exponential", rate=0.45)
        .add_arrival("express", dist="exponential", rate=0.15)
        # --8<-- [start:watch]
        # Counts of preemptions and resumptions, and the mean wait and service, at the end
        .with_stats("typical", interval=3600)
        # --8<-- [end:watch]
        .add_egress(ConsoleEgress())
    )

    # --8<-- [start:policy]
    @app.task(
        service_id="packing",
        resource_id="packers",
        priority=1,
        # What a regular order does when it loses the packer
        on_interrupt=policy,
    )
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    # --8<-- [end:policy]

    @app.task(service_id="packing", resource_id="packers", priority=0)
    def pack_express(task_id: int):
        return {"event_type": "express_packed"}

    for arrival, task, name in (
        ("orders", pack_order, "pack_order"),
        ("express", pack_express, "pack_express"),
    ):

        def loop(context: SimulationContext, arrival=arrival, task=task, name=name):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)

    # --8<-- [start:interrupt]
    def called_away(context: SimulationContext):
        """The packer is called away at 31 minutes: every regular order in the shop,
        waiting or being packed, is interrupted."""
        yield context.env.timeout(1860)
        context.interrupt("pack_order", cause="called away")

    app.add_process(called_away)
    # --8<-- [end:interrupt]
    return app


def outcomes(policy: str) -> dict:
    """Runs one hour and returns what preemption did to regular orders."""
    app = shop(policy)
    app.run(until="1 h")
    readings = app.env.stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "preempted": readings[name("preempted")],
        "interrupted": readings.get(name("interrupted"), 0),
        "finished": readings[name("finished")],
        "wait_mean": readings[name("wait_mean")],
        "service_mean": readings[name("service_mean")],
    }


if __name__ == "__main__":
    for policy in ("resume", "restart", "abandon"):
        r = outcomes(policy)
        print(
            f"{policy:<7}: {r['preempted']} preempted, {r['interrupted']} called away, "
            f"{r['finished']} packed, "
            f"mean wait {r['wait_mean']:5.2f} s, mean packing {r['service_mean']:4.2f} s"
        )

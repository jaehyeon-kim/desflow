"""The Process Interaction tutorial: packed orders wait for the courier, with a limit.

One packer, an order every 2 seconds on average, and a courier every 10 minutes.
Every record is printed to the terminal, and the script prints how many orders went
by courier and by post, with limits of 5 and 8 minutes, at the end:
python process_interaction.py 2>/dev/null shows only those lines.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(limit: float = 300) -> SimulationContext:
    """The shop, where a packed order waits at most `limit` seconds for the courier."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=4)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        # --8<-- [start:courier]
        # A courier every 10 minutes
        .add_arrival("couriers", dist="constant", value=600)
        # --8<-- [end:courier]
        # --8<-- [start:watch]
        # How many orders went each way, and the mean wait for dispatch, at the end
        .with_stats("typical", interval=8 * 3600)
        # --8<-- [end:watch]
        .add_egress(ConsoleEgress())
    )
    # --8<-- [start:signal]
    # Fired by the courier as it leaves with every order that waits for it
    app.add_signal("courier_left")
    # --8<-- [end:signal]

    @app.task(service_id="packing", resource_id="packers", next="dispatch")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    # --8<-- [start:dispatch]
    @app.task(
        # The courier's departure, or the limit, whichever comes first
        wait={
            "any": {"courier": {"signal": "courier_left"}, "limit": {"timeout": limit}}
        },
        # wait.courier.done is true when the courier came before the limit
        next={
            "when": [{"condition": "wait.courier.done", "task": "by_courier"}],
            "default": "by_post",
        },
    )
    def dispatch(task_id: int):
        return {"event_type": "dispatched"}

    # --8<-- [end:dispatch]

    @app.task()
    def by_courier(task_id: int):
        return {"event_type": "sent_by_courier"}

    @app.task()
    def by_post(task_id: int):
        return {"event_type": "sent_by_post"}

    # --8<-- [start:collect]
    # Fires courier_left with this payload, so every waiting order resumes
    @app.task(signal="courier_left")
    def collect(task_id: int):
        return {"event_type": "courier_left"}

    # --8<-- [end:collect]

    for arrival, task, name in (
        ("orders", pack_order, "pack_order"),
        ("couriers", collect, "collect"),
    ):

        def loop(context: SimulationContext, arrival=arrival, task=task, name=name):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)
    return app


def outcomes(limit: float) -> dict:
    """Runs eight hours and returns how orders went, and the mean wait for dispatch."""
    app = shop(limit)
    app.run(until="8 h")
    readings = app.env.stats.readings()
    name = "Shop.stats.tasks.{}".format
    return {
        "by_courier": readings[name("by_courier.finished")],
        "by_post": readings[name("by_post.finished")],
        "wait_mean": readings[name("dispatch.wait_mean")],
    }


if __name__ == "__main__":
    for limit in (300, 480):
        result = outcomes(limit)
        print(
            f"limit {limit / 60:.0f} min: {result['by_courier']} by courier, "
            f"{result['by_post']} by post, mean wait {result['wait_mean']:6.1f} s"
        )

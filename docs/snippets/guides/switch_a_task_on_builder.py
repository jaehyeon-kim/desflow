"""The Switch a Task On Mid-run guide, Builder API: from 1 minute, one order in four
is gift wrapped after packing.

`build()` returns the app. Run it with: python switch_a_task_on_builder.py, which runs
the shop for 5 simulated minutes and prints its events, as switch_a_task_on.yaml does.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)


def build() -> SimulationContext:
    """The shop, with gift wrapping declared from the start and switched on at 1 min."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_egress(ConsoleEgress())
        # --8<-- [start:declare]
        # The switch: a weight of 0 sends no order to wrap_gift
        .add_variable("wrap_weight", 0)
        .add_resource("packers", current_cap=1, max_cap=1)
        # The new task's resource exists from the start, unused until the switch
        .add_resource("wrappers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value="2 s")
        .add_service("wrapping", dist="constant", value="3 s")
        # --8<-- [end:declare]
        .add_arrival("orders", dist="constant", value="5 s", spawn="pack_order")
    )

    # --8<-- [start:route]
    @app.task(
        service_id="packing",
        resource_id="packers",
        # Read when each order is packed: 3 to 0 before the switch, 3 to 1 after it
        next={
            "weights": {
                "ship_order": 3,
                "wrap_gift": 'registry["Shop.variables.wrap_weight"]',
            }
        },
    )
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.task(service_id="wrapping", resource_id="wrappers", next="ship_order")
    def wrap_gift(task_id: int):
        return {"event_type": "gift_wrapped"}

    # --8<-- [end:route]
    @app.task()
    def ship_order(task_id: int):
        return {"event_type": "order_shipped"}

    # --8<-- [start:scenario]
    # At 1 minute wrapping starts, and the step records what it did
    app.add_scenario(
        [
            {
                "at": "1 min",
                "path": "Shop.variables.wrap_weight",
                "value": 1,
                "record": {"intervention": "gift_wrapping_on"},
            }
        ]
    )
    # --8<-- [end:scenario]
    return app


if __name__ == "__main__":
    build().run(until=300)

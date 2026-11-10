"""The Test a Pipeline page, builder form: the shop with a planted fault. Save it as
pipeline_shop.py for the tests."""

from pathlib import Path

from desflow import JsonlStorageEgress, SimulationContext


# --8<-- [start:shop]
def run_shop(folder: Path) -> None:
    """Runs the shop, writing its data and labels under `folder`."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=3, max_cap=5)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5, spawn="pack_order")
        # The data under test: events only, with no label
        .add_egress(
            JsonlStorageEgress(default_path=f"{folder}/data/events.jsonl"),
            when=lambda record: record["stream_type"] == "event",
        )
        # The labels only, for the scorer
        .add_egress(
            JsonlStorageEgress(default_path=f"{folder}/labels/labels.jsonl"),
            when="labels",
        )
        .add_scenario(
            [
                # The fault: the order rate triples at 300 seconds, and the label says so
                {
                    "at": 300,
                    "path": "Shop.arrival.orders.rate",
                    "value": 1.5,
                    "record": {"fault": "order_surge"},
                },
                # The end of the fault, which needs no label
                {"at": 360, "path": "Shop.arrival.orders.rate", "value": 0.5},
            ]
        )
    )

    @app.task(name="pack_order", service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    app.run(until="10 min")


# --8<-- [end:shop]

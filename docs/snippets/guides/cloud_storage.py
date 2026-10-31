"""The shop of the Write to Cloud Storage page, built with the builder API.

The shop writes an hour of orders as Parquet and JSON Lines files, as fast as the machine
allows. Run it with `STORAGE=s3 STORAGE_BUCKET=my-bucket python cloud_storage.py`, with the
credentials of that cloud set up as the page describes. cloud_storage.yaml and
cloud_storage_low_level.py are the same shop in YAML and the low-level API. All three
import the store and the sinks of cloud_storage_stores.py, which must be beside them.
"""

# ruff: noqa: E402

# --8<-- [start:shop]
import logging

from cloud_storage_stores import sinks

from desflow import SimulationContext


def build_shop(sinks: list) -> SimulationContext:
    """The shop, writing to the sinks given, as fast as the machine allows."""
    app = (
        SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
    )
    for sink in sinks:
        app.add_egress(sink)

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, entity):
        # The finished event is the order record
        return {"order_id": task_id, "amount": 19.99}

    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

    return app


app = build_shop(sinks)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 hour")
# --8<-- [end:shop]

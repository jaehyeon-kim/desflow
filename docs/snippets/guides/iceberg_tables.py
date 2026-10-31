"""The shop of the Write Iceberg Tables page, built with the builder API.

The shop appends an hour of orders to the Iceberg table shop.events, as fast as the
machine allows. Run it with `python iceberg_tables.py`; rest needs `odctl up catalog`,
postgres needs `odctl up postgres`, and glue needs AWS credentials and ICEBERG_WAREHOUSE.
iceberg_tables.yaml and iceberg_tables_low_level.py are the same shop in YAML and the
low-level API. All three import the catalog and the sink of iceberg_tables_catalog.py, which
must be beside them.
"""

# ruff: noqa: E402

# --8<-- [start:shop]
import logging

from iceberg_tables_catalog import BATCH_SIZE, sink

from desflow import IcebergStorageEgress, SimulationContext


def build_shop(sink: IcebergStorageEgress) -> SimulationContext:
    """The shop, appending to the table of the sink given, as fast as it can."""
    app = (
        SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        .add_egress(sink, batch_size=BATCH_SIZE)
    )

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


app = build_shop(sink)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 hour")
# --8<-- [end:shop]

"""A minute of the shop written to Parquet files, then read back as one table."""

from desflow import ParquetStorageEgress, SimulationContext

app = SimulationContext("Shop", factor=0, random_seed=1)
app.add_resource("packers", current_cap=1, max_cap=1)
app.add_service("packing", dist="constant", value=1.5)
app.add_arrival("orders", dist="exponential", rate=0.5)
# --8<-- [start:write]
# A file name prefix: each batch is written as output/events_<8 hex digits>.parquet
app.add_egress(ParquetStorageEgress(default_path="output/events.parquet"))
# --8<-- [end:write]


@app.task(service_id="packing", resource_id="packers")
def pack(task_id):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def order_loop(context):
    while True:
        entity = yield context.wait_for_arrival("orders")
        context.spawn(pack(context.next_instance("pack"), entity=entity))


# --8<-- [start:read]
def read_events(folder="output"):
    """Every file the sink wrote under `folder`, as one table."""
    import pyarrow.dataset as ds

    # format="json" reads the files of a JsonlStorageEgress the same way
    return ds.dataset(folder, format="parquet").to_table()


# --8<-- [end:read]

if __name__ == "__main__":
    app.run(until=60)
    events = read_events()
    print(events.num_rows, "rows:", ", ".join(events.column_names))

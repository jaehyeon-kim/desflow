"""The shop of the JSON Schema and Protobuf page, built with the builder API.

The shop publishes each packed order as JSON Schema or Protobuf and every other record as
JSON. It needs a Kafka broker and a schema registry: start both with `odctl up kafka-lite`,
then run `python json_schema_and_protobuf.py`. json_schema_and_protobuf.yaml and
json_schema_and_protobuf_low_level.py are the same shop in YAML and the low-level API. All
three import the serializers and the router of json_schema_and_protobuf_serializers.py,
which must be beside them.
"""

# ruff: noqa: E402

# --8<-- [start:shop]
import logging

from json_schema_and_protobuf_serializers import egress

from desflow import KafkaAdminConnector, SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(egress)  # orders as JSON Schema or Protobuf, the rest as JSON
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


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    # With a router, KafkaEgress creates no topics, so create them first
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    topics = ["sim-events", "sim-telemetry", "orders-json", "orders-protobuf"]
    admin.create_topics([{"name": topic} for topic in topics])
    app.run(until="1 min")
# --8<-- [end:shop]

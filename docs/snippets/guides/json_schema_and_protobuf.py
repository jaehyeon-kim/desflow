"""The shop of the Use Schema Serializers page, built with the builder API.

The shop publishes each packed order as JSON Schema or Protobuf and every other record as
JSON. It needs a Kafka broker and a schema registry: start both with `odctl up kafka-lite`,
then run `python json_schema_and_protobuf.py`. json_schema_and_protobuf.yaml and
json_schema_and_protobuf_low_level.py are the same shop in YAML and the low-level API.
The code forms import the serializers of json_schema_and_protobuf_serializers.py, which
must be beside them.
"""

# ruff: noqa: E402

# --8<-- [start:egress]
import logging
import os

from json_schema_and_protobuf_serializers import json_serializer, protobuf_serializer

from desflow import KafkaEgress, SimulationContext

egress = KafkaEgress(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    topics={
        "orders-json": {"serializer": json_serializer},
        "orders-protobuf": {"serializer": protobuf_serializer},
    },
)
# --8<-- [end:egress]

# --8<-- [start:shop]
app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(egress)  # orders as JSON Schema or Protobuf, the rest as JSON
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, entity):
    # Orders alternate between the two formats, so one run shows both
    topic = "orders-protobuf" if task_id % 2 else "orders-json"
    return {"__topic__": topic, "order_id": task_id, "amount": 19.99}


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


# --8<-- [end:shop]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    app.run(until="1 min")

"""The shop of the Use Avro and Pydantic page, built with the builder API.

The shop publishes predictions as Avro and every other record as JSON. It needs a Kafka
broker and a schema registry: start both with `odctl up kafka-lite`, then run
`python avro_and_pydantic.py`. avro_and_pydantic.yaml and avro_and_pydantic_low_level.py
are the same shop in YAML and the low-level API. The code forms import the models and
the serializer of avro_and_pydantic_models.py, which must be beside them.
"""

# ruff: noqa: E402

# --8<-- [start:egress]
import logging
import os

from avro_and_pydantic_models import MLPrediction, generated_serializer

from desflow import KafkaEgress, SimulationContext

egress = KafkaEgress(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    topics={"ml-predictions": {"serializer": generated_serializer}},  # Avro
)
# --8<-- [end:egress]

# --8<-- [start:shop]
app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(egress)  # predictions as Avro, the rest as JSON
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, entity):
    # The finished event is a prediction for the order: the fields of MLPrediction
    prediction = MLPrediction(task_id=entity.key, confidence=0.9)
    return {**prediction.model_dump(), "__topic__": "ml-predictions"}


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


# --8<-- [end:shop]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    app.run(until="1 min")

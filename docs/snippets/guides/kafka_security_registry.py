"""The shop of Use Avro and Pydantic, built with the builder API, with its predictions
serialized through a secured schema registry.

The egress is `registry_egress()` of kafka_security_connections.py, which needs
avro_and_pydantic_models.py of Use Avro and Pydantic beside it. Set the variables of
kafka_security_registry.yaml, then run `python kafka_security_registry.py`. It runs for
one minute.
"""

import logging

from avro_and_pydantic_models import MLPrediction
from kafka_security_connections import registry_egress

from desflow import KafkaEgress, SimulationContext


# --8<-- [start:shop]
def build(egress: KafkaEgress) -> SimulationContext:
    """Builds the shop of Use Avro and Pydantic, with `egress` as its one sink."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        .add_egress(egress)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, entity):
        prediction = MLPrediction(task_id=entity.key, confidence=0.9)
        return {**prediction.model_dump(), "__topic__": "ml-predictions"}

    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build(registry_egress()).run(until="1 min")
# --8<-- [end:shop]

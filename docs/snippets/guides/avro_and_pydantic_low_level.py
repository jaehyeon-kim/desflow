"""The Use Avro and Pydantic guide, low-level API: each packed order publishes a
prediction, sent to ml-predictions as Avro, and every other record goes to its topic
as JSON.

The model and the serializer are those of avro_and_pydantic_models.py, which must be
beside this script. It needs a Kafka broker and a schema registry: start both with
`odctl up kafka-lite`, then run `python avro_and_pydantic_low_level.py`.
"""

import itertools
import logging
import os

from avro_and_pydantic_models import MLPrediction, generated_serializer

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaEgress,
    Sampler,
    SimParameter,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


# --8<-- [start:egress]
egress = KafkaEgress(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    topics={"ml-predictions": {"serializer": generated_serializer}},  # Avro
)
# --8<-- [end:egress]


# --8<-- [start:model]
def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    env.publish_event(
        key,
        {
            "path_id": path_id,
            "sim_id": "Shop",
            "service": "packing",
            "status": "queued",
        },
        "pack_order",
        task_id,
    )
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key,
            {
                "path_id": path_id,
                "sim_id": "Shop",
                "service": "packing",
                "status": "started",
            },
            "pack_order",
            task_id,
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        # The finished event is a prediction for the order: the fields of MLPrediction
        prediction = MLPrediction(task_id=key, confidence=0.9)
        payload = {**prediction.model_dump(), "__topic__": "ml-predictions"}
        env.publish_event(key, payload, "pack_order", task_id)


# --8<-- [end:model]


# --8<-- [start:shop]
def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def shop(sinks: list, factor: float = 0.0) -> DynamicRealtimeEnvironment:
    """The shop, publishing to the sinks given."""
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(sinks)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env


# --8<-- [end:shop]

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    env = shop([egress])  # predictions as Avro, the rest as JSON
    try:
        env.run(until=60)
    finally:
        env.teardown()

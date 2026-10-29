"""The Use Avro, JSON Schema and Protobuf guide, low-level API: each packed order publishes
an order record, sent to orders-json as JSON Schema or to orders-protobuf as Protobuf, and
every other record goes to its topic as JSON.

The serializers and the router are those of json_schema_and_protobuf.py, which must be
beside this script, with shop_record.proto and shop_record_pb2.py. It needs a Kafka broker
and a schema registry: start both with `odctl up kafka-lite`, then run
`python json_schema_and_protobuf_low_level.py`.
"""

import itertools
import logging

from json_schema_and_protobuf import egress

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaAdminConnector,
    Sampler,
    SimParameter,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


# --8<-- [start:shop]
def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        # The finished event is the order record
        order = {"order_id": task_id, "amount": 19.99}
        env.publish_event(key, order, "pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def shop(sinks: list, factor: float = 1.0) -> DynamicRealtimeEnvironment:
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
    # With a router, KafkaEgress creates no topics, so create them first
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    topics = ["sim-events", "sim-telemetry", "orders-json", "orders-protobuf"]
    admin.create_topics([{"name": topic} for topic in topics])
    env = shop([egress])  # orders as JSON Schema or Protobuf, the rest as JSON
    try:
        env.run(until=60)
    finally:
        env.teardown()

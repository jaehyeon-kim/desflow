"""A customer table's changes are written to Kafka, and each delete is a tombstone.

The Low-level form of customer_deletes_become_kafka_tombstones.yaml, with the same
records, built on `DynamicRealtimeEnvironment`. `publish_event(key, None)` publishes a
delete, and a Kafka sink set up with `envelopes=[False]` sends it as a tombstone.

Needs: odctl up kafka-lite and the kafka extra
Run it with: python examples/gallery/customer_deletes_become_kafka_tombstones_low_level.py
"""

import itertools
import os
from datetime import datetime

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    KafkaEgress,
    Router,
    Sampler,
)

SEED = 42
UNTIL = 120
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def build(env):
    """Starts the ten customers and the source of changes on `env`."""
    sampler = Sampler(seed=SEED)
    insert_plan = sampler.generator("tasks.insert_customer.payload.plan")
    update_plan = sampler.generator("tasks.update_customer.payload.plan")
    router = Router(
        {
            "weights": {
                "insert_customer": 40,
                "update_customer": 45,
                "delete_customer": 15,
            }
        },
        rng=sampler.generator("route.change"),
    )
    gap = DistributionConfig(dist="exponential", rate=1)
    gap.stream = "arrival.changes"
    instances = {
        task: itertools.count()
        for task in ("insert_customer", "update_customer", "delete_customer")
    }
    customers = itertools.count()  # the customer ids, as `sequence` counts
    deleted = itertools.count()  # the oldest customer goes first
    inserted = []  # the customer ids of the inserts so far, oldest first

    def insert_customer():
        payload = {
            "op": "insert",
            "customer_id": next(customers),
            "plan": weighted(insert_plan, {"free": 60, "pro": 40}),
        }
        inserted.append(payload["customer_id"])
        env.publish_event(
            str(payload["customer_id"]),
            payload,
            task="insert_customer",
            instance=next(instances["insert_customer"]),
        )

    def update_customer():
        # An update changes the newest customer, as `lookup` with `strategy: last` does
        payload = {
            "op": "update",
            "customer_id": inserted[-1],
            "plan": weighted(update_plan, {"free": 40, "pro": 60}),
        }
        env.publish_event(
            str(payload["customer_id"]),
            payload,
            task="update_customer",
            instance=next(instances["update_customer"]),
        )

    def delete_customer():
        # A value of None is a tombstone for a Kafka sink with the envelope off
        env.publish_event(
            str(next(deleted)),
            None,
            task="delete_customer",
            instance=next(instances["delete_customer"]),
        )

    def changes():
        for _ in itertools.count():
            yield env.timeout(sampler.sample(gap))
            task = router.choose(None, env=env)
            {
                "insert_customer": insert_customer,
                "update_customer": update_customer,
                "delete_customer": delete_customer,
            }[task]()

    for _ in range(10):
        insert_customer()
    env.process(changes())


def run():
    """Runs the model for 120 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress(
        [KafkaEgress(bootstrap_servers=BOOTSTRAP_SERVERS, event_topic="customers-cdc")],
        envelopes=[False],
    )
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()

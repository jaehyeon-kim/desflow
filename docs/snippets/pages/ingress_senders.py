"""Sending one parameter change to each ingress connector, as the Ingress page shows.

tests/docs runs this file. The senders need running services, so they are defined but
not called here; the file checks how a message with the old keys is refused.
"""

import json

from desflow.connectors.ingress.base import parse_change


# --8<-- [start:kafka]
async def send_with_kafka(rate: float) -> None:
    """A JSON object with path and value, to the topic KafkaIngress reads."""
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(bootstrap_servers="127.0.0.1:9092")
    await producer.start()
    try:
        message = {"path": "Shop.arrival.orders.rate", "value": rate}
        await producer.send_and_wait("sim-config", json.dumps(message).encode())
    finally:
        await producer.stop()


# --8<-- [end:kafka]


# --8<-- [start:redis]
async def send_with_redis(rate: float) -> None:
    """The same JSON object, published to the channel RedisIngress subscribes to."""
    import redis.asyncio as redis

    client = redis.from_url("redis://user:password@127.0.0.1:6379/0")
    message = {"path": "Shop.arrival.orders.rate", "value": rate}
    await client.publish("simulation_params", json.dumps(message))
    await client.aclose()


# --8<-- [end:redis]


# --8<-- [start:postgres]
async def send_with_postgres(rate: float) -> None:
    """A row in the table PostgresIngress polls; value is JSON text."""
    import asyncpg

    conn = await asyncpg.connect("postgresql://user:password@127.0.0.1:5432/odctl")
    try:
        await conn.execute(
            "INSERT INTO simulation_params (path, value) VALUES ($1, $2)",
            "Shop.arrival.orders.rate",
            json.dumps(rate),
        )
    finally:
        await conn.close()


# --8<-- [end:postgres]

# --8<-- [start:old-keys]
# Every connector reads a message with the same check; one written for desflow
# 0.20.0 or earlier is refused with a hint
try:
    parse_change({"path_id": "Shop.arrival.orders.rate", "value": 1.0})
except ValueError as error:
    print(f"Skipped a Kafka ingress message: {error}")
# --8<-- [end:old-keys]

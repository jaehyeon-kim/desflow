"""Imports and constructs what the `kafka` extra exists to enable, installed alone."""

import asyncio

from aiokafka import AIOKafkaProducer

from desflow import KafkaAdminConnector, KafkaEgress, KafkaIngress

egress = KafkaEgress(bootstrap_servers="localhost:9092")
KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092")
KafkaAdminConnector(bootstrap_servers="localhost:9092")


# KafkaEgress compresses with lz4 by default, and only building its producer shows the
# extra installed lz4 support: aiokafka checks for it there, with no connection
async def _producer() -> None:
    await AIOKafkaProducer(**egress.producer_config).stop()


asyncio.run(_producer())

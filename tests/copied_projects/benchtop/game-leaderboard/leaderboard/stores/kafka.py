"""The Kafka topics, the schema subject, and the serializer that sends only the event."""

from typing import Any

import httpx
from confluent_kafka.admin import AdminClient
from desflow import KafkaAdminConnector
from desflow.connectors.egress.kafka import ConfluentAvroSerializer

from leaderboard.core.config import (
    BOOTSTRAP_SERVERS,
    CONTROL_TOPIC,
    SCHEMA_REGISTRY,
    SUBJECT,
    TOPIC,
)
from leaderboard.core.models import AVRO_SCHEMA


class ScoreSerializer(ConfluentAvroSerializer):
    """
    Encodes only the score event in Avro, without desflow's envelope.

    desflow sends each event as `{"sim_ts", "timestamp", "key", "value"}`, and
    Flink's source table expects the event's own fields, so this encodes `value` only.
    """

    def __init__(self):
        """Registers the event's schema with the schema registry on first use."""
        super().__init__(SCHEMA_REGISTRY, AVRO_SCHEMA)

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Encodes the event inside a desflow message.

        Args:
            topic (str): The Kafka topic, which names the registry subject.
            data (Any): The desflow message, with the event under `value`.

        Returns:
            bytes: The Avro-encoded event.
        """
        return super().serialize(topic, data["value"])


def create_topics() -> None:
    """Creates the score and control topics, leaving any that exist."""
    admin = KafkaAdminConnector(BOOTSTRAP_SERVERS)
    for topic in (TOPIC, CONTROL_TOPIC):  # one call each: an existing topic ends a call
        admin.create_topics([{"name": topic, "partitions": 1}])


def delete() -> None:
    """Deletes both topics and the schema subject, if they exist."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    existing = admin.list_topics(timeout=10).topics
    for topic in {TOPIC, CONTROL_TOPIC} & set(existing):
        admin.delete_topics([topic])[topic].result()
    # A subject is deleted in two steps: marked deleted, then removed for good.
    subject = f"{SCHEMA_REGISTRY}/subjects/{SUBJECT}"
    if httpx.delete(subject).status_code == 200:
        httpx.delete(subject, params={"permanent": "true"})

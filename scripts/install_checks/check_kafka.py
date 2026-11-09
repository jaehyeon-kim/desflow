"""Imports and constructs what the `kafka` extra exists to enable, installed alone."""

from desflow import KafkaAdminConnector, KafkaEgress, KafkaIngress

KafkaEgress(bootstrap_servers="localhost:9092")
KafkaIngress(topic="sim-config", bootstrap_servers="localhost:9092")
KafkaAdminConnector(bootstrap_servers="localhost:9092")

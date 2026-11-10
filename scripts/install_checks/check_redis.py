"""Imports and constructs what the `redis` extra exists to enable, installed alone."""

from desflow import RedisEgress, RedisIngress

RedisEgress(url="redis://localhost:6379/0")
RedisIngress(url="redis://localhost:6379/0")

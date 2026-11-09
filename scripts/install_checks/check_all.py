"""Imports and constructs what the `all` extra exists to enable, installed alone."""

# all names every other extra, fit and fake included
import importlib.util

for module in (
    "aiokafka",
    "fastavro",
    "fastjsonschema",
    "httpx",
    "boto3",
    "redis",
    "asyncpg",
    "pyarrow",
    "pyiceberg",
    "scipy",
    "matplotlib",
    "jsonschema",
    "google.protobuf",
    "faker",
):
    assert importlib.util.find_spec(module), module

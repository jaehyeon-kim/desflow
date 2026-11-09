"""Imports and constructs what the `parquet` extra exists to enable, installed alone."""

from desflow import JsonlStorageEgress, ParquetStorageEgress

JsonlStorageEgress(default_path="data/events.jsonl")
ParquetStorageEgress(default_path="data/events.parquet")

# Both writers import pyarrow lazily inside their run loops, so
# constructing them does not prove pyarrow is installed.
import pyarrow.parquet
from pyarrow import fs

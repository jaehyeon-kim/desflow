"""Helpers the integration tests share."""

import asyncio
import queue
import uuid
from typing import Any

import orjson
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from desflow.connectors.egress.storage import (
    JsonlStorageEgress,
    ParquetStorageEgress,
    _build_filesystem,
)


async def drain(egress: Any, q: queue.Queue) -> None:
    """Runs the egress until the queue is empty, then cancels it as teardown does.

    The budget is generous because the first commit of a run pays for the namespace,
    the table and a cold S3 connection. A tighter one failed intermittently while
    proving nothing about the connector.
    """
    task = asyncio.create_task(egress.run(q))
    for _ in range(600):
        await asyncio.sleep(0.1)
        if q.empty() and not getattr(egress, "active_tasks", 0):
            break
    else:
        pytest.fail("The egress did not drain the queue within 60 seconds.")
    task.cancel()


def _batch(first: int) -> list[dict]:
    """Three event records, as the environment builds them."""
    return [
        {
            "stream_type": "event",
            "sim_ts": float(number),
            "timestamp": "2026-01-01T00:00:01.000",
            "key": f"task-{number}",
            "value": {"path_id": "Shop.service.packing", "status": "finished"},
        }
        for number in range(first, first + 3)
    ]


def write_and_read_back(filesystem: dict, bucket: str) -> dict:
    """Writes two batches through a Parquet and a JSONL egress, then reads them back.

    The bucket (or container) is created first through the same PyArrow filesystem
    the egresses build from `filesystem`, and the folder of the files is deleted
    afterwards.

    Args:
        filesystem (dict): The `filesystem` mapping of the egresses.
        bucket (str): The bucket or container to write into.

    Returns:
        dict: `parquet`, the rows of every Parquet file, and `jsonl`, the lines of
            every JSON Lines file, each as a list of dicts.
    """
    store = _build_filesystem(filesystem, pafs)
    store.create_dir(bucket)
    folder = f"{bucket}/desflow-{uuid.uuid4().hex[:8]}"
    parquet = ParquetStorageEgress(
        default_path=f"{folder}/events.parquet", filesystem=filesystem
    )
    jsonl = JsonlStorageEgress(
        default_path=f"{folder}/events.jsonl", filesystem=filesystem
    )

    async def write() -> None:
        for egress in (parquet, jsonl):
            q: queue.Queue = queue.Queue()
            q.put(_batch(0))
            q.put(_batch(3))
            await drain(egress, q)

    try:
        asyncio.run(write())
        files = store.get_file_info(pafs.FileSelector(folder))
        parquet_rows: list[dict] = []
        jsonl_rows: list[dict] = []
        for info in files:
            if info.path.endswith(".parquet"):
                parquet_rows += pq.read_table(info.path, filesystem=store).to_pylist()
            elif info.path.endswith(".jsonl"):
                with store.open_input_stream(info.path) as stream:
                    jsonl_rows += [
                        orjson.loads(line) for line in stream.read().splitlines()
                    ]
        return {"parquet": parquet_rows, "jsonl": jsonl_rows}
    finally:
        store.delete_dir_contents(folder, missing_dir_ok=True)


def check_round_trip(rows: dict) -> None:
    """Two batches of three records come back from each sink with their columns."""
    for sink in ("parquet", "jsonl"):
        assert len(rows[sink]) == 6
        assert {"key", "status"} <= set(rows[sink][0])

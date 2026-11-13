"""A sink whose client fails once loses only the records of that write and goes on.

The events of each builder model that `scripts/regression.py capture` runs (the cases
of `test_second_run.py`) are cut into three batches of two records. Each egress sink,
with its client replaced by a fake, fails on the first write of the second batch, and
works after that. The records of the first and third batch must arrive, the two of the
second must be the only ones missing, and the failure must be logged as an error. The
Kafka sink is the exception: it sends the unconfirmed records again, so all arrive. The
fakes and helpers are those of `test_sink_records.py` and `test_connector_second_run.py`.
"""

import asyncio
import functools
import importlib.util
import json
import logging
import queue
import threading
from pathlib import Path
from typing import Any, Callable, List, Set, Tuple
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

from desflow.connectors.egress.iceberg import IcebergStorageEgress
from desflow.connectors.egress.kafka import KafkaEgress
from desflow.connectors.egress.postgres import PostgresEgress
from desflow.connectors.egress.redis import RedisEgress
from desflow.connectors.egress.storage import (
    JsonlStorageEgress,
    ParquetStorageEgress,
)

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, file: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, ROOT / "tests" / "unit" / file)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


records = _load("sink_records_cases", "test_sink_records.py")
second_run = _load("connector_second_run_cases", "test_connector_second_run.py")

MODELS = records.MODELS
IDS = records.IDS
KEPT = {0, 1, 4, 5}
LOST = {2, 3}
real_sleep = asyncio.sleep


@functools.lru_cache(maxsize=None)
def _batches(name: str) -> Tuple[Any, ...]:
    """The model's first six events as three batches of two, each record with a
    distinct `id`, and the model's own route keys removed."""
    case = next(case for case in MODELS if case[0] == name)
    events = records._events(case[1], case[2], case[3])
    batch = records._routed(events, "__none__", ["missing"] * 6)
    for record in batch:
        for route in ("__table__", "__path__", "__stream__", "__topic__"):
            record["value"].pop(route, None)
    return tuple(batch[i : i + 2] for i in (0, 2, 4))


def _record_id(row: Any) -> int:
    """The `id` a test put in a record, whether the sink wrote the record whole or
    only its value."""
    value = row.get("value")
    return int((value if isinstance(value, dict) else row)["id"])


def _fail_on(write: int, counter: List[int], work: Callable[..., Any]) -> Callable:
    """Wraps a client call so the `write`-th call raises, and the others work."""

    def call(*args: Any, **kwargs: Any) -> Any:
        counter.append(1)
        if len(counter) == write:
            raise RuntimeError("the client failed once")
        return work(*args, **kwargs)

    return call


async def _fast_sleep(delay: float) -> None:
    """A sleep that waits at most 10 ms, so a sink's back-off after an error is short."""
    await real_sleep(min(delay, 0.01))


def _drive(egress: Any, batches: Tuple[Any, ...]) -> None:
    """Runs the sink on the batches until its queue is empty and its writes are done."""
    egress_queue: queue.Queue = queue.Queue()
    for batch in batches:
        egress_queue.put(batch)

    async def main() -> None:
        task = asyncio.create_task(egress.run(egress_queue))
        # As teardown does: the queue is empty and no write is running. A write that
        # runs in a daemon thread is not waited for when the loop closes.
        for _ in range(1000):
            if egress_queue.empty() and not getattr(egress, "active_tasks", 0):
                break
            await real_sleep(0.01)
        await real_sleep(0.3)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    with patch("asyncio.sleep", _fast_sleep):
        asyncio.run(main())


def _check(arrived: Set[int], caplog: Any, name: str) -> None:
    assert arrived == KEPT, (
        f"{name}: arrived {sorted(arrived)}, expected {sorted(KEPT)}"
    )
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, f"{name}: the failure was not logged as an error"
    assert "failed once" in caplog.text, name


@pytest.mark.parametrize("kind", ["jsonl", "parquet"])
@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_a_file_sink_loses_only_the_failed_write(
    kind, name, make, until, run, tmp_path, caplog
):
    batches = _batches(name)
    sink_class = JsonlStorageEgress if kind == "jsonl" else ParquetStorageEgress
    sink = sink_class(default_path=str(tmp_path / f"out.{kind}"))
    sink.filesystem = records._local_filesystem()
    opened: List[int] = []
    # The client of a file sink is the filesystem stream (JSON Lines) or the Parquet
    # writer, which writes through the filesystem
    if kind == "jsonl":
        filesystem = sink.filesystem
        stream = _fail_on(2, opened, filesystem.open_output_stream)
        sink.filesystem = MagicMock(wraps=filesystem, open_output_stream=stream)
        with caplog.at_level(logging.WARNING):
            _drive(sink, batches)
    else:
        with (
            patch.object(pq, "write_table", _fail_on(2, opened, pq.write_table)),
            caplog.at_level(logging.WARNING),
        ):
            _drive(sink, batches)
    arrived: Set[int] = set()
    for path in tmp_path.rglob("*"):
        if not path.is_file():
            continue
        if kind == "jsonl":
            arrived |= {_record_id(json.loads(line)) for line in path.open("rb")}
        else:
            arrived |= {_record_id(row) for row in pq.read_table(path).to_pylist()}
    _check(arrived, caplog, name)


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_iceberg_loses_only_the_failed_commit(name, make, until, run, tmp_path, caplog):
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.table import Table

    catalog = SqlCatalog(
        "c", uri=f"sqlite:///{tmp_path}/c.db", warehouse=f"file://{tmp_path}/wh"
    )
    sink = IcebergStorageEgress(catalog=catalog, default_table="sim.t")
    appended: List[int] = []
    failing = _fail_on(2, appended, Table.append)
    with (
        patch.object(Table, "append", failing),
        caplog.at_level(logging.WARNING),
    ):
        _drive(sink, _batches(name))
    table = catalog.load_table("sim.t")
    arrived = {_record_id(row) for row in table.scan().to_arrow().to_pylist()}
    _check(arrived, caplog, name)


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_postgres_loses_only_the_failed_insert(name, make, until, run, caplog):
    fake = second_run.FakeAsyncpg(["id"], threading.Event())
    inserted: List[int] = []
    rows: List[Any] = []
    calls: List[int] = []

    def insert(sql: str, values: list) -> None:
        rows.extend(values)

    work = _fail_on(2, calls, insert)

    async def executemany(self: Any, sql: str, values: list) -> None:
        inserted.append(len(values))
        work(sql, values)

    sink = PostgresEgress("postgresql://u:p@h/db", table_name="t")
    with (
        patch(
            "desflow.connectors.egress.postgres.asyncpg.create_pool", fake.create_pool
        ),
        patch.object(second_run._FakeConn, "executemany", executemany),
        caplog.at_level(logging.WARNING),
    ):
        _drive(sink, _batches(name))
    _check({int(row[0]) for row in rows}, caplog, name)


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_redis_loses_only_the_failed_pipeline(name, make, until, run, caplog):
    arrived: Set[int] = set()
    calls: List[int] = []

    def from_url(*args: Any, **kwargs: Any) -> Any:
        client = MagicMock()

        def pipeline() -> Any:
            pending: List[str] = []
            pipe = MagicMock()
            pipe.xadd.side_effect = lambda target, fields: pending.append(
                fields["payload"]
            )

            def deliver() -> None:
                arrived.update(_record_id(json.loads(p)) for p in pending)

            run_pipeline = _fail_on(2, calls, deliver)

            async def execute() -> None:
                run_pipeline()

            pipe.execute = execute
            return pipe

        async def aclose() -> None:
            return None

        client.pipeline = pipeline
        client.aclose = aclose
        return client

    sink = RedisEgress("redis://h:6379/0", stream_name="s")
    with (
        patch("desflow.connectors.egress.redis.redis.from_url", from_url),
        caplog.at_level(logging.WARNING),
    ):
        _drive(sink, _batches(name))
    _check(arrived, caplog, name)


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_kafka_sends_the_rest_of_a_batch_after_a_failed_send(
    name, make, until, run, caplog
):
    """The Kafka sink keeps the records the broker did not confirm and sends them
    once the producer is back, so every record arrives."""
    arrived: Set[int] = set()
    calls: List[int] = []

    def producer(**config: Any) -> Any:
        client = MagicMock()

        async def noop(*args: Any, **kwargs: Any) -> None:
            return None

        def deliver(topic: str, value: bytes, key: Any) -> None:
            arrived.add(_record_id(json.loads(value)))

        send = _fail_on(3, calls, deliver)

        async def send_message(topic: str, value: bytes, key: Any = None) -> None:
            send(topic, value, key)

        client.start = noop
        client.stop = noop
        client.send = send_message
        return client

    sink = KafkaEgress(bootstrap_servers="b:9092", topic_router=lambda d: "t")
    with (
        patch("desflow.connectors.egress.kafka.AIOKafkaProducer", producer),
        patch.object(KafkaEgress, "_create_topics", lambda self: None),
        caplog.at_level(logging.WARNING),
    ):
        _drive(sink, _batches(name))
    assert arrived == KEPT | LOST, (
        f"{name}: arrived {sorted(arrived)}, expected {sorted(KEPT | LOST)}"
    )
    assert "failed once" in caplog.text, name

import asyncio
import queue
import uuid
from datetime import datetime

import pyarrow as pa
import pytest

from desflow.connectors.egress.iceberg import IcebergStorageEgress


@pytest.fixture
def namespace(iceberg_catalog):
    """A namespace of its own per test, dropped afterwards."""
    name = f"des_test_{uuid.uuid4().hex[:8]}"
    yield name
    for identifier in iceberg_catalog.list_tables(name):
        iceberg_catalog.drop_table(identifier)
    iceberg_catalog.drop_namespace(name)


async def _drain(egress: IcebergStorageEgress, q: queue.Queue) -> None:
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


@pytest.mark.asyncio
@pytest.mark.integration
async def test_snapshot_count_equals_flush_count(iceberg_catalog, namespace):
    """One commit per flush is the behaviour the buffer size exists to control.

    A writer committing per file instead would leave three times the snapshots here,
    and nothing else in the suite would notice.
    """
    identifier = f"{namespace}.events"
    egress = IcebergStorageEgress(catalog=iceberg_catalog, default_table=identifier)
    q: queue.Queue = queue.Queue()

    for flush in range(3):
        q.put([{"key": f"task-{flush}-{i}", "n": i} for i in range(4)])

    await _drain(egress, q)

    table = iceberg_catalog.load_table(identifier)
    assert len(table.metadata.snapshots) == 3
    assert table.scan().to_arrow().num_rows == 12


@pytest.mark.asyncio
@pytest.mark.integration
async def test_explicit_schema_and_location_survive_the_round_trip(
    iceberg_catalog, namespace
):
    """A pinned timestamp column reads back as a timestamp, at the pinned location."""
    identifier = f"{namespace}.events"
    location = f"s3://warehouse/{namespace}/pinned"
    pinned = pa.schema([("key", pa.string()), ("event_time", pa.timestamp("us"))])

    egress = IcebergStorageEgress(
        catalog=iceberg_catalog,
        default_table=identifier,
        schemas={identifier: pinned},
        locations={identifier: location},
    )
    q: queue.Queue = queue.Queue()
    q.put([{"key": "a", "event_time": datetime(2026, 1, 1, 12, 30)}])

    await _drain(egress, q)

    table = iceberg_catalog.load_table(identifier)
    assert table.location() == location

    arrow = table.scan().to_arrow()
    # Inference would have read this as a string, which is the failure the explicit
    # schema exists to prevent.
    assert pa.types.is_timestamp(arrow.schema.field("event_time").type)
    assert arrow.to_pylist()[0]["key"] == "a"


@pytest.mark.asyncio
@pytest.mark.integration
async def test_upsert_updates_rows_and_a_rerun_adds_none(iceberg_catalog, namespace):
    """With upsert_keys a changed key is updated, a new key inserted, and a rerun of the same batch duplicates nothing."""
    identifier = f"{namespace}.orders"
    egress = IcebergStorageEgress(
        catalog=iceberg_catalog,
        default_table=identifier,
        upsert_keys={identifier: ["order_id"]},
    )
    first = [
        {"order_id": 1, "status": "processing"},
        {"order_id": 2, "status": "processing"},
    ]
    second = [
        {"order_id": 1, "status": "shipped"},
        {"order_id": 3, "status": "processing"},
        {"order_id": 1, "status": "delivered"},
    ]
    q: queue.Queue = queue.Queue()
    for batch in (first, second, second):
        q.put(batch)

    await _drain(egress, q)

    rows = iceberg_catalog.load_table(identifier).scan().to_arrow().to_pylist()
    assert sorted((r["order_id"], r["status"]) for r in rows) == [
        (1, "delivered"),
        (2, "processing"),
        (3, "processing"),
    ]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_a_column_a_later_batch_adds_reaches_the_table(
    iceberg_catalog, namespace
):
    """The catalog accepts the new optional column, and earlier rows read null."""
    identifier = f"{namespace}.events"
    egress = IcebergStorageEgress(catalog=iceberg_catalog, default_table=identifier)
    q: queue.Queue = queue.Queue()
    q.put([{"stream_type": "event", "key": "o-1", "value": {"a": 1}}])
    q.put([{"stream_type": "event", "key": "o-2", "value": {"a": 2, "b": "x"}}])

    await _drain(egress, q)

    rows = iceberg_catalog.load_table(identifier).scan().to_arrow().to_pylist()
    assert sorted((r["key"], r["a"], r["b"]) for r in rows) == [
        ("o-1", 1, None),
        ("o-2", 2, "x"),
    ]

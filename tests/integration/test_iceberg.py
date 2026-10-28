import queue
import uuid
from datetime import datetime

import pyarrow as pa
import pytest

from desflow.connectors.egress.iceberg import IcebergStorageEgress
from tests.integration.helpers import drain

pytestmark = [pytest.mark.integration, pytest.mark.storage]


@pytest.fixture
def namespace(iceberg_catalog):
    """A namespace of its own per test, dropped afterwards."""
    name = f"des_test_{uuid.uuid4().hex[:8]}"
    yield name
    for identifier in iceberg_catalog.list_tables(name):
        iceberg_catalog.drop_table(identifier)
    iceberg_catalog.drop_namespace(name)


@pytest.mark.asyncio
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

    await drain(egress, q)

    table = iceberg_catalog.load_table(identifier)
    assert len(table.metadata.snapshots) == 3
    assert table.scan().to_arrow().num_rows == 12


@pytest.mark.asyncio
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

    await drain(egress, q)

    table = iceberg_catalog.load_table(identifier)
    assert table.location() == location

    arrow = table.scan().to_arrow()
    # Inference would have read this as a string, which is the failure the explicit
    # schema exists to prevent.
    assert pa.types.is_timestamp(arrow.schema.field("event_time").type)
    assert arrow.to_pylist()[0]["key"] == "a"


@pytest.mark.asyncio
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

    await drain(egress, q)

    rows = iceberg_catalog.load_table(identifier).scan().to_arrow().to_pylist()
    assert sorted((r["order_id"], r["status"]) for r in rows) == [
        (1, "delivered"),
        (2, "processing"),
        (3, "processing"),
    ]


@pytest.mark.asyncio
async def test_a_column_a_later_batch_adds_reaches_the_table(
    iceberg_catalog, namespace
):
    """The catalog accepts the new optional column, and earlier rows read null."""
    identifier = f"{namespace}.events"
    egress = IcebergStorageEgress(catalog=iceberg_catalog, default_table=identifier)
    q: queue.Queue = queue.Queue()
    q.put([{"stream_type": "event", "key": "o-1", "value": {"a": 1}}])
    q.put([{"stream_type": "event", "key": "o-2", "value": {"a": 2, "b": "x"}}])

    await drain(egress, q)

    rows = iceberg_catalog.load_table(identifier).scan().to_arrow().to_pylist()
    assert sorted((r["key"], r["a"], r["b"]) for r in rows) == [
        ("o-1", 1, None),
        ("o-2", 2, "x"),
    ]


@pytest.mark.asyncio
async def test_a_column_null_in_the_first_batch_waits_for_a_value(
    iceberg_catalog, namespace
):
    """Iceberg has no null type: a task that is None in every row of the first batch
    is left out of the created table, then added when a batch gives it a value."""
    identifier = f"{namespace}.events"
    egress = IcebergStorageEgress(catalog=iceberg_catalog, default_table=identifier)
    q: queue.Queue = queue.Queue()
    q.put([{"stream_type": "event", "key": "o-1", "task": None, "value": {"a": 1}}])
    q.put([{"stream_type": "event", "key": "o-2", "task": "pack", "value": {"a": 2}}])

    await drain(egress, q)

    rows = iceberg_catalog.load_table(identifier).scan().to_arrow().to_pylist()
    assert sorted((r["key"], r["task"]) for r in rows) == [
        ("o-1", None),
        ("o-2", "pack"),
    ]

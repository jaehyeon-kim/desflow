import pytest

# Attempt to import PyArrow, skip tests if not installed
try:
    import pyarrow as pa

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

from desflow.connectors.egress.iceberg import IcebergStorageEgress

pytestmark = pytest.mark.skipif(
    not HAS_PYARROW, reason="pyarrow is required for the Iceberg egress tests"
)


def _has_upsert() -> bool:
    try:
        from pyiceberg.table import Table
    except ImportError:
        return False
    return hasattr(Table, "upsert")


# upsert_keys needs pyiceberg 0.9.0; the iceberg extra's floor is 0.7.0
needs_upsert = pytest.mark.skipif(
    not _has_upsert(), reason="upsert_keys needs pyiceberg 0.9.0 or later"
)


class FakeSchema:
    """Stands in for a pyiceberg Schema, which the connector reads back as Arrow."""

    def __init__(self, arrow_schema):
        self._arrow_schema = arrow_schema

    def as_arrow(self):
        return self._arrow_schema


class FakeTable:
    """Records every append, so a test can count commits and inspect what was sent."""

    def __init__(self, identifier, arrow_schema, location):
        self.identifier = identifier
        self.location = location
        self._schema = FakeSchema(arrow_schema)
        self.appended: list = []
        self.upserted: list = []
        self.unions: list = []

    def schema(self):
        return self._schema

    def append(self, df):
        self.appended.append(df)

    def upsert(self, df, join_cols=None):
        self.upserted.append((df, join_cols))

    def update_schema(self):
        table = self

        class Update:
            def __enter__(self):
                return self

            def union_by_name(self, schema):
                table.unions.append(schema)
                merged = pa.unify_schemas([table._schema.as_arrow(), schema])
                table._schema = FakeSchema(merged)

            def __exit__(self, *exc):
                return False

        return Update()


class FakeCatalog:
    """A pyiceberg catalog reduced to the three calls the connector makes."""

    def __init__(self):
        self.namespaces: list = []
        self.tables: dict = {}
        self.create_calls: list = []

    def create_namespace_if_not_exists(self, namespace):
        self.namespaces.append(namespace)

    def create_table_if_not_exists(self, identifier, schema, location=None):
        self.create_calls.append((identifier, schema, location))
        if identifier not in self.tables:
            self.tables[identifier] = FakeTable(identifier, schema, location)
        return self.tables[identifier]

    def table_exists(self, identifier):
        return identifier in self.tables

    def drop_table(self, identifier):
        self.dropped = getattr(self, "dropped", []) + [identifier]
        del self.tables[identifier]

    def load_table(self, identifier):
        if identifier not in self.tables:
            raise KeyError(f"no table {identifier}")
        return self.tables[identifier]


def test_requires_a_destination():
    """Without a table or a router the connector has nowhere to write."""
    with pytest.raises(ValueError, match="default_table or table_router"):
        IcebergStorageEgress(catalog=FakeCatalog())


def test_identifier_must_name_a_namespace(caplog):
    """A bare table name is not an Iceberg identifier: its records are skipped with a
    warning that says so, and the other tables are written."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="events")

    egress._write_batch([{"key": "a"}], pa)

    assert catalog.tables == {}
    assert "names no namespace" in caplog.text
    assert "1 named a table with no namespace" in caplog.text


def test_router_splits_records_across_tables():
    """Each target table gets its own append, holding only its own records."""
    catalog = FakeCatalog()

    def router(data: dict):
        if data["stream_type"] == "telemetry":
            return "sim.telemetry"
        return "sim.events"

    egress = IcebergStorageEgress(catalog=catalog, table_router=router)
    egress._write_batch(
        [
            {"stream_type": "event", "key": "a"},
            {"stream_type": "telemetry", "key": "b"},
            {"stream_type": "event", "key": "c"},
        ],
        pa,
    )

    events = catalog.tables["sim.events"]
    telemetry = catalog.tables["sim.telemetry"]
    assert [r["key"] for r in events.appended[0].to_pylist()] == ["a", "c"]
    assert [r["key"] for r in telemetry.appended[0].to_pylist()] == ["b"]
    assert catalog.namespaces == ["sim", "sim"]


def test_router_returning_none_drops_the_record():
    """A dropped record reaches no table at all."""
    catalog = FakeCatalog()

    def router(data: dict):
        return None if data["key"] == "drop-me" else "sim.events"

    egress = IcebergStorageEgress(catalog=catalog, table_router=router)
    egress._write_batch([{"key": "drop-me"}, {"key": "keep-me"}], pa)

    rows = catalog.tables["sim.events"].appended[0].to_pylist()
    assert [r["key"] for r in rows] == ["keep-me"]


def test_every_record_dropped_creates_no_table():
    """A batch with nothing to write must not reach the catalog."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, table_router=lambda data: None)

    egress._write_batch([{"key": "a"}, {"key": "b"}], pa)

    assert catalog.create_calls == []
    assert catalog.namespaces == []


def test_one_append_per_batch_is_one_commit_per_flush():
    """Snapshot count follows flush count, which is the reason for the large buffer."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.events")

    for _ in range(3):
        egress._write_batch([{"key": "a"}, {"key": "b"}], pa)

    table = catalog.tables["sim.events"]
    assert len(table.appended) == 3
    assert [t.num_rows for t in table.appended] == [2, 2, 2]


def test_the_table_is_created_once_and_reused():
    """A cached handle keeps a batch to one append rather than a catalog round trip."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.events")

    egress._write_batch([{"key": "a"}], pa)
    egress._write_batch([{"key": "b"}], pa)

    assert len(catalog.create_calls) == 1


def test_an_explicit_schema_overrides_inference():
    """Inference reads sim_ts as an integer here; the pinned schema has to win."""
    catalog = FakeCatalog()
    pinned = pa.schema([("key", pa.string()), ("sim_ts", pa.float64())])
    egress = IcebergStorageEgress(
        catalog=catalog, default_table="sim.events", schemas={"sim.events": pinned}
    )

    egress._write_batch([{"key": "a", "sim_ts": 1}], pa)

    identifier, schema, _ = catalog.create_calls[0]
    assert schema == pinned
    assert catalog.tables["sim.events"].appended[0].schema.field("sim_ts").type == (
        pa.float64()
    )


def test_an_explicit_location_is_passed_to_the_catalog():
    """Pinned so a consumer reading by path is not defeated by a random suffix."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.events",
        locations={"sim.events": "s3://warehouse/pinned/events"},
    )

    egress._write_batch([{"key": "a"}], pa)

    assert catalog.create_calls[0][2] == "s3://warehouse/pinned/events"


def test_later_batches_are_cast_to_the_table_schema():
    """A table that already exists governs, not whatever the next batch infers."""
    catalog = FakeCatalog()
    pinned = pa.schema([("key", pa.string()), ("sim_ts", pa.float64())])
    egress = IcebergStorageEgress(
        catalog=catalog, default_table="sim.events", schemas={"sim.events": pinned}
    )

    egress._write_batch([{"key": "a", "sim_ts": 1.5}], pa)
    # sim_ts arrives as an integer, which inference alone would read as int64.
    egress._write_batch([{"key": "b", "sim_ts": 2}], pa)

    second = catalog.tables["sim.events"].appended[1]
    assert second.schema == pinned
    assert second.to_pylist() == [{"key": "b", "sim_ts": 2.0}]


def test_whole_numbers_are_inferred_as_longs_and_a_later_fraction_is_refused(caplog):
    """A table created from whole numbers has a long column, in a list or a struct
    too, and so has a column a later batch adds, so an integer of any size is written.
    A later fraction is refused with a message that says how to keep fractions."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.orders")
    big = 1767225600123456789  # an epoch time in nanoseconds, above 2**53

    egress._write_batch(
        [
            {
                "stream_type": "event",
                "key": "a",
                "value": {"amount": big, "tags": [1], "box": {"n": 2}, "flag": True},
            }
        ],
        pa,
    )
    egress._write_batch(
        [{"stream_type": "event", "key": "b", "value": {"amount": 12, "later": big}}],
        pa,
    )
    with caplog.at_level("ERROR"):
        egress._write_batch(
            [{"stream_type": "event", "key": "c", "value": {"amount": 12.5}}], pa
        )

    schema = catalog.create_calls[0][1]
    assert schema.field("amount").type == pa.int64()
    assert schema.field("tags").type == pa.list_(pa.int64())
    assert schema.field("box").type == pa.struct([("n", pa.int64())])
    assert schema.field("flag").type == pa.bool_()
    table = catalog.tables["sim.orders"]
    assert table.unions[0].field("later").type == pa.int64()
    assert table.appended[0].to_pylist()[0]["amount"] == big
    assert len(table.appended) == 2
    assert (
        "Name the column double in schemas before the table is created" in caplog.text
    )
    assert "table_policy: drop_and_create" in caplog.text


def test_a_table_without_upsert_keys_is_appended():
    """Without upsert_keys nothing changes: the batch is appended."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.orders")

    egress._write_batch([{"order_id": 1, "status": "new"}], pa)

    table = catalog.tables["sim.orders"]
    assert len(table.appended) == 1 and table.upserted == []


@needs_upsert
def test_a_table_with_upsert_keys_is_upserted_on_those_keys():
    """A table named in upsert_keys is written with upsert, joined on its keys."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.orders",
        upsert_keys={"sim.orders": ["order_id"]},
    )

    egress._write_batch([{"order_id": 1, "status": "new"}], pa)

    table = catalog.tables["sim.orders"]
    assert table.appended == []
    df, join_cols = table.upserted[0]
    assert join_cols == ["order_id"]
    assert df.to_pylist() == [{"order_id": 1, "status": "new"}]


@needs_upsert
def test_a_batch_that_brings_a_column_to_an_upsert_table_is_refused(caplog):
    """PyIceberg's `upsert` cannot update a row written before a column was added, so
    the sink logs one error that names the table and the columns, writes nothing of
    that batch and adds no column. A later batch with the table's columns is written."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.orders",
        upsert_keys={"sim.orders": ["order_id"]},
    )
    egress._write_batch([{"order_id": 1, "qty": 1}], pa)

    with caplog.at_level("ERROR"):
        egress._write_batch([{"order_id": 1, "qty": 2, "big": 3}], pa)
    egress._write_batch([{"order_id": 2, "qty": 5}], pa)

    table = catalog.tables["sim.orders"]
    assert table.unions == []
    assert len(table.upserted) == 2
    assert len(caplog.records) == 1
    assert "sim.orders has upsert_keys" in caplog.text
    assert "the columns 'big' that the table lacks" in caplog.text
    assert "Name every column in schemas and create the table again" in caplog.text


@needs_upsert
def test_upsert_keys_given_as_a_string_are_one_column():
    """`upsert_keys: {sim.orders: order_id}` names one key column, not eight characters."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.orders",
        upsert_keys={"sim.orders": "order_id"},
    )

    egress._write_batch([{"order_id": 1, "status": "new"}], pa)

    assert egress.upsert_keys == {"sim.orders": ["order_id"]}
    assert catalog.tables["sim.orders"].upserted[0][1] == ["order_id"]


@needs_upsert
def test_upsert_keeps_the_last_record_per_key_in_a_flush():
    """Two changes to one key in one flush leave only the later one."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.orders",
        upsert_keys={"sim.orders": ["order_id"]},
    )

    egress._write_batch(
        [
            {"order_id": 1, "status": "new"},
            {"order_id": 2, "status": "new"},
            {"order_id": 1, "status": "shipped"},
        ],
        pa,
    )

    df, _ = catalog.tables["sim.orders"].upserted[0]
    assert sorted(df.to_pylist(), key=lambda r: r["order_id"]) == [
        {"order_id": 1, "status": "shipped"},
        {"order_id": 2, "status": "new"},
    ]


@needs_upsert
def test_upsert_record_without_its_key_is_skipped_with_a_warning(caplog):
    """A record with a null or missing key column cannot be matched, so a keyed entry
    skips it, and one warning at the end of the flush gives the count. The record that
    has its key is still upserted. Before 1.0.0 the first record stopped the whole
    batch with a ValueError."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.orders",
        upsert_keys={"sim.orders": ["order_id"]},
    )

    egress._write_batch(
        [{"status": "new"}, {"order_id": 1, "status": "new"}, {"status": "old"}], pa
    )

    df, join_cols = catalog.tables["sim.orders"].upserted[0]
    assert df.to_pylist() == [{"order_id": 1, "status": "new"}]
    [warning] = [r for r in caplog.records if "skipped records" in r.getMessage()]
    assert "2 had a null or missing key column of their table" in warning.getMessage()


@needs_upsert
def test_upsert_record_with_part_of_a_composite_key_is_skipped_alone(caplog):
    """A record that has some of the key columns but not all of them cannot be matched
    either, and is skipped; the record with the whole key is upserted."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.orders",
        upsert_keys={"sim.orders": ["order_id", "line"]},
    )

    egress._write_batch(
        [
            {"order_id": 1, "status": "new"},
            {"order_id": 2, "line": 1, "status": "new"},
            {"order_id": 3, "line": None, "status": "new"},
        ],
        pa,
    )

    df, _ = catalog.tables["sim.orders"].upserted[0]
    assert df.to_pylist() == [{"order_id": 2, "line": 1, "status": "new"}]
    assert "2 had a null or missing key column" in caplog.text


def test_upsert_keys_on_a_pyiceberg_without_upsert_fails_at_construction(monkeypatch):
    """On pyiceberg older than 0.9.0 the option fails before the run, naming the version."""
    from pyiceberg.table import Table

    monkeypatch.delattr(Table, "upsert", raising=False)
    with pytest.raises(ImportError, match="0.9.0"):
        IcebergStorageEgress(
            catalog=FakeCatalog(),
            default_table="sim.orders",
            upsert_keys={"sim.orders": ["order_id"]},
        )


def test_without_router_events_are_flat_and_telemetry_is_left_out():
    """With only default_table, the table gets one flat row per event."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.events")
    event = {
        "stream_type": "event",
        "key": "task-1",
        "value": {"path_id": "Shop.service.packing", "status": "finished"},
    }

    egress._write_batch(
        [event, {"stream_type": "telemetry", "path_id": "p", "value": 1.0}], pa
    )

    rows = catalog.tables["sim.events"].appended[0].to_pylist()
    assert rows == [
        {
            "stream_type": "event",
            "key": "task-1",
            "path_id": "Shop.service.packing",
            "status": "finished",
        }
    ]
    assert "value" in event


def test_with_router_records_are_not_flattened():
    """A router keeps today's behaviour, so the nested value reaches the table."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, table_router=lambda d: "sim.raw")

    egress._write_batch([{"stream_type": "telemetry", "key": "a", "value": 1.0}], pa)

    rows = catalog.tables["sim.raw"].appended[0].to_pylist()
    assert rows == [{"stream_type": "telemetry", "key": "a", "value": 1.0}]


def test_iso_strings_are_converted_for_time_columns():
    """An ISO string reaches a timestamp or date column as a datetime or date."""
    from datetime import date, datetime

    catalog = FakeCatalog()
    pinned = pa.schema(
        [
            ("key", pa.string()),
            ("timestamp", pa.timestamp("us")),
            ("day", pa.date32()),
            ("label", pa.string()),
        ]
    )
    egress = IcebergStorageEgress(
        catalog=catalog, default_table="sim.events", schemas={"sim.events": pinned}
    )
    record = {
        "key": "a",
        "timestamp": "2026-01-01T12:30:00.500",
        "day": "2026-01-01",
        "label": "2026-01-01T00:00:00",
    }

    egress._write_batch([record], pa)

    [row] = catalog.tables["sim.events"].appended[0].to_pylist()
    assert row["timestamp"] == datetime(2026, 1, 1, 12, 30, 0, 500000)
    assert row["day"] == date(2026, 1, 1)
    # A string column keeps its string.
    assert row["label"] == "2026-01-01T00:00:00"
    # The record other sinks hold is unchanged.
    assert record["timestamp"] == "2026-01-01T12:30:00.500"


def test_catalog_properties_are_loaded_on_first_write(monkeypatch):
    """A mapping is built with PyIceberg's load_catalog, once, when a table is needed."""
    import pyiceberg.catalog

    fake = FakeCatalog()
    calls = []

    def fake_load_catalog(name=None, **properties):
        calls.append((name, properties))
        return fake

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", fake_load_catalog)
    egress = IcebergStorageEgress(
        catalog={
            "name": "odctl",
            "uri": "http://localhost:8181",
            "s3.path-style-access": True,
            "s3.port": 8333,
        },
        default_table="sim.events",
    )
    assert calls == []

    egress._write_batch([{"key": "a"}], pa)
    egress._write_batch([{"key": "b"}], pa)

    assert calls == [
        (
            "odctl",
            {
                "uri": "http://localhost:8181",
                "s3.path-style-access": "true",
                "s3.port": "8333",
            },
        )
    ]
    assert egress.catalog is fake
    assert len(fake.tables["sim.events"].appended) == 2


def test_a_sql_catalog_on_sqlite_writes_one_snapshot_per_flush(tmp_path):
    """A SQL catalog mapping reaches a real PyIceberg catalog; no service is needed."""
    from pyiceberg.catalog import load_catalog

    properties = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{tmp_path}/warehouse",
    }
    egress = IcebergStorageEgress(
        catalog={"name": "local", **properties}, default_table="sim.events"
    )

    for flush in range(3):
        egress._write_batch(
            [{"key": f"task-{flush}-{i}", "n": i} for i in range(4)], pa
        )

    table = load_catalog("local", **properties).load_table("sim.events")
    assert len(table.metadata.snapshots) == 3
    assert table.scan().to_arrow().num_rows == 12


def test_catalog_object_is_used_as_given():
    """A Catalog object keeps working, with no properties recorded."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.events")

    assert egress.catalog is catalog and egress.catalog_properties is None


def test_schema_mapping_of_type_names_becomes_an_arrow_schema():
    """A schema written as column to type name creates the table with those types."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        default_table="sim.events",
        schemas={
            "sim.events": {
                "key": "string",
                "sim_ts": "double",
                "count": "long",
                "ok": "boolean",
                "timestamp": "timestamp",
            }
        },
    )

    egress._write_batch(
        [
            {
                "key": "a",
                "sim_ts": 1,
                "count": 2,
                "ok": True,
                "timestamp": "2026-01-01T00:00:00.000",
            }
        ],
        pa,
    )

    _, schema, _ = catalog.create_calls[0]
    assert schema == pa.schema(
        [
            ("key", pa.string()),
            ("sim_ts", pa.float64()),
            ("count", pa.int64()),
            ("ok", pa.bool_()),
            ("timestamp", pa.timestamp("us")),
        ]
    )


def test_unknown_type_name_is_refused_at_construction():
    """A misspelt type fails before the run, naming the column and the choices."""
    with pytest.raises(ValueError, match="'when' of sim.events.*timestamp"):
        IcebergStorageEgress(
            catalog=FakeCatalog(),
            default_table="sim.events",
            schemas={"sim.events": {"when": "datetime"}},
        )


def test_reserved_keys_are_dropped(caplog):
    """An Iceberg table gets no column for a reserved payload key it does not route by."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.events")
    event = {
        "stream_type": "event",
        "key": "orders-1",
        "value": {"__stream__": "t", "n": 1},
    }

    egress._write_batch([event], pa)

    rows = catalog.tables["sim.events"].appended[0].to_pylist()
    assert rows == [{"stream_type": "event", "key": "orders-1", "n": 1}]
    assert "IcebergStorageEgress drops the payload key '__stream__'" in caplog.text


def test_an_inferred_table_has_every_column_and_gains_new_ones():
    """The created schema has the columns of every row of the first batch, and a
    column that first appears later is added to the table, not dropped."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.orders")
    egress._write_batch(
        [
            {"stream_type": "event", "key": "o-1", "value": {"a": 1}},
            {"stream_type": "event", "key": "o-2", "value": {"a": 2, "b": "x"}},
        ],
        pa,
    )
    table = catalog.tables["sim.orders"]
    assert "b" in table.schema().as_arrow().names
    egress._write_batch(
        [{"stream_type": "event", "key": "o-3", "value": {"a": 3, "c": 1.5}}], pa
    )
    assert len(table.unions) == 1
    assert table.appended[-1].to_pylist()[0]["c"] == 1.5


def test_an_iceberg_write_error_is_logged(caplog):
    """A batch the table refuses is logged at ERROR, and the sink goes on."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.orders")
    egress._write_batch([{"stream_type": "event", "key": "o-1", "value": {"a": 1}}], pa)
    with caplog.at_level("ERROR"):
        egress._write_batch(
            [{"stream_type": "event", "key": "o-2", "value": {"a": "text"}}], pa
        )
    assert any(
        r.levelname == "ERROR" and "sim.orders" in r.getMessage()
        for r in caplog.records
    )


def test_a_low_level_event_without_a_task_still_creates_the_table():
    """A low-level publish_event without task= gives task and instance as None in
    every row; Iceberg has no null type, so the created schema leaves them out and
    the rows are written."""
    from desflow import DynamicRealtimeEnvironment

    catalog = FakeCatalog()
    env = DynamicRealtimeEnvironment(factor=0)
    env.setup_egress(
        [IcebergStorageEgress(catalog=catalog, default_table="sim.events")]
    )

    def orders(env):
        for n in range(3):
            yield env.timeout(1)
            env.publish_event(f"orders-{n + 1}", {"event_type": "order_placed"})

    env.process(orders(env))
    env.run(until=5)
    env.teardown()
    table = catalog.tables["sim.events"]
    schema = table.schema().as_arrow()
    assert not any(pa.types.is_null(field.type) for field in schema)
    assert "task" not in schema.names
    assert sum(df.num_rows for df in table.appended) == 3


def test_the_shut_down_message_counts_only_batches_committed(caplog):
    """A batch the table refuses is not counted among the snapshots committed."""
    import asyncio
    import queue

    egress = IcebergStorageEgress(catalog=FakeCatalog(), default_table="sim.orders")
    q: queue.Queue = queue.Queue()
    for value in (1, "text", 2):
        q.put([{"stream_type": "event", "key": "o", "value": {"a": value}}])

    async def drain():
        task = asyncio.create_task(egress.run(q))
        while not q.empty() or getattr(egress, "active_tasks", 0):
            await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    with caplog.at_level("INFO"):
        asyncio.run(drain())
    assert "Successfully committed 2 total snapshots" in caplog.text


def _event(value, key="orders-1"):
    return {"stream_type": "event", "key": key, "value": value}


def test_table_field_routes_and_is_removed():
    """`__table__` in an event's value is the table it is written to, and the column
    is not written. A record without it goes to `default_table`."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.rest")

    egress._write_batch(
        [
            _event({"__table__": "sim.orders", "n": 1}),
            _event({"__table__": "sim.items", "n": 2}),
            _event({"n": 3}),
        ],
        pa,
    )

    assert sorted(catalog.tables) == ["sim.items", "sim.orders", "sim.rest"]
    assert catalog.tables["sim.orders"].appended[0].to_pylist() == [
        {"stream_type": "event", "key": "orders-1", "n": 1}
    ]
    assert "__table__" not in catalog.tables["sim.items"].appended[0].column_names


def test_table_router_wins_over_the_table_field():
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, table_router=lambda d: "sim.routed")

    egress._write_batch([_event({"__table__": "sim.other", "n": 1})], pa)

    assert list(catalog.tables) == ["sim.routed"]
    assert catalog.tables["sim.routed"].appended[0].column_names == [
        "stream_type",
        "key",
        "value",
    ]


@needs_upsert
def test_an_entry_that_names_its_tables_routes_by_the_table_field_alone(caplog):
    """With tables in `upsert_keys` and no default table, `__table__` is the only
    destination. A record without it is skipped and counted, and each table is
    upserted on its own keys."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        upsert_keys={"air.obs": ["station", "at"], "air.day": ["day"]},
    )

    egress._write_batch(
        [
            _event({"__table__": "air.obs", "station": "s", "at": 1, "pm": 2}),
            _event({"__table__": "air.day", "day": "d1"}),
            _event({"__table__": "air.day", "pm": 9}),
            _event({"pm": 3}),
            {"stream_type": "telemetry", "path_id": "p", "value": 1},
        ],
        pa,
    )

    assert catalog.tables["air.obs"].upserted[0][1] == ["station", "at"]
    assert catalog.tables["air.day"].upserted[0][0].to_pylist() == [
        {"stream_type": "event", "key": "orders-1", "day": "d1"}
    ]
    [warning] = [r for r in caplog.records if "skipped records" in r.getMessage()]
    assert "had no __table__" in warning.getMessage()
    assert "1 had a null or missing key column" in warning.getMessage()


def test_an_entry_without_any_destination_is_refused():
    with pytest.raises(
        ValueError, match="default_table or table_router, or tables named"
    ):
        IcebergStorageEgress(catalog=FakeCatalog())


def test_table_policy_create_makes_the_namespace_and_the_table():
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.t")

    egress._write_batch([_event({"n": 1})], pa)

    assert catalog.namespaces == ["sim"]
    assert egress.table_policy == "create"
    assert len(catalog.create_calls) == 1


def test_table_policy_drop_and_create_drops_an_existing_table_once():
    catalog = FakeCatalog()
    catalog.tables["sim.t"] = FakeTable(
        "sim.t", pa.schema([("old", pa.string())]), None
    )
    egress = IcebergStorageEgress(
        catalog=catalog, default_table="sim.t", table_policy="drop_and_create"
    )

    egress._write_batch([_event({"n": 1})], pa)
    egress._write_batch([_event({"n": 2})], pa)

    assert catalog.dropped == ["sim.t"]
    table = catalog.tables["sim.t"]
    assert "old" not in table.schema().as_arrow().names
    assert len(table.appended) == 2


def test_table_policy_manual_creates_nothing_and_needs_the_table(caplog):
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog, default_table="sim.t", table_policy="manual"
    )

    egress._write_batch([_event({"n": 1})], pa)

    assert catalog.namespaces == [] and catalog.create_calls == []
    assert "no table sim.t" in caplog.text
    catalog.tables["sim.t"] = FakeTable(
        "sim.t", pa.schema([("n", pa.int64()), ("stream_type", pa.string())]), None
    )
    egress._write_batch([_event({"n": 1})], pa)
    assert len(catalog.tables["sim.t"].appended) == 1
    assert catalog.create_calls == []


def test_table_policy_refuses_an_unknown_word():
    with pytest.raises(ValueError, match="table_policy 'always' is not one of"):
        IcebergStorageEgress(
            catalog=FakeCatalog(), default_table="sim.t", table_policy="always"
        )


def test_envelope_false_writes_the_value_as_the_row(caplog):
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.t")
    egress.envelope = False

    egress._write_batch(
        [
            _event({"n": 1}),
            _event("text"),
            _event(None),
            _event({"__table__": "sim.u", "n": 2}),
        ],
        pa,
    )

    assert catalog.tables["sim.t"].appended[0].to_pylist() == [{"n": 1}]
    assert catalog.tables["sim.u"].appended[0].to_pylist() == [{"n": 2}]
    assert "2 had a value that is not a mapping" in caplog.text


@pytest.mark.parametrize(
    "values, rows",
    [
        pytest.param(
            [{"id": 7}, None],
            [
                {"stream_type": "event", "key": "orders-0", "id": 7},
                {"stream_type": "event", "key": "orders-1", "id": None},
            ],
            id="after-a-mapping",
        ),
        pytest.param([None], [{"stream_type": "event", "key": "orders-0"}], id="alone"),
    ],
)
def test_envelope_on_writes_a_null_value_as_the_envelope_columns(
    tmp_path, values, rows
):
    """With `envelope` on, a record whose value is null keeps its envelope columns.
    A table the sink creates has no `value` column, because a column that holds
    only null is not created."""
    from pyiceberg.catalog import load_catalog

    properties = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{tmp_path}/warehouse",
    }
    egress = IcebergStorageEgress(
        catalog={"name": "local", **properties}, default_table="sim.rows"
    )

    egress._write_batch(
        [_event(value, key=f"orders-{i}") for i, value in enumerate(values)], pa
    )

    table = load_catalog("local", **properties).load_table("sim.rows")
    assert table.scan().to_arrow().to_pylist() == rows
    assert "value" not in [field.name for field in table.schema().fields]


def test_envelope_false_leaves_a_label_record_whole():
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, default_table="sim.t")
    egress.envelope = False

    egress._write_batch(
        [{"stream_type": "label", "key": "k", "sim_ts": 1.0, "value": {"fault": "x"}}],
        pa,
    )

    [row] = catalog.tables["sim.t"].appended[0].to_pylist()
    assert row["stream_type"] == "label" and row["fault"] == "x"


def test_a_schemas_only_entry_counts_an_event_with_no_table(caplog):
    """An entry with `schemas` and no default or router has no destination for an
    event without `__table__`; it is counted in the warning, not dropped silently."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog, schemas={"sim.t": pa.schema([("n", pa.int64())])}
    )

    egress._write_batch([_event({"n": 1}), _event({"__table__": "sim.t", "n": 2})], pa)

    assert catalog.tables["sim.t"].appended[0].to_pylist() == [{"n": 2}]
    assert "1 had no __table__" in caplog.text


def _telemetry(value):
    return {"stream_type": "telemetry", "path_id": "tank.level", "value": value}


@pytest.mark.parametrize(
    "batches",
    [[[15], [15.5], [16.25]], [[15, 16, 17], [15.5, 16.25]]],
    ids=["batches of one row", "batches of several rows"],
)
def test_telemetry_value_is_a_double_when_an_integer_comes_first(tmp_path, batches):
    """A statistic that is an integer first, such as a capacity of 15, does not fix
    the column as an integer: a later fraction is kept, not truncated."""
    from pyiceberg.catalog import load_catalog

    properties = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{tmp_path}/warehouse",
    }
    egress = IcebergStorageEgress(
        catalog={"name": "local", **properties}, table_router=lambda d: "sim.telemetry"
    )

    for batch in batches:
        egress._write_batch([_telemetry(v) for v in batch], pa)

    table = load_catalog("local", **properties).load_table("sim.telemetry")
    column = table.scan().to_arrow().column("value")
    assert column.type == pa.float64()
    assert sorted(column.to_pylist()) == sorted(float(v) for b in batches for v in b)


def _preempted(since, nested):
    row = {"status": "preempted", "resource": "m", "usage_since": since}
    if nested:
        return {"stream_type": "event", "key": "a-1", "task": "t", "value": row}
    return {"stream_type": "event", "key": "a-1", "task": "t", **row}


@pytest.mark.parametrize("nested", [False, True], ids=["flat row", "router row"])
def test_usage_since_is_a_double_when_an_integer_comes_first(nested):
    """The time a preempted unit was granted is an integer when the clock is, and
    a later grant at 3.5 is not truncated, flat or inside `value`."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(
        catalog=catalog,
        **(
            {"table_router": lambda d: "sim.e"}
            if nested
            else {"default_table": "sim.e"}
        ),
    )

    egress._write_batch([_preempted(3, nested)], pa)
    egress._write_batch([_preempted(3.5, nested)], pa)

    rows = [r for df in catalog.tables["sim.e"].appended for r in df.to_pylist()]
    found = [(r["value"] if nested else r)["usage_since"] for r in rows]
    assert found == [3.0, 3.5]


def test_the_integers_are_copied_not_changed_in_place():
    """Every sink receives the same record objects, so another sink still reads 15
    and 3."""
    catalog = FakeCatalog()
    egress = IcebergStorageEgress(catalog=catalog, table_router=lambda d: "sim.t")
    record, preempted = _telemetry(15), _preempted(3, True)

    egress._write_batch([record, preempted], pa)

    assert type(record["value"]) is int and record["value"] == 15
    assert type(preempted["value"]["usage_since"]) is int


def test_a_fraction_for_an_integer_column_is_a_lost_batch_not_a_truncation(caplog):
    """A table fixed with an integer column refuses a later 15.5 and logs the batch,
    in a column, a struct and a list; a float with no fraction still goes in."""
    catalog = FakeCatalog()
    pinned = pa.schema(
        [
            ("n", pa.int64()),
            ("s", pa.struct([("a", pa.int64())])),
            ("l", pa.list_(pa.int64())),
        ]
    )
    egress = IcebergStorageEgress(
        catalog=catalog, default_table="sim.t", schemas={"sim.t": pinned}
    )
    egress._write_batch(
        [{"stream_type": "event", "n": 15.0, "s": {"a": 2.0}, "l": [3.0]}], pa
    )
    table = catalog.tables["sim.t"]
    assert table.appended[0].to_pylist() == [{"n": 15, "s": {"a": 2}, "l": [3]}]

    for field, bad in [("n", 15.5), ("s", {"a": 2.5}), ("l", [3, 3.5])]:
        record = {"stream_type": "event", "n": 1, "s": {"a": 1}, "l": [1]}
        record[field] = bad
        with caplog.at_level("ERROR"):
            egress._write_batch([record], pa)
        assert f"column '{field}'" in caplog.text

    assert len(table.appended) == 1
    assert caplog.text.count("could not write 1 rows to sim.t") == 3


READING_RUNS = {
    "steps": "{until: 5, steps: true}",
    "segments": "{until: 5, segments: [{until: 1}, {until: 2.5}, {until: 4}]}",
}
READING_ROWS = {"steps": 26, "segments": 3}


@pytest.mark.parametrize("sink", ["iceberg", "parquet"])
@pytest.mark.parametrize("kind", sorted(READING_RUNS))
def test_run_steps_and_segments_readings_are_written_whole(tmp_path, sink, kind):
    """A real model with `run.steps` or `run.segments`, with its telemetry routed to
    one destination of its own: every reading is written, and `now` and `peek` are
    doubles in the schema, because the first reading is at time 0 or 1 and a later one
    is at a fraction."""
    pq = pytest.importorskip("pyarrow.parquet")
    from desflow import SimulationContext

    path = tmp_path / "model.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "containers: {tank: {level: 15, current_cap: 100, max_cap: 100}}\n"
        "arrivals: {a: {gap: 0.5, max_events: 6, spawn: t}}\n"
        "tasks: {t: {put: {container: tank, amount: 0.5}, payload: {v: 1}}}\n"
        f"run: {READING_RUNS[kind]}\n"
    )
    context = SimulationContext.from_yaml(path)

    def readings(record):
        return record.get("stream_type") == "telemetry"

    if sink == "iceberg":
        properties = {
            "type": "sql",
            "uri": f"sqlite:///{tmp_path}/catalog.db",
            "warehouse": f"file://{tmp_path}/warehouse",
        }
        egress = IcebergStorageEgress(
            catalog={"name": "local", **properties},
            table_router=lambda d: "sim.telemetry" if readings(d) else None,
        )
    else:
        from desflow.connectors.egress.storage import ParquetStorageEgress

        egress = ParquetStorageEgress(
            path_router=lambda d: (
                str(tmp_path / "telemetry" / "t.parquet") if readings(d) else None
            )
        )
    context.add_egress(egress, batch_size=1)
    context.run()

    if sink == "iceberg":
        from pyiceberg.catalog import load_catalog

        table = load_catalog("local", **properties).load_table("sim.telemetry")
        rows = table.scan().to_arrow()
    else:
        rows = pq.read_table(str(tmp_path / "telemetry"))
    value = rows.schema.field("value").type
    assert {value.field("now").type, value.field("peek").type} == {pa.float64()}
    assert rows.num_rows == READING_ROWS[kind]


HINT = "route them to different tables or paths"


def test_the_hint_is_added_only_for_a_mix_of_a_number_and_a_mapping():
    from desflow.connectors.egress.base import write_error

    statistic = {"stream_type": "telemetry", "value": 3}
    reading = {"stream_type": "telemetry", "value": {"now": 1.0}}
    event = {"stream_type": "event", "value": {"a": 1}}
    empty = {"stream_type": "telemetry", "value": None}
    struct = pa.schema([("value", pa.struct([("now", pa.float64())]))])
    number = pa.schema([("value", pa.float64())])
    bad = pa.ArrowInvalid("bad")
    # A conversion error, and the rows of one batch hold both kinds
    mixed = write_error(bad, [statistic, reading])
    assert mixed.startswith("bad. ") and HINT in mixed
    assert "an event or a step or segment reading has a mapping" in mixed
    # An event and a statistic mix as a reading and a statistic do
    assert HINT in write_error(bad, [statistic, event])
    # A number against a struct column, and a mapping against a column that is not
    assert HINT in write_error(bad, [statistic], struct)
    assert HINT in write_error(bad, [reading], number)
    assert HINT in write_error(bad, [event], number)
    # An error that is not a conversion gets none, however mixed the rows are
    assert write_error(PermissionError("denied"), [statistic, reading]) == "denied"
    assert write_error(ValueError("lost"), [statistic, reading], struct) == "lost"
    assert write_error(PermissionError("denied"), [statistic], struct) == "denied"
    # Rows of one kind that agree with the written column, or with no column yet
    assert write_error(bad, [reading], struct) == "bad"
    assert write_error(bad, [statistic], number) == "bad"
    assert write_error(bad, [statistic]) == "bad"
    # A null fits any column, so it counts for neither kind
    assert write_error(bad, [empty, reading]) == "bad"
    assert write_error(bad, [empty], struct) == "bad"
    assert write_error(bad, [empty, statistic], number) == "bad"
    # Rows that carry neither telemetry nor an event
    assert write_error(bad, [{"value": 3}, {"value": {"a": 1}}], struct) == "bad"


def test_a_conversion_error_on_another_field_of_telemetry_gets_no_hint(
    tmp_path, caplog
):
    """Two readings that agree on `value` and differ on another field are refused by
    Arrow, and the logged error does not blame the mix."""
    from pyarrow import fs

    from desflow.connectors.egress.storage import ParquetStorageEgress

    egress = ParquetStorageEgress(path_router=lambda d: str(tmp_path / "t.parquet"))
    egress.filesystem = fs.LocalFileSystem()
    pq = pytest.importorskip("pyarrow.parquet")
    first = {"stream_type": "telemetry", "value": {"now": 1.0}, "extra": {"a": 1}}
    second = {"stream_type": "telemetry", "value": {"now": 2.0}, "extra": 5}
    with caplog.at_level("ERROR"):
        egress._write_batch([first], pa, pq)
        assert caplog.text == ""
        egress._write_batch([second], pa, pq)
    assert "could not write 1 rows" in caplog.text
    assert HINT not in caplog.text


@pytest.mark.parametrize("sink", ["iceberg", "parquet"])
def test_a_statistic_after_a_reading_in_one_destination_is_logged_with_its_cause(
    tmp_path, caplog, sink
):
    """A router that sends both to one table or file loses the statistic's batch, and
    the logged error says why."""
    pq = pytest.importorskip("pyarrow.parquet")
    reading = {
        "stream_type": "telemetry",
        "path": "system.simulation.step",
        "value": {"step": 1, "now": 0.5, "peek": 1.0},
    }
    statistic = {"stream_type": "telemetry", "path": "level", "value": 3}
    if sink == "iceberg":
        egress = IcebergStorageEgress(
            catalog={
                "name": "local",
                "type": "sql",
                "uri": f"sqlite:///{tmp_path}/catalog.db",
                "warehouse": f"file://{tmp_path}/warehouse",
            },
            table_router=lambda d: "sim.telemetry",
        )
        writes = (egress._write_batch, (pa,))
    else:
        from desflow.connectors.egress.storage import ParquetStorageEgress

        egress = ParquetStorageEgress(path_router=lambda d: str(tmp_path / "t.parquet"))
        writes = (egress._write_batch, (pa, pq))
    if sink == "parquet":
        from pyarrow import fs

        egress.filesystem = fs.LocalFileSystem()
    with caplog.at_level("ERROR"):
        writes[0]([reading], *writes[1])
        assert caplog.text == ""
        writes[0]([statistic], *writes[1])
    assert "could not write 1 rows" in caplog.text
    assert HINT in caplog.text


def _hint_sink(sink, tmp_path):
    pq = pytest.importorskip("pyarrow.parquet")
    if sink == "iceberg":
        egress = IcebergStorageEgress(
            catalog={
                "name": "local",
                "type": "sql",
                "uri": f"sqlite:///{tmp_path}/catalog.db",
                "warehouse": f"file://{tmp_path}/warehouse",
            },
            table_router=lambda d: "sim.telemetry",
        )
        return egress, (pa,)
    from pyarrow import fs

    from desflow.connectors.egress.storage import ParquetStorageEgress

    egress = ParquetStorageEgress(path_router=lambda d: str(tmp_path / "t.parquet"))
    egress.filesystem = fs.LocalFileSystem()
    return egress, (pa, pq)


EVENT_ROW = {"stream_type": "event", "path": "Shop.sold", "value": {"qty": 1}}
STATISTIC_ROW = {"stream_type": "telemetry", "path": "level", "value": 3}


@pytest.mark.parametrize("sink", ["iceberg", "parquet"])
def test_a_statistic_after_an_event_is_logged_with_a_hint_that_names_the_event(
    tmp_path, caplog, sink
):
    egress, args = _hint_sink(sink, tmp_path)
    with caplog.at_level("ERROR"):
        egress._write_batch([EVENT_ROW], *args)
        assert caplog.text == ""
        egress._write_batch([STATISTIC_ROW], *args)
    assert "could not write 1 rows" in caplog.text
    assert "an event or a step or segment reading has a mapping" in caplog.text


def test_an_event_and_a_statistic_in_one_batch_are_logged_with_the_hint(
    tmp_path, caplog
):
    egress, args = _hint_sink("iceberg", tmp_path)
    with caplog.at_level("ERROR"):
        egress._write_batch([STATISTIC_ROW, EVENT_ROW], *args)
    assert "could not write 2 rows" in caplog.text
    assert HINT in caplog.text


def test_a_null_value_with_a_conversion_error_on_another_field_gets_no_hint(
    tmp_path, caplog
):
    egress, args = _hint_sink("parquet", tmp_path)
    reading = {"stream_type": "telemetry", "value": {"now": 1.0}, "x": [1]}
    null = {"stream_type": "telemetry", "value": None, "x": 5}
    with caplog.at_level("ERROR"):
        egress._write_batch([reading], *args)
        assert caplog.text == ""
        egress._write_batch([null], *args)
    assert "could not write 1 rows" in caplog.text
    assert HINT not in caplog.text


@pytest.mark.parametrize(
    ("path_id", "doubled"),
    [
        ("system.simulation.step", True),
        ("system.simulation.segment", True),
        ("Shop.my_metric", False),
    ],
)
def test_only_a_step_or_segment_reading_has_its_times_as_doubles(path_id, doubled):
    from desflow.connectors.egress.base import as_doubles

    record = {
        "stream_type": "telemetry",
        "path_id": path_id,
        "value": {"other": 1, "now": 3, "peek": 4},
    }
    value = as_doubles(record)["value"]
    assert value == (
        {"other": 1, "now": 3.0, "peek": 4.0} if doubled else record["value"]
    )
    assert type(value["now"]) is (float if doubled else int)
    assert type(value["other"]) is int


@pytest.mark.asyncio
async def test_the_run_logs_a_batch_it_cannot_write_and_goes_on(
    run_until, caplog, monkeypatch
):
    """A batch whose write raises in the run is logged at ERROR, the next batch is
    written, and no write is left counted as in progress."""
    import queue

    egress = IcebergStorageEgress(catalog=FakeCatalog(), default_table="sim.orders")
    results = [RuntimeError("catalog down"), 1]
    written = []

    def write(batch, pa_module):
        result = results.pop(0)
        if isinstance(result, Exception):
            raise result
        written.append(batch)
        return result

    monkeypatch.setattr(egress, "_write_batch", write)
    q: queue.Queue = queue.Queue()
    q.put([{"a": 1}])
    q.put([{"a": 2}])
    await run_until(egress.run(q), lambda: bool(written))
    assert written == [[{"a": 2}]]
    assert "Iceberg Writer: batch not written: catalog down" in caplog.text
    assert egress.active_tasks == 0


@pytest.mark.asyncio
async def test_a_catalog_setting_pyiceberg_refuses_ends_the_sink(caplog):
    """An unknown catalog type is a setting no wait fixes, so the task ends with
    PyIceberg's error at the start of the run instead of logging every batch as
    not written."""
    import asyncio
    import queue

    egress = IcebergStorageEgress(
        catalog={"type": "sqll", "uri": "sqlite:///unused.db"},
        default_table="shop.orders",
    )
    batches: queue.Queue = queue.Queue()
    batches.put([{"stream_type": "event", "key": "a", "value": {"n": 1}}])

    with pytest.raises(ValueError, match="sqll"):
        await asyncio.wait_for(egress.run(batches), timeout=5)

    assert "could not write" not in caplog.text


@pytest.mark.asyncio
async def test_a_catalog_answer_that_cannot_be_read_does_not_end_the_sink(
    monkeypatch, caplog
):
    """A REST catalog's configuration answer that is not JSON is a `ValueError` from
    `load_catalog`, but a later try may get a good answer, so the sink goes on."""
    import asyncio
    import json
    import queue

    import pyiceberg.catalog

    fake = FakeCatalog()
    attempts = []

    def load_catalog(name=None, **properties):
        attempts.append(1)
        if len(attempts) == 1:
            raise json.JSONDecodeError("Expecting value", "<html>", 0)
        return fake

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", load_catalog)
    egress = IcebergStorageEgress(
        catalog={"uri": "http://localhost:8181"}, default_table="sim.events"
    )
    batches: queue.Queue = queue.Queue()
    batches.put([{"stream_type": "event", "key": "a", "value": {"n": 1}}])

    task = asyncio.create_task(egress.run(batches))
    for _ in range(100):
        if "sim.events" in fake.tables and fake.tables["sim.events"].appended:
            break
        await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert len(attempts) == 2
    assert "could not build the catalog yet: Expecting value" in caplog.text
    assert len(fake.tables["sim.events"].appended) == 1


@pytest.mark.asyncio
async def test_a_catalog_that_does_not_answer_yet_is_built_again_at_the_first_write(
    monkeypatch, caplog
):
    """An error other than a refused setting is logged, and the sink goes on: the first
    write builds the catalog again."""
    import asyncio
    import queue

    import pyiceberg.catalog

    fake = FakeCatalog()
    attempts = []

    def flaky_load_catalog(name=None, **properties):
        attempts.append(1)
        if len(attempts) == 1:
            raise ConnectionError("catalog is down")
        return fake

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", flaky_load_catalog)
    egress = IcebergStorageEgress(
        catalog={"uri": "http://localhost:8181"}, default_table="sim.events"
    )
    batches: queue.Queue = queue.Queue()
    batches.put([{"stream_type": "event", "key": "a", "value": {"n": 1}}])

    task = asyncio.create_task(egress.run(batches))
    for _ in range(100):
        if "sim.events" in fake.tables and fake.tables["sim.events"].appended:
            break
        await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert len(attempts) == 2
    assert "could not build the catalog yet: catalog is down" in caplog.text
    assert len(fake.tables["sim.events"].appended) == 1


def test_a_catalog_that_never_answers_does_not_keep_the_process_alive():
    """The catalog is built in a daemon thread, so a call that never returns does not
    stop the process from ending after the sink is cancelled."""
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent(
        """
        import asyncio, queue, threading
        from desflow.connectors.egress.iceberg import IcebergStorageEgress

        started = threading.Event()

        class Silent(IcebergStorageEgress):
            def _check_catalog(self):
                started.set()
                threading.Event().wait()

        async def main():
            sink = Silent(catalog={"type": "rest", "uri": "http://x"}, default_table="t.e")
            task = asyncio.create_task(sink.run(queue.Queue()))
            # The cancel must come while the call that never returns is running
            assert await asyncio.to_thread(started.wait, 10), "the call did not start"
            task.cancel()
            (result,) = await asyncio.gather(task, return_exceptions=True)
            # The sink ends on a cancel, as itself or as the cancel; a setup error fails
            assert result is None or isinstance(result, asyncio.CancelledError), result

        asyncio.run(main())
        """
    )
    subprocess.run([sys.executable, "-c", code], timeout=30, check=True)

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


def test_identifier_must_name_a_namespace():
    """A bare table name is not an Iceberg identifier, and says so before writing."""
    egress = IcebergStorageEgress(catalog=FakeCatalog(), default_table="events")

    with pytest.raises(ValueError, match="names no namespace"):
        egress._write_batch([{"key": "a"}], pa)


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
    """A record with none of its table's key columns cannot be matched, so a keyed
    entry skips it, and one warning at the end of the flush gives the count. The
    record that has its key is still upserted. Before 1.0.0 the first record stopped
    the whole batch with a ValueError."""
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
    assert "2 had none of the key columns of their table" in warning.getMessage()


@needs_upsert
def test_upsert_record_with_part_of_a_composite_key_still_raises():
    """The skip is for a record with none of the key columns. One that has some of
    them is a mistake in the model, and still stops the batch."""
    egress = IcebergStorageEgress(
        catalog=FakeCatalog(),
        default_table="sim.orders",
        upsert_keys={"sim.orders": ["order_id", "line"]},
    )

    with pytest.raises(ValueError, match="line"):
        egress._write_batch([{"order_id": 1, "status": "new"}], pa)


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
    assert "1 had none of the key columns" in warning.getMessage()


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
        [_event({"n": 1}), _event("text"), _event({"__table__": "sim.u", "n": 2})], pa
    )

    assert catalog.tables["sim.t"].appended[0].to_pylist() == [{"n": 1}]
    assert catalog.tables["sim.u"].appended[0].to_pylist() == [{"n": 2}]
    assert "1 had a value that is not a mapping" in caplog.text


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

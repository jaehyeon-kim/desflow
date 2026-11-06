import queue
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from desflow.connectors.egress.postgres import PostgresEgress


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__.return_value = conn
    pool.acquire.return_value = acquire_ctx
    return pool, conn


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_postgres_egress_run(mock_create_pool, mock_pool, run_until):
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": "id"}, {"column_name": "value"}]
    egress = PostgresEgress("postgresql://user:password@localhost/db", "test_table")
    egress_queue = queue.Queue()
    batch = [
        {"value": {"__table__": "test_table", "id": 1, "value": "a"}},
        {"value": {"__table__": "other_table", "id": 2, "value": "b"}},
        {"value": {"id": 3, "value": "c"}},
    ]
    egress_queue.put(batch)
    await run_until(
        egress.run(egress_queue),
        lambda: conn.executemany.called and egress_queue.empty(),
    )
    mock_create_pool.assert_called_once()
    assert conn.executemany.call_count == 1
    call_args = conn.executemany.call_args[0]
    query, values = call_args[0], call_args[1]
    assert "INSERT INTO test_table" in query
    assert "ON CONFLICT DO NOTHING" in query
    assert len(values) == 2
    assert (1, "a") in values
    assert (3, "c") in values


def test_default_query_skips_conflicts():
    """Without upsert_keys a record whose key exists is skipped, as before."""
    egress = PostgresEgress("postgresql://u:p@localhost/db", "orders")
    assert egress._insert_query(["order_id", "status"]) == (
        "INSERT INTO orders (order_id, status) VALUES ($1, $2) ON CONFLICT DO NOTHING"
    )


def test_upsert_query_updates_the_other_columns():
    """With upsert_keys a conflict on the key updates every other column the batch carries."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "orders", upsert_keys=["order_id"]
    )
    assert egress._insert_query(["order_id", "status", "shipped_at"]) == (
        "INSERT INTO orders (order_id, status, shipped_at) VALUES ($1, $2, $3) "
        "ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status, "
        "shipped_at = EXCLUDED.shipped_at"
    )


def test_upsert_with_only_key_columns_does_nothing_on_conflict():
    """A batch made only of key columns has nothing to update."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "links", upsert_keys=["a", "b"]
    )
    assert egress._insert_query(["a", "b"]).endswith("ON CONFLICT (a, b) DO NOTHING")


def test_upsert_batch_without_the_key_raises():
    """A batch missing a key column cannot be upserted, and says which one."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "orders", upsert_keys=["order_id"]
    )
    with pytest.raises(ValueError, match="order_id"):
        egress._insert_query(["status"])


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_upsert_key_missing_from_the_table_fails_at_start(
    mock_create_pool, mock_pool
):
    """An upsert key the table does not have stops the writer before any batch."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": "id"}, {"column_name": "status"}]
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "orders", upsert_keys=["order_id"]
    )
    with pytest.raises(ValueError, match="order_id"):
        await egress.run(queue.Queue())


def test_records_with_different_columns_are_written_in_separate_runs():
    """A partial record gets its own statement, so it cannot set another record's columns to NULL."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "orders", upsert_keys=["order_id"]
    )
    egress.valid_columns = {"order_id", "status", "total"}
    full_1 = {"order_id": 1, "status": "new", "total": 10.0}
    partial = {"order_id": 1, "status": "shipped"}
    full_2 = {"order_id": 2, "status": "new", "total": 5.0, "sim_ts": 1.0}

    runs = egress._column_runs([full_1, partial, full_2])

    assert [keys for keys, _ in runs] == [
        ["order_id", "status", "total"],
        ["order_id", "status"],
        ["order_id", "status", "total"],
    ]
    assert [len(records) for _, records in runs] == [1, 1, 1]


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_iso_strings_are_converted_for_time_columns(
    mock_create_pool, mock_pool, run_until
):
    """asyncpg rejects a string for a timestamp column, so the writer converts it."""
    from datetime import date, datetime, timezone

    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [
        {"column_name": "order_id", "data_type": "integer"},
        {"column_name": "timestamp", "data_type": "timestamp without time zone"},
        {"column_name": "placed_at", "data_type": "timestamp with time zone"},
        {"column_name": "order_day", "data_type": "date"},
        {"column_name": "note", "data_type": "text"},
    ]
    egress = PostgresEgress("postgresql://u:p@localhost/db", "orders")
    egress_queue = queue.Queue()
    egress_queue.put(
        [
            {
                "timestamp": "2026-01-01T12:00:00.000",
                "value": {
                    "order_id": 1,
                    "placed_at": "2026-01-01T12:00:00+00:00",
                    "order_day": "2026-01-01",
                    "note": "2026-01-01",
                },
            }
        ]
    )
    await run_until(
        egress.run(egress_queue),
        lambda: conn.executemany.called and egress_queue.empty(),
    )

    query, values = conn.executemany.call_args[0]
    row = dict(zip(query.split("(")[1].split(")")[0].split(", "), values[0]))
    assert row["timestamp"] == datetime(2026, 1, 1, 12, 0)
    assert row["placed_at"] == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    assert row["order_day"] == date(2026, 1, 1)
    assert row["note"] == "2026-01-01"


ORDERS = {
    "columns": {"order_id": "INT", "status": "TEXT", "placed_at": "TIMESTAMP"},
    "primary_key": ["order_id"],
}


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_tables_are_created_before_the_columns_are_read(
    mock_create_pool, mock_pool
):
    """Each table in tables is created at start, and then its columns are read."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    order = []
    conn.execute.side_effect = lambda sql: order.append(("execute", sql))
    conn.fetch.side_effect = lambda *a: (
        order.append(("fetch",))
        or [{"column_name": "order_id", "data_type": "integer"}]
    )
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db",
        tables={"orders": ORDERS, "items": {"columns": {"item_id": "INT"}}},
        table_name="orders",
    )

    await egress._init_pool()

    assert order == [
        (
            "execute",
            "CREATE TABLE IF NOT EXISTS orders (order_id INT, status TEXT, "
            "placed_at TIMESTAMP, PRIMARY KEY (order_id))",
        ),
        ("execute", "CREATE TABLE IF NOT EXISTS items (item_id INT)"),
        ("fetch",),
    ]
    assert egress.valid_columns == {"order_id"}


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_a_table_created_by_another_writer_meanwhile_is_ignored(
    mock_create_pool, mock_pool
):
    """Two writers creating one table at once must not stop either of them."""
    import asyncpg

    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.execute.side_effect = asyncpg.exceptions.UniqueViolationError("pg_type")
    conn.fetch.return_value = [{"column_name": "order_id", "data_type": "integer"}]
    egress = PostgresEgress("postgresql://u:p@localhost/db", tables={"orders": ORDERS})

    await egress._init_pool()

    assert egress.valid_columns == {"order_id"}


def test_table_name_defaults_from_tables():
    """One table in tables is the target; none keeps the old default."""
    one = PostgresEgress("postgresql://u:p@localhost/db", tables={"orders": ORDERS})
    none = PostgresEgress("postgresql://u:p@localhost/db")

    assert one.table_name == "orders"
    assert none.table_name == "simulation_data"


def test_several_tables_need_a_table_name():
    """With several tables the writer cannot guess which one it fills."""
    with pytest.raises(ValueError, match="table_name"):
        PostgresEgress(
            "postgresql://u:p@localhost/db",
            tables={"orders": ORDERS, "items": {"columns": {"item_id": "INT"}}},
        )


def test_primary_key_may_be_one_name():
    """A single key column can be written without a list."""
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db",
        tables={"orders": {"columns": {"order_id": "INT"}, "primary_key": "order_id"}},
    )
    assert egress.tables["orders"] == ({"order_id": "INT"}, ["order_id"])


@pytest.mark.parametrize(
    "spec, message",
    [
        ({"order_id": "INT"}, "needs 'columns'"),
        ({"columns": {}}, "at least one column"),
        ({"columns": {"a": "INT"}, "primary_key": ["b"]}, r"\['b'\]"),
        ({"columns": {"a": "INT"}, "indexes": []}, "unknown key"),
    ],
)
def test_malformed_tables_are_refused_at_construction(spec, message):
    """A mistake in tables fails when the egress is built, not at the first batch."""
    with pytest.raises(ValueError, match=message):
        PostgresEgress("postgresql://u:p@localhost/db", tables={"orders": spec})


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_events_carry_task_fields_and_labels_are_json_rows(
    mock_create_pool, mock_pool, run_until
):
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    columns = ["event_id", "task", "instance", "path", "old", "new", "label"]
    conn.fetch.return_value = [{"column_name": name} for name in columns]
    egress = PostgresEgress("postgresql://u:p@localhost/db", "records")
    egress_queue = queue.Queue()
    egress_queue.put(
        [
            {
                "stream_type": "event",
                "key": "orders-1",
                "task": "pack",
                "instance": 0,
                "value": {},
            },
            # A payload field of the same name wins over the record field
            {
                "stream_type": "event",
                "key": "orders-2",
                "task": "pack",
                "instance": 1,
                "value": {"task": "mine"},
            },
            {
                "stream_type": "label",
                "path": "Shop.service.packing.mean",
                "old": 3.0,
                "new": 5.0,
                "label": {"fault": "slow_packing"},
            },
        ]
    )
    await run_until(
        egress.run(egress_queue),
        lambda: conn.executemany.call_count == 3 and egress_queue.empty(),
    )
    rows = []
    for call in conn.executemany.call_args_list:
        query, values = call[0]
        names = query.split("(", 1)[1].split(")", 1)[0].split(", ")
        rows.extend(dict(zip(names, row)) for row in values)
    assert rows == [
        {"event_id": "orders-1", "task": "pack", "instance": 0},
        {"task": "mine", "event_id": "orders-2", "instance": 1},
        {
            "path": "Shop.service.packing.mean",
            "old": "3.0",
            "new": "5.0",
            "label": '{"fault":"slow_packing"}',
        },
    ]


def _event(value, key="orders-1"):
    return {"stream_type": "event", "key": key, "sim_ts": 1.0, "value": value}


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_table_policy_create_makes_the_schema_first(mock_create_pool, mock_pool):
    """`create` runs CREATE SCHEMA IF NOT EXISTS for a qualified table name before
    the table, and reads the columns of that schema's table."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": "order_id", "data_type": "integer"}]
    conn.fetchval.return_value = None
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db",
        tables={
            "shop.orders": {"columns": {"order_id": "INT"}},
            "plain": {"columns": {"a": "INT"}},
        },
        table_name="shop.orders",
    )

    await egress._init_pool()

    assert [call.args[0] for call in conn.execute.call_args_list] == [
        "CREATE SCHEMA IF NOT EXISTS shop",
        "CREATE TABLE IF NOT EXISTS shop.orders (order_id INT)",
        "CREATE TABLE IF NOT EXISTS plain (a INT)",
    ]
    assert conn.fetch.call_args.args[1:] == ("shop", "orders")
    assert egress.valid_columns == {"order_id"}


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_table_policy_create_leaves_an_existing_schema_alone(
    mock_create_pool, mock_pool
):
    """A schema that already exists is not created again, because CREATE SCHEMA IF
    NOT EXISTS needs the CREATE privilege on the database even then."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": "order_id", "data_type": "integer"}]
    conn.fetchval.return_value = 1
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db",
        tables={"shop.orders": {"columns": {"order_id": "INT"}}},
    )

    await egress._init_pool()

    assert conn.fetchval.call_args.args[1] == "shop"
    assert [call.args[0] for call in conn.execute.call_args_list] == [
        "CREATE TABLE IF NOT EXISTS shop.orders (order_id INT)"
    ]


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_table_policy_drop_and_create_drops_the_table_first(
    mock_create_pool, mock_pool
):
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = []
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db",
        tables={"orders": {"columns": {"order_id": "INT"}}},
        table_policy="drop_and_create",
    )

    await egress._init_pool()

    assert [call.args[0] for call in conn.execute.call_args_list] == [
        "DROP TABLE IF EXISTS orders",
        "CREATE TABLE IF NOT EXISTS orders (order_id INT)",
    ]


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_table_policy_manual_runs_no_ddl(mock_create_pool, mock_pool):
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": "order_id", "data_type": "integer"}]
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db",
        tables={"orders": {"columns": {"order_id": "INT"}}},
        table_policy="manual",
    )

    await egress._init_pool()

    conn.execute.assert_not_called()
    assert egress.valid_columns == {"order_id"}


def test_table_policy_refuses_an_unknown_word():
    with pytest.raises(ValueError, match="table_policy 'sometimes' is not one of"):
        PostgresEgress("postgresql://u:p@localhost/db", table_policy="sometimes")


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_keyed_entry_skips_a_record_without_its_key_columns(
    mock_create_pool, mock_pool, run_until, caplog
):
    """A keyed entry skips an event that has none of its key columns, and logs one
    warning for the batch with the count. The events that have the key are upserted,
    and an event for another table is left to that table's entry without a warning.
    Before 1.0.0 the first such event stopped the batch with a ValueError."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": "id"}, {"column_name": "status"}]
    egress = PostgresEgress(
        "postgresql://u:p@localhost/db", "orders", upsert_keys=["id"]
    )
    egress_queue = queue.Queue()
    egress_queue.put(
        [
            _event({"__table__": "orders", "id": 1, "status": "new"}),
            _event({"__table__": "orders", "status": "lost"}),
            _event({"status": "also lost"}),
            _event({"__table__": "users", "name": "x"}),
            _event({"id": 2, "status": "new"}),
        ]
    )

    await run_until(
        egress.run(egress_queue),
        lambda: conn.executemany.called and egress_queue.empty(),
    )

    query, values = conn.executemany.call_args[0]
    assert "ON CONFLICT (id) DO UPDATE" in query
    assert values == [(1, "new"), (2, "new")]
    [warning] = [r for r in caplog.records if "skipped records" in r.getMessage()]
    assert "2 had none of the key columns of their table" in warning.getMessage()


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_envelope_false_writes_the_value_alone(
    mock_create_pool, mock_pool, run_until, caplog
):
    """With `envelope` off the row is the value: no `sim_ts`, `event_id` or `task`
    columns are added, and a value that is not a mapping, null included, is skipped
    with one warning."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [
        {"column_name": name} for name in ("id", "sim_ts", "event_id", "task")
    ]
    egress = PostgresEgress("postgresql://u:p@localhost/db", "orders")
    egress.envelope = False
    egress_queue = queue.Queue()
    egress_queue.put(
        [
            {**_event({"id": 1}), "task": "pack"},
            _event("text"),
            _event(None),
        ]
    )

    await run_until(
        egress.run(egress_queue),
        lambda: conn.executemany.called and egress_queue.empty(),
    )

    query, values = conn.executemany.call_args[0]
    assert query.startswith("INSERT INTO orders (id) VALUES ($1)")
    assert values == [(1,)]
    assert "2 had a value that is not a mapping" in caplog.text


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_envelope_on_skips_an_event_whose_value_is_not_a_mapping(
    mock_create_pool, mock_pool, run_until, caplog
):
    """With `envelope` on, an event whose value is null is skipped with the same
    warning as with `envelope` off; a telemetry reading, a number, is skipped
    without one."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": name} for name in ("id", "event_id")]
    egress = PostgresEgress("postgresql://u:p@localhost/db", "orders")
    egress_queue = queue.Queue()
    egress_queue.put(
        [
            _event({"id": 1}),
            _event(None, key="orders-2"),
            {"stream_type": "telemetry", "path_id": "p", "sim_ts": 1.0, "value": 3},
        ]
    )

    await run_until(
        egress.run(egress_queue),
        lambda: conn.executemany.called and egress_queue.empty(),
    )

    query, values = conn.executemany.call_args[0]
    assert values == [(1, "orders-1")]
    assert "skipped records in this flush: 1 had a value that is not a mapping" in (
        caplog.text
    )


@pytest.mark.asyncio
@patch("desflow.connectors.egress.postgres.asyncpg.create_pool", new_callable=AsyncMock)
async def test_a_batch_that_writes_nothing_or_fails_leaves_the_sink_running(
    mock_create_pool, mock_pool, run_until, caplog
):
    """An empty batch, a batch with no row, and a batch with no column of the table
    write nothing; a batch the database refuses is logged. Each leaves the sink
    going on with the next batch, and no write counted as in progress. A telemetry
    reading that is a mapping carries its `path_id`."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.return_value = [{"column_name": n} for n in ("id", "x", "path_id")]
    conn.executemany.side_effect = [Exception("down"), None]
    egress = PostgresEgress("postgresql://u:p@localhost/db", "orders")
    egress_queue = queue.Queue()
    reading = {"stream_type": "telemetry", "path_id": "p", "sim_ts": 1.0}
    for batch in (
        [],
        [{**reading, "value": 3}],
        [_event({"other": 1})],
        [{**reading, "value": {"x": 1}}],
        [_event({"id": 7})],
    ):
        egress_queue.put(batch)

    await run_until(
        egress.run(egress_queue),
        lambda: conn.executemany.call_count == 2,
    )

    first, second = conn.executemany.call_args_list
    assert first[0][0].startswith("INSERT INTO orders (x, path_id)")
    assert first[0][1] == [(1, "p")]
    assert second[0][1] == [(7,)]
    assert "No matching columns for table orders. Skipping." in caplog.text
    assert "Error inserting batch into orders: down" in caplog.text
    assert egress.active_tasks == 0

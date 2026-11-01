import asyncio
import queue
import pytest
import asyncpg

from desflow.connectors.egress.postgres import PostgresEgress
from desflow.connectors.ingress.postgres import PostgresIngress

pytestmark = [pytest.mark.integration, pytest.mark.services]


async def _fetch(dsn, sql):
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetch(sql)
    finally:
        await conn.close()


async def _run_egress_until(egress, q, done, timeout=15.0):
    """Run the egress until `done()` is true or the timeout passes, then stop it.

    A fixed one-second wait failed on a slow CI runner with half the rows written,
    so the tests poll the table instead.
    """
    task = asyncio.create_task(egress.run(q))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    try:
        while not await done() and loop.time() < deadline:
            await asyncio.sleep(0.1)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_postgres_ingress_loads_historical_state(postgres_container):
    """
    Tests that PostgresIngress correctly executes its Phase 1 startup
    by loading the latest is_applied = TRUE row from history.
    """
    dsn = postgres_container

    # 1. Manually set up the database and history
    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP TABLE IF EXISTS test_sim_params")
    await conn.execute("""
        CREATE TABLE test_sim_params (
            id SERIAL PRIMARY KEY,
            path TEXT,
            value TEXT,
            is_applied BOOLEAN DEFAULT FALSE
        );
    """)

    # Insert multiple historical records for the same path
    await conn.execute("""
        INSERT INTO test_sim_params (path, value, is_applied)
        VALUES
            ('Test.param', '1.0', TRUE),
            ('Test.param', '5.0', TRUE),
            ('Test.param', '10.0', FALSE);
    """)
    await conn.close()

    ingress = PostgresIngress(dsn, table_name="test_sim_params")
    q = queue.Queue()

    # 2. Run the ingress loop in a background task
    task = asyncio.create_task(ingress.run(q))

    # 3. Wait a brief moment for it to process Phase 1 and Phase 2
    await asyncio.sleep(1.0)
    task.cancel()

    items = []
    while not q.empty():
        items.append(q.get_nowait())

    assert len(items) == 2
    assert items[0] == ("Test.param", 5.0)  # Phase 1: Historical load parsed as float
    assert items[1] == ("Test.param", 10.0)  # Phase 2: New update fetch parsed as float

    # Verify it updated the table marking ID 3 as applied
    conn = await asyncpg.connect(dsn)
    is_applied = await conn.fetchval(
        "SELECT is_applied FROM test_sim_params WHERE value = '10.0'"
    )
    assert is_applied is True
    await conn.close()


async def _run_ingress_until(ingress, q, count, timeout=15.0):
    """Run the ingress until the queue holds `count` changes or the timeout passes."""
    task = asyncio.create_task(ingress.run(q))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    try:
        while q.qsize() < count and loop.time() < deadline:
            await asyncio.sleep(0.1)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_postgres_ingress_creates_its_table(postgres_container):
    """The table PostgresIngress creates takes rows with path and value."""
    dsn = postgres_container
    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP TABLE IF EXISTS test_new_params")
    await conn.close()

    ingress = PostgresIngress(dsn, table_name="test_new_params", poll_interval=0.1)
    await ingress._init_pool()  # creates the table, as run() does first
    conn = await asyncpg.connect(dsn)
    await conn.execute(
        "INSERT INTO test_new_params (path, value) VALUES ('Test.param', '3')"
    )
    await conn.close()

    q = queue.Queue()
    await _run_ingress_until(ingress, q, 1)

    assert q.get_nowait() == ("Test.param", 3)


@pytest.mark.asyncio
async def test_postgres_ingress_old_columns_and_rename(postgres_container, caplog):
    """A table with the old columns logs the rename statements, and running them makes it work."""
    dsn = postgres_container
    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP TABLE IF EXISTS test_old_params")
    await conn.execute("""
        CREATE TABLE test_old_params (
            id SERIAL PRIMARY KEY,
            param_path VARCHAR(255) NOT NULL,
            param_value TEXT NOT NULL,
            is_applied BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        INSERT INTO test_old_params (param_path, param_value) VALUES ('Test.param', '7');
    """)
    await conn.close()

    with caplog.at_level("ERROR", logger="desflow.connectors.ingress.postgres"):
        await _run_ingress_until(
            PostgresIngress(dsn, table_name="test_old_params", poll_interval=0.1),
            queue.Queue(),
            count=1,
            timeout=1.0,
        )
    hint = caplog.records[0].getMessage()
    statements = hint[hint.index("ALTER TABLE") :]
    assert statements == (
        "ALTER TABLE test_old_params RENAME COLUMN param_path TO path; "
        "ALTER TABLE test_old_params RENAME COLUMN param_value TO value;"
    )

    conn = await asyncpg.connect(dsn)
    await conn.execute(statements)
    await conn.close()

    q = queue.Queue()
    await _run_ingress_until(
        PostgresIngress(dsn, table_name="test_old_params", poll_interval=0.1), q, 1
    )
    assert q.get_nowait() == ("Test.param", 7)


@pytest.mark.asyncio
async def test_postgres_egress_column_filtering(postgres_container):
    """
    Tests that PostgresEgress successfully filters out extraneous dict keys
    and dynamically inserts into a table.
    """
    dsn = postgres_container

    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP TABLE IF EXISTS test_orders")
    await conn.execute("""
        CREATE TABLE test_orders (
            order_id INT PRIMARY KEY,
            total_amount REAL
        );
    """)
    await conn.close()

    egress = PostgresEgress(dsn, table_name="test_orders")
    q = queue.Queue()

    mock_event = [
        {
            "key": "order-1",
            "sim_ts": 123.4,
            "timestamp": "2026-07-10T12:00:00Z",
            "value": {
                "__table__": "test_orders",
                "order_id": 99,
                "total_amount": 100.50,
                "extra_field": "should_be_ignored",
            },
        }
    ]

    q.put(mock_event)

    async def written():
        return len(await _fetch(dsn, "SELECT * FROM test_orders")) == 1

    await _run_egress_until(egress, q, written)

    rows = await _fetch(dsn, "SELECT * FROM test_orders")

    assert len(rows) == 1
    assert rows[0]["order_id"] == 99
    assert rows[0]["total_amount"] == 100.50


@pytest.mark.asyncio
async def test_postgres_egress_upsert_updates_existing_rows(postgres_container):
    """With upsert_keys, a later record for a key updates the row, and the last one in a flush wins."""
    dsn = postgres_container

    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP TABLE IF EXISTS test_order_status")
    await conn.execute("""
        CREATE TABLE test_order_status (
            order_id INT PRIMARY KEY,
            status TEXT
        );
    """)
    await conn.close()

    egress = PostgresEgress(
        dsn, table_name="test_order_status", upsert_keys=["order_id"]
    )
    q = queue.Queue()
    q.put([{"value": {"order_id": 1, "status": "processing"}}])
    q.put(
        [
            {"value": {"order_id": 1, "status": "shipped"}},
            {"value": {"order_id": 2, "status": "processing"}},
            {"value": {"order_id": 1, "status": "delivered"}},
        ]
    )

    sql = "SELECT * FROM test_order_status ORDER BY order_id"

    async def written():
        rows = await _fetch(dsn, sql)
        return [(r["order_id"], r["status"]) for r in rows] == [
            (1, "delivered"),
            (2, "processing"),
        ]

    await _run_egress_until(egress, q, written)

    rows = await _fetch(dsn, sql)

    assert [(r["order_id"], r["status"]) for r in rows] == [
        (1, "delivered"),
        (2, "processing"),
    ]


@pytest.mark.asyncio
async def test_postgres_egress_partial_upsert_keeps_other_columns(postgres_container):
    """A record carrying only the key and status updates the status and leaves the other columns alone."""
    dsn = postgres_container

    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP TABLE IF EXISTS test_order_totals")
    await conn.execute("""
        CREATE TABLE test_order_totals (
            order_id INT PRIMARY KEY,
            status TEXT,
            total REAL
        );
    """)
    await conn.close()

    egress = PostgresEgress(
        dsn, table_name="test_order_totals", upsert_keys=["order_id"]
    )
    q = queue.Queue()
    q.put(
        [
            {"value": {"order_id": 1, "status": "processing", "total": 10.0}},
            {"value": {"order_id": 1, "status": "shipped"}},
            {"value": {"order_id": 2, "status": "processing", "total": 5.0}},
        ]
    )

    sql = "SELECT * FROM test_order_totals ORDER BY order_id"

    async def written():
        rows = await _fetch(dsn, sql)
        return len(rows) == 2 and rows[0]["status"] == "shipped"

    await _run_egress_until(egress, q, written)

    rows = await _fetch(dsn, sql)

    assert [(r["order_id"], r["status"], r["total"]) for r in rows] == [
        (1, "shipped", 10.0),
        (2, "processing", 5.0),
    ]


@pytest.mark.asyncio
async def test_keyed_entry_skips(postgres_container, caplog):
    """A keyed entry upserts the events that carry its key column and skips the
    others, with a warning that gives the count, instead of losing the batch."""
    import logging

    dsn = postgres_container
    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP TABLE IF EXISTS test_keyed_skip")
    await conn.execute(
        "CREATE TABLE test_keyed_skip (order_id INT PRIMARY KEY, status TEXT)"
    )
    await conn.close()
    egress = PostgresEgress(dsn, table_name="test_keyed_skip", upsert_keys=["order_id"])
    q = queue.Queue()
    q.put(
        [
            {"value": {"order_id": 1, "status": "packed"}},
            {"value": {"status": "queued"}},
            {"value": {"order_id": 1, "status": "shipped"}},
        ]
    )
    sql = "SELECT * FROM test_keyed_skip"

    async def written():
        return [(r["order_id"], r["status"]) for r in await _fetch(dsn, sql)] == [
            (1, "shipped")
        ]

    with caplog.at_level(logging.WARNING):
        await _run_egress_until(egress, q, written)

    assert [(r["order_id"], r["status"]) for r in await _fetch(dsn, sql)] == [
        (1, "shipped")
    ]
    assert "1 had none of the key columns of their table" in caplog.text


@pytest.mark.asyncio
async def test_table_policy_modes_and_schema(postgres_container):
    """`create` makes the schema and the table; `drop_and_create` empties a table that
    holds a row; `manual` creates nothing; and `create` leaves an existing schema
    alone, so a role that may use it but not create schemas still starts."""
    dsn = postgres_container
    conn = await asyncpg.connect(dsn)
    await conn.execute("DROP SCHEMA IF EXISTS b3_shop CASCADE")
    await conn.execute("DROP SCHEMA IF EXISTS b3_limited CASCADE")
    await conn.execute("DROP ROLE IF EXISTS b3_limited_role")
    await conn.close()
    tables = {"b3_shop.orders": {"columns": {"order_id": "INT", "status": "TEXT"}}}
    sql = "SELECT * FROM b3_shop.orders"

    async def run_with(policy, rows, expected, target=dsn, name="b3_shop.orders"):
        egress = PostgresEgress(
            target,
            table_name=name,
            tables=tables if name == "b3_shop.orders" else None,
            table_policy=policy,
        )
        q = queue.Queue()
        q.put([{"value": row} for row in rows])

        async def written():
            # The sink makes the table after it starts, and `drop_and_create` drops
            # it first, so it may not be there at this moment
            try:
                found = await _fetch(dsn, f"SELECT order_id FROM {name}")
            except asyncpg.exceptions.UndefinedTableError:
                return False
            # By its last row, because the row before a drop also makes a count of 1
            ids = [r["order_id"] for r in found]
            return len(ids) == expected and rows[-1]["order_id"] in ids

        await _run_egress_until(egress, q, written)

    await run_with("create", [{"order_id": 1, "status": "a"}], 1)
    await run_with("drop_and_create", [{"order_id": 2, "status": "b"}], 1)
    assert [r["order_id"] for r in await _fetch(dsn, sql)] == [2]
    await run_with("manual", [{"order_id": 3, "status": "c"}], 2)
    assert sorted(r["order_id"] for r in await _fetch(dsn, sql)) == [2, 3]

    # `manual` creates no table: with none there, nothing is written
    absent = await _fetch(dsn, "SELECT to_regclass('b3_shop.not_made') AS found")
    egress = PostgresEgress(dsn, table_name="b3_shop.not_made", table_policy="manual")
    q = queue.Queue()
    q.put([{"value": {"order_id": 1}}])
    task = asyncio.create_task(egress.run(q))
    await asyncio.sleep(3)
    task.cancel()
    assert absent[0]["found"] is None
    assert (await _fetch(dsn, "SELECT to_regclass('b3_shop.not_made') AS found"))[0][
        "found"
    ] is None

    # A role with no CREATE on the database, but CREATE on an existing schema
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("CREATE SCHEMA b3_limited")
        await conn.execute(
            "CREATE ROLE b3_limited_role LOGIN PASSWORD 'limited' NOCREATEDB"
        )
        await conn.execute("REVOKE CREATE ON DATABASE odctl FROM b3_limited_role")
        await conn.execute(
            "GRANT USAGE, CREATE ON SCHEMA b3_limited TO b3_limited_role"
        )
    finally:
        await conn.close()
    limited = dsn.replace("user:password", "b3_limited_role:limited")
    limited_tables = {"b3_limited.orders": {"columns": {"order_id": "INT"}}}
    egress = PostgresEgress(
        limited,
        table_name="b3_limited.orders",
        tables=limited_tables,
    )
    q = queue.Queue()
    q.put([{"value": {"order_id": 9}}])

    async def stored():
        try:
            return len(await _fetch(dsn, "SELECT * FROM b3_limited.orders")) == 1
        except asyncpg.exceptions.UndefinedTableError:
            return False

    await _run_egress_until(egress, q, stored)
    assert [
        r["order_id"] for r in await _fetch(dsn, "SELECT * FROM b3_limited.orders")
    ] == [9]

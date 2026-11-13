import asyncio
import queue
from unittest.mock import AsyncMock, MagicMock, patch
import asyncpg
import pytest
from desflow.connectors.ingress.postgres import PostgresIngress


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__.return_value = conn
    pool.acquire.return_value = acquire_ctx
    pool.close = AsyncMock()
    return pool, conn


def test_postgres_ingress_refuses_a_poll_interval_that_is_not_a_positive_number():
    """The constructor refuses a `poll_interval` of 0. `checked_poll` has its own test, so one
    refused value shows that the constructor calls it."""
    with pytest.raises(
        ValueError, match="poll_interval is a number of seconds above 0"
    ):
        PostgresIngress("postgresql://u:p@localhost/db", poll_interval=0)


@pytest.mark.asyncio
@patch(
    "desflow.connectors.ingress.postgres.asyncpg.create_pool",
    new_callable=AsyncMock,
)
async def test_postgres_ingress_waits_for_a_database_that_is_not_up(
    mock_create_pool, mock_pool, run_until, caplog
):
    """A database that refuses connections when the run starts delays the ingress,
    which then reads the table once the database answers."""
    pool, conn = mock_pool
    mock_create_pool.side_effect = [ConnectionRefusedError("refused"), pool]
    conn.fetch.side_effect = [[], [{"id": 1, "path": "sim.rate", "value": "5.5"}], []]
    ingress = PostgresIngress("postgresql://u:p@localhost/db", poll_interval=0.01)
    ingress_queue = queue.Queue()

    with patch("desflow.connectors._retry.FIRST_DELAY", 0.01):
        await run_until(ingress.run(ingress_queue), lambda: not ingress_queue.empty())

    assert ingress_queue.get_nowait() == ("sim.rate", 5.5)
    assert mock_create_pool.call_count == 2
    assert "PostgresIngress could not connect: refused" in caplog.text


@pytest.mark.parametrize("refused", ["table_name", "certificate"])
@pytest.mark.asyncio
@patch(
    "desflow.connectors.ingress.postgres.asyncpg.create_pool",
    new_callable=AsyncMock,
)
async def test_postgres_ingress_stops_on_an_error_that_no_wait_fixes(
    mock_create_pool, mock_pool, refused
):
    """A table name the server refuses, or a server certificate that does not verify,
    ends the ingress with its error at once, while a refused connection is waited
    out. A failed verification is both an `OSError`, as a refused connection is, and
    a `ValueError`."""
    import ssl

    pool, conn = mock_pool
    if refused == "table_name":
        error: Exception = asyncpg.PostgresSyntaxError('syntax error at "-"')
        mock_create_pool.return_value = pool
        conn.execute.side_effect = error
    else:
        error = ssl.SSLCertVerificationError("certificate verify failed")
        mock_create_pool.side_effect = error
    ingress = PostgresIngress("postgresql://u:p@localhost/db", "bad-name")

    with pytest.raises(type(error)):
        await asyncio.wait_for(ingress.run(queue.Queue()), timeout=2)

    mock_create_pool.assert_called_once()


@pytest.mark.asyncio
@patch(
    "desflow.connectors.ingress.postgres.asyncpg.create_pool",
    new_callable=AsyncMock,
)
async def test_postgres_ingress_run(mock_create_pool, mock_pool):
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.side_effect = [
        [
            {"id": 1, "path": "sim.rate", "value": "5.5"},
            {"id": 2, "path": "sim.enabled", "value": "true"},
            {"id": 3, "path": "sim.string", "value": '"hello"'},
        ],
        [],
    ]
    ingress = PostgresIngress(
        "postgresql://user:password@localhost/db", "params", poll_interval=0.1
    )
    ingress_queue = queue.Queue()
    task = asyncio.create_task(ingress.run(ingress_queue))
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    mock_create_pool.assert_called_once()
    assert conn.execute.call_count >= 1
    assert ingress_queue.qsize() == 3
    assert ingress_queue.get_nowait() == ("sim.rate", 5.5)
    assert ingress_queue.get_nowait() == ("sim.enabled", True)
    assert ingress_queue.get_nowait() == ("sim.string", "hello")


async def _run_briefly(ingress, ingress_queue):
    task = asyncio.create_task(ingress.run(ingress_queue))
    await asyncio.sleep(0.15)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
@patch(
    "desflow.connectors.ingress.postgres.asyncpg.create_pool",
    new_callable=AsyncMock,
)
async def test_postgres_ingress_logs_skips_and_marks_bad_rows(
    mock_create_pool, mock_pool, caplog
):
    """A row with a null path or value is logged, skipped and marked applied, so it is read once."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.side_effect = [
        [],
        [
            {"id": 1, "path": None, "value": "5"},
            {"id": 2, "path": "sim.rate", "value": "null"},
            {"id": 3, "path": "sim.rate", "value": "6"},
        ],
        [],
    ]
    ingress = PostgresIngress("postgresql://localhost/db", "params", poll_interval=0.1)
    ingress_queue = queue.Queue()

    with caplog.at_level("WARNING", logger="desflow.connectors.ingress.postgres"):
        await _run_briefly(ingress, ingress_queue)

    assert ingress_queue.get_nowait() == ("sim.rate", 6)
    assert ingress_queue.empty()
    skipped = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(skipped) == 2
    assert all(m.startswith("Skipped a PostgreSQL ingress row") for m in skipped)
    update = [c for c in conn.execute.call_args_list if "UPDATE" in c.args[0]]
    assert update[0].args[1] == [1, 2, 3]


@pytest.mark.asyncio
@patch(
    "desflow.connectors.ingress.postgres.asyncpg.create_pool",
    new_callable=AsyncMock,
)
async def test_postgres_ingress_names_the_column_rename(
    mock_create_pool, mock_pool, caplog
):
    """A table with the old column names gets an error line with the statements that rename them."""
    pool, conn = mock_pool
    mock_create_pool.return_value = pool
    conn.fetch.side_effect = asyncpg.UndefinedColumnError(
        'column "path" does not exist'
    )
    ingress = PostgresIngress("postgresql://localhost/db", "params", poll_interval=0.1)

    with caplog.at_level("ERROR", logger="desflow.connectors.ingress.postgres"):
        await _run_briefly(ingress, queue.Queue())

    assert caplog.records
    assert all(
        "ALTER TABLE params RENAME COLUMN param_path TO path;" in r.getMessage()
        and "ALTER TABLE params RENAME COLUMN param_value TO value;" in r.getMessage()
        for r in caplog.records
    )

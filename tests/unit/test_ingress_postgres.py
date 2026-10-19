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
    return pool, conn


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

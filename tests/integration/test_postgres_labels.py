"""Ground-truth records written by PostgresEgress to a real database.

The row's `old`, `new` and `label` are JSON text, which only a server can show a JSONB
column accepts.
"""

import asyncio
import queue

import asyncpg
import pytest

from desflow.connectors.egress.postgres import PostgresEgress

pytestmark = [pytest.mark.integration, pytest.mark.services]


@pytest.mark.asyncio
async def test_labels_are_written_to_jsonb_columns(postgres_container):
    dsn = postgres_container
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("DROP TABLE IF EXISTS sim_labels")
    finally:
        await conn.close()

    egress = PostgresEgress(
        dsn,
        tables={
            "sim_labels": {
                "columns": {
                    "sim_ts": "DOUBLE PRECISION",
                    "timestamp": "TIMESTAMP",
                    "path": "TEXT",
                    "old": "JSONB",
                    "new": "JSONB",
                    "label": "JSONB",
                }
            }
        },
    )
    records = queue.Queue()
    records.put(
        [
            {
                "stream_type": "label",
                "sim_ts": 720.0,
                "timestamp": "2026-10-16T10:12:00.000",
                "path": "Shop.service.packing.scale",
                "old": 1.0,
                "new": 1.2,
                "label": {"fault": "slow_packing"},
            },
            {
                "stream_type": "label",
                "sim_ts": 800.0,
                "timestamp": "2026-10-16T10:13:20.000",
                "path": None,
                "old": None,
                "new": None,
                "label": {"note": "checked"},
            },
        ]
    )

    async def rows():
        conn = await asyncpg.connect(dsn)
        try:
            return await conn.fetch(
                "SELECT sim_ts, path, old::text AS old, new::text AS new, "
                "label->>'fault' AS fault, label->>'note' AS note "
                "FROM sim_labels ORDER BY sim_ts"
            )
        except asyncpg.exceptions.UndefinedTableError:
            return []
        finally:
            await conn.close()

    task = asyncio.create_task(egress.run(records))
    try:
        for _ in range(150):
            if len(await rows()) == 2:
                break
            await asyncio.sleep(0.1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if egress.pool is not None:
            await egress.pool.close()

    found = [dict(row) for row in await rows()]
    assert found == [
        {
            "sim_ts": 720.0,
            "path": "Shop.service.packing.scale",
            "old": "1.0",
            "new": "1.2",
            "fault": "slow_packing",
            "note": None,
        },
        # JSON null, not SQL NULL, for a step with only a record
        {
            "sim_ts": 800.0,
            "path": None,
            "old": "null",
            "new": "null",
            "fault": None,
            "note": "checked",
        },
    ]

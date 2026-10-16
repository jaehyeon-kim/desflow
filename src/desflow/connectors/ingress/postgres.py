import asyncio
import json
import logging
import queue
from typing import Any, Mapping, Tuple

import asyncpg

from desflow.connectors.ingress.base import BaseIngress, parse_change

logger = logging.getLogger(__name__)

# Logged with a missing-column error: a table created by desflow 0.20.0 or
# earlier still has the old column names.
_RENAME_HINT = (
    "; desflow 0.21.0 renamed the columns param_path and param_value to path and"
    " value: ALTER TABLE {table} RENAME COLUMN param_path TO path; ALTER TABLE"
    " {table} RENAME COLUMN param_value TO value;"
)


def _row_change(row: Mapping[str, Any]) -> Tuple[str, Any]:
    """Reads the change in a row, parsing `value` as JSON and keeping the text when it is not JSON."""
    raw_val = row["value"]
    try:
        # Parse JSON to keep types (e.g. ints, dicts)
        val = json.loads(raw_val)
    except (json.JSONDecodeError, TypeError):
        val = raw_val
    return parse_change({"path": row["path"], "value": val})


class PostgresIngress(BaseIngress):
    """
    Asynchronous ingress provider for polling updates from a PostgreSQL database.

    This connector polls a specified configuration table for rows where 'is_applied'
    is FALSE. It parses the parameter path and value, forwards them to the
    simulation registry, and then updates the row to mark it as applied. A row
    with a null path or value is logged as a warning, skipped and marked as
    applied, so it is not read again.
    """

    def __init__(
        self,
        connection_dsn: str,
        table_name: str = "simulation_params",
        poll_interval: float = 2.0,
        **kwargs: Any,
    ):
        """
        Initializes the PostgresIngress.

        Args:
            connection_dsn: PostgreSQL connection string (DSN).
            table_name: Table to poll for parameter updates. Must have columns:
                        id (serial), path (text), value (text/json), is_applied (boolean).
            poll_interval: Seconds to wait between database polls.
            **kwargs: Additional connection pool arguments for asyncpg.
        """
        self.dsn = connection_dsn
        self.table_name = table_name
        self.poll_interval = poll_interval
        self.kwargs = kwargs
        self.pool: asyncpg.Pool | None = None

    async def _init_pool(self) -> None:
        """Initializes the asyncpg connection pool if not already created."""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(self.dsn, **self.kwargs)

            # Ensure the table exists
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id SERIAL PRIMARY KEY,
                        path VARCHAR(255) NOT NULL,
                        value TEXT NOT NULL,
                        is_applied BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)

    def _describe(self, error: Exception) -> str:
        """Returns the error text, with the column rename statements for a missing column."""
        if isinstance(error, asyncpg.UndefinedColumnError):
            return f"{error}{_RENAME_HINT.format(table=self.table_name)}"
        return str(error)

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Main execution loop fetching dynamic parameter updates from PostgreSQL.

        Args:
            ingress_queue: A thread-safe queue used to transmit (path, value)
                tuples to the SimulationRegistry.
        """
        await self._init_pool()

        # Phase 1: Load the most recently applied state, so a restart resumes from it
        try:
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                # Get the latest applied value for each distinct path
                rows = await conn.fetch(f"""
                    SELECT DISTINCT ON (path) path, value
                    FROM {self.table_name}
                    WHERE is_applied = TRUE
                    ORDER BY path, id DESC
                """)
                for row in rows:
                    try:
                        path, val = _row_change(row)
                    except ValueError as e:
                        logger.warning(f"Skipped a PostgreSQL ingress row: {e}")
                        continue
                    ingress_queue.put((path, val))
                    logger.info(f"Loaded historical parameter state: {path} = {val}")
        except Exception as e:
            logger.error(
                f"Error loading historical state from {self.table_name}: "
                f"{self._describe(e)}"
            )

        # Phase 2: Continuously poll for new unapplied updates
        while True:
            try:
                assert self.pool is not None
                async with self.pool.acquire() as conn:
                    # Fetch all unapplied parameter updates
                    rows = await conn.fetch(f"""
                        SELECT id, path, value
                        FROM {self.table_name}
                        WHERE is_applied = FALSE
                        ORDER BY id ASC
                    """)

                    if rows:
                        applied_ids = []
                        for row in rows:
                            # A bad row is marked applied too, so it is skipped once
                            applied_ids.append(row["id"])
                            try:
                                path, val = _row_change(row)
                            except ValueError as e:
                                logger.warning(
                                    f"Skipped a PostgreSQL ingress row {row['id']}: {e}"
                                )
                                continue

                            # Send to the simulation registry queue
                            ingress_queue.put((path, val))
                            logger.info(f"Ingested parameter update: {path} = {val}")

                        # Mark the rows as applied so we don't process them again
                        await conn.execute(
                            f"""
                            UPDATE {self.table_name}
                            SET is_applied = TRUE
                            WHERE id = ANY($1::int[])
                        """,
                            applied_ids,
                        )

            except Exception as e:
                logger.error(
                    f"Error polling PostgresIngress table {self.table_name}: "
                    f"{self._describe(e)}"
                )

            # Wait before polling again
            await asyncio.sleep(self.poll_interval)

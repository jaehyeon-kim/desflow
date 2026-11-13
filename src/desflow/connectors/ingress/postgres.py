import asyncio
import json
import logging
import queue
from typing import Any, List, Mapping, Tuple

import asyncpg

from desflow.connectors._checks import checked_poll
from desflow.connectors._retry import connect_with_retry, is_temporary_postgres_error
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
        self.poll_interval = checked_poll(poll_interval, "poll_interval")
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

        If PostgreSQL does not answer at start, the ingress retries with a back-off
        that grows to one minute, as the Kafka ingress does. An error that no wait
        fixes, such as a `table_name` the server refuses, or a role that may read the table
        but not update it, ends the task at once. The rows of a poll are marked applied
        before their changes are sent, so an update that fails sends nothing. When the
        connection fails during that update, the next poll asks which rows the server
        marked and sends their changes.

        Args:
            ingress_queue: A thread-safe queue used to transmit (path, value)
                tuples to the SimulationRegistry.

        Raises:
            asyncpg.PostgresError: If the server refuses a setting, such as the
                password, `table_name` or the role's privilege to update the table.
        """
        try:
            await connect_with_retry(
                self._init_pool,
                self._close_pool,
                "PostgresIngress",
                is_temporary_postgres_error,
            )
            await self._ingest(ingress_queue)
        finally:
            await self._close_pool()

    async def _close_pool(self) -> None:
        """Closes the pool and forgets it, so the next run makes its own in its own
        event loop."""
        pool, self.pool = self.pool, None
        if pool is not None:
            await pool.close()

    @staticmethod
    def _send(ingress_queue: queue.Queue, changes: List[Tuple[str, Any]]) -> None:
        """Puts changes on the queue, in order, and logs each."""
        for path, val in changes:
            ingress_queue.put((path, val))
            logger.info(f"Ingested parameter update: {path} = {val}")

    async def _ingest(self, ingress_queue: queue.Queue) -> None:
        """Loads the applied history, then polls for new rows until it is cancelled."""
        # Phase 1: Load the most recently applied state, so a restart resumes from it
        try:
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                # Get the latest applied value for each distinct path, in the order
                # the changes were made: a `current_cap` made after a `max_cap` is
                # clamped by the new maximum only if it is applied after it
                rows = await conn.fetch(f"""
                    SELECT path, value FROM (
                        SELECT DISTINCT ON (path) id, path, value
                        FROM {self.table_name}
                        WHERE is_applied = TRUE
                        ORDER BY path, id DESC
                    ) latest
                    ORDER BY id
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
        # The changes of rows whose marking failed with no answer, as a lost
        # connection gives: the server may have committed it, and then the next poll
        # no longer reads those rows
        pending: List[Tuple[int, Tuple[str, Any]]] = []
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

                    if pending:
                        # A pending row the read above does not return was marked
                        # by the server, whose commit came late, before that read.
                        # The check comes after the read, so a row it finds marked
                        # is sent before the newer rows of this poll. A pending row
                        # the read returns is marked again below. A row that is
                        # neither returned nor marked is gone from the table.
                        read_ids = {r["id"] for r in rows}
                        unread = [p for p in pending if p[0] not in read_ids]
                        if unread:
                            marked = await conn.fetch(
                                f"""
                                SELECT id FROM {self.table_name}
                                WHERE id = ANY($1::bigint[]) AND is_applied = TRUE
                            """,
                                [row_id for row_id, _ in unread],
                            )
                            committed = {row["id"] for row in marked}
                            recovered = [
                                c for row_id, c in unread if row_id in committed
                            ]
                            pending = [p for p in pending if p not in unread]
                            self._send(ingress_queue, recovered)

                    if rows:
                        applied_ids = []
                        changes = []
                        for row in rows:
                            # A bad row is marked applied too, so it is skipped once
                            applied_ids.append(row["id"])
                            try:
                                changes.append((row["id"], _row_change(row)))
                            except ValueError as e:
                                logger.warning(
                                    f"Skipped a PostgreSQL ingress row {row['id']}: {e}"
                                )

                        # Mark the rows as applied before the changes are sent, so a
                        # failed update sends nothing and the next poll reads the
                        # rows again, instead of sending the same changes at every poll
                        # A pending id the poll reads again is sent from here, so it
                        # leaves the pending list
                        unrecovered = [p for p in pending if p[0] not in applied_ids]
                        pending = unrecovered + changes
                        await conn.execute(
                            f"""
                            UPDATE {self.table_name}
                            SET is_applied = TRUE
                            WHERE id = ANY($1::bigint[])
                        """,
                            applied_ids,
                        )
                        pending = unrecovered

                        # Send to the simulation registry queue
                        self._send(ingress_queue, [change for _, change in changes])

            except asyncpg.InsufficientPrivilegeError:
                # A role that may not update the table is a setting, which no wait
                # fixes
                raise
            except Exception as e:
                logger.error(
                    f"Error polling PostgresIngress table {self.table_name}: "
                    f"{self._describe(e)}"
                )

            # Wait before polling again
            await asyncio.sleep(self.poll_interval)

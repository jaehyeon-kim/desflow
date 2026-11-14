import asyncio
import logging
import queue
import weakref
from collections import Counter
from collections.abc import Mapping
from typing import Any

import asyncpg
import orjson

from desflow.connectors._postgres import ensure_schema
from desflow.connectors._postgres import fold as _fold
from desflow.connectors._retry import connect_with_retry, is_temporary_postgres_error
from desflow.connectors.egress.base import (
    BaseEgress,
    keep_keyed,
    parse_iso_columns,
    warn_skipped,
)

logger = logging.getLogger(__name__)

TABLE_POLICIES = ("create", "drop_and_create", "manual")

# information_schema data types whose columns take a Python datetime or date.
# asyncpg rejects an ISO string for these, so such strings are converted first.
_TIME_TYPES = {
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
    "date": "date",
}


def _table_spec(name: str, spec: Any) -> tuple[dict[str, str], list[str]]:
    """
    Reads one entry of `tables` into its columns and primary key.

    Args:
        name (str): The table name, used in errors.
        spec (Any): A mapping with `columns` (column name to SQL type) and an
            optional `primary_key` (a column name or a list of them).

    Returns:
        tuple[dict[str, str], list[str]]: The columns and the primary key columns.

    Raises:
        ValueError: If the entry has no columns, has an unknown key, or names a
            primary key column it does not define.
    """
    if not isinstance(spec, Mapping) or not isinstance(spec.get("columns"), Mapping):
        raise ValueError(
            f"Table '{name}' needs 'columns', a mapping of column name to SQL type"
        )
    unknown = set(spec) - {"columns", "primary_key"}
    if unknown:
        raise ValueError(
            f"Table '{name}' has unknown key(s) {sorted(unknown)}; "
            "the keys are columns and primary_key"
        )
    columns = {str(column): str(sql) for column, sql in spec["columns"].items()}
    if not columns:
        raise ValueError(f"Table '{name}' needs at least one column")
    key = spec.get("primary_key") or []
    primary_key = [key] if isinstance(key, str) else [str(k) for k in key]
    missing = [k for k in primary_key if k not in columns]
    if missing:
        raise ValueError(
            f"Table '{name}' has primary key column(s) {missing} not in its columns"
        )
    return columns, primary_key


# The schema that holds a table, as PostgreSQL resolves its name ($1), and the schema
# that holds the table of the same name without a schema ($2). A table that does not
# exist yet is created in the first schema of `search_path` that exists.
_RESOLVE_SCHEMAS_SQL = """
SELECT
    (SELECT n.nspname FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.oid = to_regclass($1)) AS table_schema,
    COALESCE(
        (SELECT n.nspname FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.oid = to_regclass($2)),
        current_schema(),
        'public'
    ) AS bare_schema
"""


def _folded(record: dict) -> dict:
    """
    Returns a record with its keys folded as PostgreSQL reads the columns.

    When two keys name one column, the first is kept, as `_column_runs` keeps it.

    Args:
        record: A row bound for the table.

    Returns:
        dict: Each column's value, by the column's lower-case name.
    """
    folded: dict = {}
    for key, value in record.items():
        folded.setdefault(_fold(key), value)
    return folded


def _label_row(record: dict) -> dict:
    """
    Returns the row a ground-truth record is written as.

    `old`, `new` and `label` can hold any JSON value, so they are written as JSON
    text, which a TEXT or a JSONB column takes.

    Args:
        record: A record with `stream_type` "label".

    Returns:
        dict: `sim_ts`, `timestamp`, `path`, `old`, `new` and `label`.
    """
    return {
        "sim_ts": record.get("sim_ts"),
        "timestamp": record.get("timestamp"),
        "path": record.get("path"),
        "old": orjson.dumps(record.get("old")).decode(),
        "new": orjson.dumps(record.get("new")).decode(),
        "label": orjson.dumps(record.get("label")).decode(),
    }


def _create_table_sql(
    name: str, columns: dict[str, str], primary_key: list[str]
) -> str:
    """
    Builds the `CREATE TABLE IF NOT EXISTS` statement for a `tables` entry.

    Names are not quoted, matching the `INSERT` statements this connector writes,
    so both refer to the same table and columns.

    Args:
        name (str): The table name.
        columns (dict[str, str]): Column name to SQL type.
        primary_key (list[str]): The primary key columns, or an empty list.

    Returns:
        str: The statement.
    """
    parts = [f"{column} {sql}" for column, sql in columns.items()]
    if primary_key:
        parts.append(f"PRIMARY KEY ({', '.join(primary_key)})")
    return f"CREATE TABLE IF NOT EXISTS {name} ({', '.join(parts)})"


class PostgresEgress(BaseEgress):
    """
    Asynchronous egress provider for persisting simulation data to PostgreSQL.

    This connector handles long-term storage of simulation results or
    continuous generation of relational CDC data, performing bulk inserts
    of batched data into a specified PostgreSQL table using asyncpg.

    By default a record whose key already exists is skipped (`ON CONFLICT DO
    NOTHING`). Name the key columns in `upsert_keys` to update the existing row
    instead, which a simulation needs when it changes rows it wrote earlier, such
    as an order moving from processing to shipped.

    A payload field `__table__` names the table of that event, and is removed from the
    row. An entry writes the events of its own `table_name` and ignores the others, so
    one entry per table, all fed by the same events, splits a stream into tables. Names
    are compared as PostgreSQL reads them: `Orders` and `public.orders` are the table
    `orders`. A sink warns once about a `__table__` that no PostgresEgress of the
    process writes, which is usually a misspelt name. An
    entry with `upsert_keys` is keyed: it skips an event with a null or missing key column,
    and logs one warning at the end of each batch with the count. Before 1.0.0 such an
    event stopped the batch with an error.

    An ISO time string bound for a timestamp or date column is converted to a
    `datetime` or `date` first, because asyncpg rejects the string itself.

    Tables named in `tables` are created at start when they do not exist, so a run
    needs no separate schema step.

    An event's row also carries the record's `key` as `event_id`, and its `task` and
    `instance`, unless the payload has fields of those names. A ground-truth record,
    sent to a sink added with `when: labels`, is written as `sim_ts`, `timestamp`,
    `path`, `old`, `new` and `label`, the last three as JSON text.

    Examples:
        Updating orders as their status changes:

        ```python
        egress = PostgresEgress(
            "postgresql://user:password@127.0.0.1:5432/db",
            table_name="orders",
            upsert_keys=["order_id"],
        )
        ```

        Creating the table at start, in the shape a YAML file gives:

        ```python
        egress = PostgresEgress(
            "postgresql://user:password@127.0.0.1:5432/db",
            tables={
                "orders": {
                    "columns": {"order_id": "INT", "status": "TEXT"},
                    "primary_key": ["order_id"],
                }
            },
        )
        ```
    """

    # The sinks that are running, so a sink can tell whether another one of its model
    # writes the table a record names. A sink is in it from the start of `run` to its
    # end, so the sinks of a model that has ended are not counted.
    _writers: weakref.WeakSet = weakref.WeakSet()

    def __init__(
        self,
        connection_dsn: str,
        table_name: str | None = None,
        upsert_keys: list[str] | str | None = None,
        tables: dict[str, Any] | None = None,
        table_policy: str = "create",
        **kwargs: Any,
    ):
        """
        Initializes the PostgresEgress with connection details.

        Args:
            connection_dsn: PostgreSQL connection string (DSN).
            table_name: Target table for simulation records. Defaults to the one
                table in `tables` when it names exactly one, and to
                "simulation_data" when `tables` is not given.
            upsert_keys: Optional key columns, a list or one column name. When given, a record whose key
                already exists updates that row's other columns, and the columns
                need a unique index or constraint. When None, such a record is
                skipped.
            tables: Optional tables to create at start when they do not exist. Each
                table name maps to `columns`, a mapping of column name to SQL type,
                and an optional `primary_key`, a column name or a list of them.
            table_policy: What the sink does about the tables in `tables` when the
                run starts. `create`, the default, creates the schema of a qualified
                name (`CREATE SCHEMA IF NOT EXISTS`) and the table when they do not
                exist. `drop_and_create` also drops the table first
                (`DROP TABLE IF EXISTS`). `manual` creates nothing.
            **kwargs: Additional connection pool arguments for asyncpg.

        Raises:
            ValueError: If a `tables` entry is malformed, if `table_policy` is not one
                of its three words, or if `tables` names several tables and
                `table_name` does not say which one to write to.
        """
        if table_policy not in TABLE_POLICIES:
            raise ValueError(
                f"table_policy '{table_policy}' is not one of {', '.join(TABLE_POLICIES)}"
            )
        self.table_policy = table_policy
        self.tables: dict[str, tuple[dict[str, str], list[str]]] = {
            str(name): _table_spec(str(name), spec)
            for name, spec in (tables or {}).items()
        }
        if table_name is None:
            if len(self.tables) > 1:
                raise ValueError(
                    f"tables names {sorted(self.tables)}; set table_name to the one "
                    "this egress writes to"
                )
            table_name = next(iter(self.tables), "simulation_data")
        self.dsn = connection_dsn
        self.table_name = table_name
        if isinstance(upsert_keys, str):
            upsert_keys = [upsert_keys]
        self.upsert_keys = list(upsert_keys) if upsert_keys else None
        self.kwargs = kwargs
        self.pool: asyncpg.Pool | None = None
        self.valid_columns: set[str] = set()
        self.time_columns: dict[str, str] = {}
        # The schemas that `_writes` compares a routed name in, until the sink
        # connects and reads them from the database's `search_path`
        self.table_schema = "public"
        self.bare_schema = "public"
        # Whether `table_schema` and `bare_schema` are read from the database yet
        self._resolved = False
        self._unrouted_warned: set[str] = set()
        # Names that no table of this sink matched while another sink was starting
        self._unrouted_pending: set[str] = set()

    def _warn_unrouted(self, target: str) -> None:
        """Warns once that no running PostgresEgress writes the table a record names.

        A sink that has not connected yet does not know its schema, so the check waits
        until every running sink has read it, and a record of a table that a sink
        still starting writes is not reported. Until then the name is kept, and
        `_check_unrouted` checks it when the last sink has read its schema."""
        if target in self._unrouted_warned:
            return
        if not all(w._resolved for w in self._writers):
            self._unrouted_pending.add(target)
            return
        self._unrouted_warned.add(target)
        if not any(w._writes(target) for w in self._writers):
            logger.warning(
                f"PostgresEgress skips records whose __table__ is {target!r}: no "
                f"running PostgresEgress writes a table of that name."
            )

    def _check_unrouted(self) -> None:
        """Checks the names kept while a sink was starting, once every sink has read
        its schema."""
        if not all(w._resolved for w in self._writers):
            return
        pending, self._unrouted_pending = self._unrouted_pending, set()
        for target in sorted(pending):
            self._warn_unrouted(target)

    def _writes(self, target: str) -> bool:
        """
        Returns whether a record routed to `target` is for this sink's table.

        The statements do not quote names, so PostgreSQL folds a name to lower case.
        A name without a schema is the table of that name in `bare_schema`, the
        first schema of `search_path` that holds one.

        Args:
            target (str): A table name, such as `Orders` or `shop.orders`.

        Returns:
            bool: Whether `target` names the table in `table_schema` that this sink
                writes.
        """
        schema, _, name = _fold(target).rpartition(".")
        own_name = _fold(self.table_name).rpartition(".")[2]
        return name == own_name and (schema or self.bare_schema) == self.table_schema

    async def _init_pool(self) -> None:
        if self.pool is None:
            self.pool = await asyncpg.create_pool(dsn=self.dsn, **self.kwargs)
            logger.info(f"PostgresEgress connected to {self.table_name}")

            # Cache valid columns to safely ignore extra injected fields (like sim_ts)
            self.valid_columns = set()
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                for name, (columns, primary_key) in self.tables.items():
                    if self.table_policy == "manual":
                        break
                    try:
                        await ensure_schema(conn, name)
                        if self.table_policy == "drop_and_create":
                            await conn.execute(f"DROP TABLE IF EXISTS {name}")
                        await conn.execute(
                            _create_table_sql(name, columns, primary_key)
                        )
                    except (
                        asyncpg.exceptions.DuplicateTableError,
                        asyncpg.exceptions.UniqueViolationError,
                    ):
                        # Another writer created it between the existence check
                        # and the create, which IF NOT EXISTS does not guard.
                        pass
                schemas = await conn.fetchrow(
                    _RESOLVE_SCHEMAS_SQL,
                    self.table_name,
                    self.table_name.rpartition(".")[2],
                )
                self.bare_schema = schemas["bare_schema"]
                self.table_schema = (
                    schemas["table_schema"]
                    or _fold(self.table_name).rpartition(".")[0]
                    or self.bare_schema
                )
                self._resolved = True
                for writer in list(PostgresEgress._writers):
                    writer._check_unrouted()
                # The one table the statements reach: PostgreSQL folds the name's case
                # and resolves it through `search_path` here as it does in them
                try:
                    rows = await conn.fetch(
                        "SELECT attname AS column_name, "
                        "format_type(atttypid, NULL) AS data_type "
                        "FROM pg_attribute WHERE attrelid = $1::regclass "
                        "AND attnum > 0 AND NOT attisdropped",
                        self.table_name,
                    )
                except asyncpg.exceptions.UndefinedTableError:
                    rows = []
                self.valid_columns = {row["column_name"] for row in rows}
                self.time_columns = {
                    row["column_name"]: _TIME_TYPES[row.get("data_type")]
                    for row in rows
                    if row.get("data_type") in _TIME_TYPES
                }
                if not self.valid_columns:
                    logger.warning(
                        f"Table '{self.table_name}' does not exist or has no columns!"
                    )
                elif self.upsert_keys:
                    missing = [
                        k
                        for k in self.upsert_keys
                        if _fold(k) not in self.valid_columns
                    ]
                    if missing:
                        raise ValueError(
                            f"Table '{self.table_name}' has no column(s) "
                            f"{sorted(missing)} to use as upsert keys"
                        )

    def _time_keys(self, records: list[dict]) -> dict[str, str]:
        """
        Returns the payload keys that name a time column, with each column's kind.

        PostgreSQL folds a column name to lower case, so a key `CreatedAt` names the
        column `createdat`, and `time_columns` holds the folded names.

        Args:
            records: The rows bound for the table.

        Returns:
            dict[str, str]: Each key as the records spell it, to the kind of the time
                column it names, as `parse_iso_time` takes it.
        """
        return {
            key: self.time_columns[_fold(key)]
            for record in records
            for key in record
            if _fold(key) in self.time_columns
        }

    def _column_runs(self, records: list[dict]) -> list[tuple[list[str], list[dict]]]:
        """
        Splits a batch into runs of consecutive records that carry the same columns.

        Only columns the table has are kept. A record with none of them is dropped.
        The statements do not quote names, so PostgreSQL reads a payload key in lower
        case, and a key matches the column it names in any case. When two keys name
        one column, the first is kept.

        Args:
            records: The batch's records, in the order they arrived.

        Returns:
            list[tuple[list[str], list[dict]]]: Each run's columns and its records.
        """
        runs: list[tuple[list[str], list[dict]]] = []
        for record in records:
            keys: list[str] = []
            for k in record:
                folded = _fold(k)
                if folded in self.valid_columns and folded not in {
                    _fold(key) for key in keys
                }:
                    keys.append(k)
            if not keys:
                continue
            if runs and runs[-1][0] == keys:
                runs[-1][1].append(record)
            else:
                runs.append((keys, [record]))
        return runs

    def _insert_query(self, keys: list[str]) -> str:
        """
        Builds the insert statement for a batch's columns.

        Without `upsert_keys` a conflicting record is skipped, which keeps a
        restarted simulation from failing on rows it already wrote. With
        `upsert_keys` it updates the columns the record carries, apart from the
        keys themselves.

        Args:
            keys: The batch's columns, in the order of the values.

        Returns:
            str: The `INSERT` statement, with one `$n` placeholder per column.

        Raises:
            ValueError: If a key column in `upsert_keys` is missing from the batch.
        """
        columns = ", ".join(keys)
        placeholders = ", ".join(f"${i + 1}" for i in range(len(keys)))
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        if not self.upsert_keys:
            return f"{query} ON CONFLICT DO NOTHING"
        folded_keys = {_fold(k) for k in keys}
        missing = [k for k in self.upsert_keys if _fold(k) not in folded_keys]
        if missing:
            raise ValueError(
                f"Records for {self.table_name} lack the upsert key column(s) {missing}"
            )
        conflict = ", ".join(self.upsert_keys)
        key_columns = {_fold(k) for k in self.upsert_keys}
        updates = [k for k in keys if _fold(k) not in key_columns]
        if not updates:
            return f"{query} ON CONFLICT ({conflict}) DO NOTHING"
        assignments = ", ".join(f"{k} = EXCLUDED.{k}" for k in updates)
        return f"{query} ON CONFLICT ({conflict}) DO UPDATE SET {assignments}"

    def _rows(self, batch: list) -> list[dict]:
        """
        Turns a batch into the rows of this sink's table.

        A label record becomes a row of its own. An event or telemetry record whose
        value is a mapping becomes that mapping, with the envelope fields when
        `envelope` is on, unless its `__table__` names another table or a keyed
        entry finds a null or missing key column. One warning gives the counts of the
        records skipped.

        Args:
            batch: The batch's records, in the order they arrived.

        Returns:
            list[dict]: The rows, in the order of their records.
        """
        # Support multi-table multiplexing using a __table__ key
        clean_batch = []
        skipped: Counter = Counter()
        keyed = bool(self.upsert_keys)
        for item in batch:
            if item.get("stream_type") == "label":
                clean_batch.append(_label_row(item))
                continue
            # `item` is an EventPayload or TelemetryPayload dict.
            # The actual user data is in `item["value"]`.
            payload = item.get("value")
            if not isinstance(payload, dict):
                # PostgresEgress requires dictionaries (or Pydantic models
                # dumped to dicts). A telemetry reading that is a number
                # is not written either, and is skipped without a warning
                if item.get("stream_type") != "telemetry":
                    skipped["not_mapping"] += 1
                continue
            # Merge the simulation metadata into the payload so it CAN be inserted if the user configured their schema to accept it
            payload_copy = payload.copy()
            if self.envelope:
                payload_copy["sim_ts"] = item.get("sim_ts")
                payload_copy["timestamp"] = item.get("timestamp")
                if "key" in item:
                    payload_copy["event_id"] = item["key"]
                    # A payload field of the same name wins, as in flat files
                    for field in ("task", "instance"):
                        payload_copy.setdefault(field, item.get(field))
                if "path_id" in item:
                    payload_copy["path_id"] = item["path_id"]
            # A null or empty `__table__` is no route: the record goes to this table
            target = payload_copy.pop("__table__", None) or self.table_name
            if type(target) is not str:
                skipped["not_text"] += 1
                continue
            if not self._writes(target):
                self._warn_unrouted(target)
                continue
            if keyed and not keep_keyed(
                [_folded(payload_copy)],
                [_fold(k) for k in self.upsert_keys or []],
                skipped,
            ):
                continue
            clean_batch.append(payload_copy)
        warn_skipped(self, skipped, "__table__")
        return clean_batch

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        Main execution loop for PostgreSQL data persistence.

        If PostgreSQL does not answer at start, the sink retries with a back-off that
        grows to one minute, as the Kafka sinks do. An error that no wait fixes, such
        as a table definition the server refuses, ends the task at once.

        Args:
            egress_queue: A thread-safe queue containing batches of simulation data.

        Raises:
            ValueError: If a column in `upsert_keys` is not in the table.
            asyncpg.PostgresError: If the server refuses a setting, such as the
                password or a table definition.
        """
        # Schemas read in an earlier run may have changed, so other sinks wait for
        # this one to read them again
        self._resolved = False
        PostgresEgress._writers.add(self)
        try:
            await connect_with_retry(
                self._init_pool,
                self._close_pool,
                "PostgresEgress",
                is_temporary_postgres_error,
            )
            await self._write_forever(egress_queue)
        finally:
            PostgresEgress._writers.discard(self)
            await self._close_pool()

    async def _close_pool(self) -> None:
        """Closes the pool and forgets it, so the next run makes its own in its own
        event loop and sets the tables up again."""
        pool, self.pool = self.pool, None
        if pool is not None:
            await pool.close()

    async def _write_forever(self, egress_queue: queue.Queue) -> None:
        """Writes the batches of the queue until the task is cancelled."""
        while True:
            try:
                # Receive a batch (list) of dictionaries
                batch = egress_queue.get_nowait()
                if not batch:
                    continue

                clean_batch = self._rows(batch)
                if not clean_batch:
                    continue

                # The records carry times as ISO strings, which asyncpg rejects for
                # a timestamp or date column.
                clean_batch = parse_iso_columns(
                    clean_batch, self._time_keys(clean_batch)
                )

                # Each run of records with the same columns is one statement, in the
                # order they arrived. A statement built from one record's columns
                # would set another record's missing columns to NULL on an upsert.
                runs = self._column_runs(clean_batch)
                if not runs:
                    logger.warning(
                        f"No matching columns for table {self.table_name}. Skipping."
                    )
                    continue

                assert self.pool is not None
                with self._writing():
                    async with self.pool.acquire() as conn:
                        for keys, records in runs:
                            values = [
                                tuple(data.get(k) for k in keys) for data in records
                            ]
                            await conn.executemany(self._insert_query(keys), values)

                written = sum(len(records) for _, records in runs)
                logger.debug(f"Inserted {written} records into {self.table_name}")

            except queue.Empty:
                # Yield to the event loop if the queue is empty
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error inserting batch into {self.table_name}: {e}")
                # Back-off on database failures to prevent rapid crash looping
                if not self._drop_backlog(
                    egress_queue, "PostgresEgress", is_temporary_postgres_error(e)
                ):
                    await asyncio.sleep(1)

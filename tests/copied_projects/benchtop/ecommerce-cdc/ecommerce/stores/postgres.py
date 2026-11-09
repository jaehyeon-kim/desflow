"""The PostgreSQL store: the six tables in the `cdc` schema, and the replication slot."""

import asyncio
import time
import types

import asyncpg
from pydantic import BaseModel

from ecommerce.core.config import DSN, REPLICATION_SLOT, SCHEMA
from ecommerce.core.models import TABLES

_SQL_TYPES = {str: "TEXT", int: "BIGINT", float: "DOUBLE PRECISION"}


def ddl(model: type[BaseModel]) -> str:
    """
    Builds the `CREATE TABLE` statement for a model's table, keyed on `id`.

    Args:
        model (type[BaseModel]): The row model.

    Returns:
        str: The statement.
    """
    columns = []
    for name, info in model.model_fields.items():
        kind = info.annotation
        if isinstance(kind, types.UnionType):  # such as `str | None`
            kind = next(t for t in kind.__args__ if t is not type(None))
        key = " PRIMARY KEY" if name == "id" else ""
        # A Literal, such as the order status, is not in _SQL_TYPES and becomes text.
        columns.append(f"{name} {_SQL_TYPES.get(kind, 'TEXT')}{key}")
    return f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLES[model]} ({', '.join(columns)})"


def _execute(*statements: str) -> None:
    """Runs statements in one connection."""

    async def run() -> None:
        conn = await asyncpg.connect(DSN)
        try:
            for statement in statements:
                await conn.execute(statement)
        finally:
            await conn.close()

    asyncio.run(run())


def create_tables() -> None:
    """Creates the six tables, if they do not exist yet."""
    _execute(*(ddl(model) for model in TABLES))


def drop_tables() -> None:
    """Drops the six tables, if they exist."""
    _execute(*(f"DROP TABLE IF EXISTS {SCHEMA}.{t}" for t in TABLES.values()))


def drop_slot(wait: float = 30) -> None:
    """
    Drops the Debezium connector's replication slot, once the connector has let go of it.

    Args:
        wait (float): How many seconds to wait for the slot to become inactive.
    """
    query = f"SELECT active FROM pg_replication_slots WHERE slot_name = '{REPLICATION_SLOT}'"

    async def active() -> bool | None:
        conn = await asyncpg.connect(DSN)
        try:
            return await conn.fetchval(query)
        finally:
            await conn.close()

    deadline = time.monotonic() + wait
    while (state := asyncio.run(active())) and time.monotonic() < deadline:
        time.sleep(1)  # the slot stays active until the connector's task ends
    if state is False:
        _execute(f"SELECT pg_drop_replication_slot('{REPLICATION_SLOT}')")

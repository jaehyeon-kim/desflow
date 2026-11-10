"""A table name reaches the same table in the set-up, the column lookup and the insert.

PostgreSQL folds an unquoted name to lower case and looks a name without a schema up
through the search path. The fake catalogue holds `public.orders(order_id, status)` and
`archive.orders(order_id, archived_at)`, and answers a column lookup the way PostgreSQL
does for the query the sink sends: `pg_attribute` through a `regclass`, or the columns
of every table `information_schema` finds by the name as written.
"""

import asyncio
import queue
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from desflow.connectors.egress.postgres import PostgresEgress

CATALOGUE: Dict[str, Dict[str, List[str]]] = {
    "public": {"orders": ["order_id", "status"]},
    "archive": {"orders": ["order_id", "archived_at"]},
}
SEARCH_PATH = ["public", "archive"]


class FakeConn:
    def __init__(self, statements: List[str]):
        self.statements = statements

    async def execute(self, sql: str, *args: Any) -> None:
        self.statements.append(sql)

    async def executemany(self, sql: str, rows: list) -> None:
        self.statements.append(sql)

    async def fetchval(self, sql: str, *args: Any) -> int:
        return 1

    async def fetch(self, sql: str, *args: Any) -> list:
        if "pg_attribute" in sql:
            return [
                {"column_name": column, "data_type": "text"}
                for column in self._regclass(args[0])
            ]
        if "information_schema" in sql:
            return self._information_schema(sql, args)
        return []

    @staticmethod
    def _regclass(name: str) -> List[str]:
        """The columns of the one table a name resolves to, as `::regclass` does."""
        schema, dot, table = name.lower().rpartition(".")
        schemas = [schema] if dot else SEARCH_PATH
        for found in schemas:
            if table in CATALOGUE.get(found, {}):
                return CATALOGUE[found][table]
        raise LookupError(f'relation "{name}" does not exist')

    @staticmethod
    def _information_schema(sql: str, args: tuple) -> list:
        """The columns of every table that has the name as written."""
        rows = []
        for schema, tables in CATALOGUE.items():
            for table, columns in tables.items():
                if "table_schema" in sql and (schema, table) != tuple(args[:2]):
                    continue
                if "table_schema" not in sql and table != args[0]:
                    continue
                rows += [{"column_name": c, "data_type": "text"} for c in columns]
        return rows


class FakePool:
    def __init__(self, statements: List[str]):
        self.statements = statements

    def acquire(self) -> Any:
        conn = FakeConn(self.statements)

        class Context:
            async def __aenter__(self) -> Any:
                return conn

            async def __aexit__(self, *exc: Any) -> None:
                return None

        return Context()

    async def close(self) -> None:
        return None


@pytest.mark.parametrize(
    "table", ["orders", "Orders", "public.orders", "public.Orders"]
)
def test_a_table_name_reaches_one_table_with_its_own_columns(table):
    statements: List[str] = []

    async def create_pool(*args: Any, **kwargs: Any) -> Any:
        return FakePool(statements)

    sink = PostgresEgress("postgresql://u:p@h/db", table_name=table)
    batch = [
        {
            "stream_type": "event",
            "key": "a-1",
            "value": {"order_id": 1, "status": "new", "archived_at": "x"},
        }
    ]
    egress_queue: queue.Queue = queue.Queue()
    egress_queue.put(batch)

    async def main() -> None:
        with patch(
            "desflow.connectors.egress.postgres.asyncpg.create_pool", create_pool
        ):
            task = asyncio.create_task(sink.run(egress_queue))
            for _ in range(300):
                if any("INSERT" in s for s in statements):
                    break
                await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(main())
    assert sink.valid_columns == {"order_id", "status"}, table
    inserts = [s for s in statements if "INSERT" in s]
    assert inserts, f"{table}: nothing was inserted"
    assert "archived_at" not in inserts[0]
    assert f"INSERT INTO {table} (order_id, status)" in inserts[0]

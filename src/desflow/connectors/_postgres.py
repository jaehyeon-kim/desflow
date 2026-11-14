"""PostgreSQL names and the schema set-up that the PostgreSQL ingress and egress share."""

from typing import Any, Optional

_ASCII_LOWER = {
    ord(letter): ord(letter.lower()) for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
}


def fold(name: str) -> str:
    """
    Returns an unquoted name as PostgreSQL reads it: `A` to `Z` in lower case.

    In a multi-byte encoding such as UTF8, PostgreSQL's `downcase_identifier`
    (`src/backend/parser/scansup.c`) folds only these letters, so `Ärger` stays
    `Ärger`, where `str.lower()` gives `ärger`.

    Args:
        name: A table or column name, without quotes.

    Returns:
        str: The name with `A` to `Z` changed to `a` to `z`.
    """
    return name.translate(_ASCII_LOWER)


def create_schema_sql(name: str) -> Optional[str]:
    """
    Builds the `CREATE SCHEMA IF NOT EXISTS` statement for a qualified table name.

    Args:
        name (str): The table name, such as `dashboard.orders`.

    Returns:
        Optional[str]: The statement, or None when the name has no schema.
    """
    schema, dot, _ = name.rpartition(".")
    return f"CREATE SCHEMA IF NOT EXISTS {schema}" if dot else None


async def ensure_schema(conn: Any, name: str) -> None:
    """
    Creates the schema of a qualified table name when the database does not have it.

    `CREATE SCHEMA IF NOT EXISTS` checks the database privilege first, so the schema
    is looked up before it is created: a role that may only use an existing schema is
    not asked to create it. A name with no schema needs nothing.

    Args:
        conn: An asyncpg connection.
        name: The table name, such as `dashboard.params`.
    """
    schema_sql = create_schema_sql(name)
    if schema_sql is None:
        return
    schema = fold(name.rpartition(".")[0])
    known = await conn.fetchval("SELECT 1 FROM pg_namespace WHERE nspname = $1", schema)
    if not known:
        await conn.execute(schema_sql)

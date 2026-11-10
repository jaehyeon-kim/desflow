"""The store: this project's tables in odctl's PostgreSQL, and the dashboards' query.

Timestamps are ISO strings, as desflow publishes them, so queries cast them.
"""

import asyncpg

from sales.core.config import DSN, PARAMS_TABLE, SCHEMA

TABLES = ("products", "users", "orders", "order_items")
UPSERT_KEYS = {"orders": ["id"], "order_items": ["id"]}  # rows whose status changes

_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};
CREATE TABLE IF NOT EXISTS {SCHEMA}.products (id BIGINT PRIMARY KEY, name TEXT,
    category TEXT, department TEXT, retail_price FLOAT8, cost FLOAT8);
CREATE TABLE IF NOT EXISTS {SCHEMA}.users (id TEXT PRIMARY KEY, age INT, gender TEXT,
    country TEXT, traffic_source TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS {SCHEMA}.orders (id TEXT PRIMARY KEY, user_id TEXT,
    status TEXT, num_of_item INT, created_at TEXT);
CREATE TABLE IF NOT EXISTS {SCHEMA}.order_items (id TEXT PRIMARY KEY, order_id TEXT,
    user_id TEXT, product_id BIGINT, status TEXT, sale_price FLOAT8, created_at TEXT);
CREATE TABLE IF NOT EXISTS {PARAMS_TABLE} (id SERIAL PRIMARY KEY,
    path VARCHAR(255) NOT NULL, value TEXT NOT NULL,
    is_applied BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
-- desflow 0.21.0 reads `path` and `value`; a table made before it gets its columns renamed.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema || '.' || table_name = '{PARAMS_TABLE}'
               AND column_name = 'param_path') THEN
        ALTER TABLE {PARAMS_TABLE} RENAME COLUMN param_path TO path;
        ALTER TABLE {PARAMS_TABLE} RENAME COLUMN param_value TO value;
    END IF;
END $$;
"""

# The order items of the last $1 minutes, with their users and products.
# clock_timestamp() is the time now; current_timestamp would stay at the transaction's start.
RECENT_ITEMS = f"""
SELECT u.id AS user_id, u.age, u.gender, u.country, u.traffic_source,
    o.order_id, o.id AS item_id, p.category, p.cost, o.status AS item_status,
    o.sale_price, o.created_at
FROM {SCHEMA}.order_items AS o
JOIN {SCHEMA}.users AS u ON u.id = o.user_id
JOIN {SCHEMA}.products AS p ON p.id = o.product_id
WHERE o.created_at::timestamptz >= clock_timestamp() - make_interval(mins => $1)
"""


async def create_tables() -> None:
    """Creates the schema, the four tables and the parameter table, if they do not exist."""
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(_DDL)
    finally:
        await conn.close()


async def drop_schema() -> None:
    """Drops the schema with every table in it, if it exists."""
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    finally:
        await conn.close()


async def send_change(path: str, value: float) -> None:
    """
    Adds a parameter change for the running simulation to pick up.

    Args:
        path (str): The registry path, such as `sales.arrival.visitor.rate`.
        value (float): Its new value.
    """
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(
            f"INSERT INTO {PARAMS_TABLE} (path, value) VALUES ($1, $2)",
            path,
            str(value),
        )
    finally:
        await conn.close()


async def recent_items(conn: asyncpg.Connection, minutes: int) -> list[dict]:
    """
    Reads the order items of the last `minutes`, with their users and products.

    Args:
        conn (asyncpg.Connection): An open connection.
        minutes (int): The lookback window.

    Returns:
        list[dict]: One record per order item.
    """
    return [dict(row) for row in await conn.fetch(RECENT_ITEMS, minutes)]

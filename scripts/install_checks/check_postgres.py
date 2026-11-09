"""Imports and constructs what the `postgres` extra exists to enable, installed alone."""

from desflow import PostgresEgress, PostgresIngress

PostgresEgress(connection_dsn="postgresql://user:pass@localhost:5432/sim")
PostgresIngress(connection_dsn="postgresql://user:pass@localhost:5432/sim")

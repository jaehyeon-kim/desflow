"""Loads simulated days into the four Iceberg tables, for a backfill or a daily run.

A run covers `n_days` ending with `until`. With `reset`, or when the tables do not exist
yet, it recreates the tables, stores the window's first hour and `seed` on them, and
appends. Otherwise it continues from the stored first hour and seed, and upserts on each
table's keys, so running a day again replaces it.

Run: python -m airq.feature.load --n-days 730 --reset   (backfill)
     python -m airq.feature.load --until today          (one day; default: yesterday)
"""

import argparse
import logging
from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta

from pydantic import BaseModel

from airq.core import days
from airq.core.config import DEFAULT_SEED, ORIGIN_PROPERTY, SEED_PROPERTY, TABLES
from airq.core.models import Observation
from airq.feature.generator import generate
from airq.feature.simulation import simulate
from airq.stores.iceberg import catalog, recreate_tables

logger = logging.getLogger(
    "airq.feature.load"
)  # __name__ is "__main__" under python -m


def advance(records: Iterator[list[BaseModel]], start: datetime) -> list[Observation]:
    """
    Moves the generator up to `start`, and returns the readings of the day before.

    The generator must run from the stored first hour to reproduce the same values. The
    readings of the day before are kept, because the first day's air quality row needs
    the previous day's mean.

    Args:
        records (Iterator[list[BaseModel]]): The generator's hourly rows, from the first
            hour.
        start (datetime): The first hour to load, in UTC.

    Returns:
        list[Observation]: The readings of the 24 hours before `start`.
    """
    previous: list[Observation] = []
    for rows in records:
        reading = rows[-1]
        if reading.measured_at >= start - timedelta(days=1):
            previous.append(reading)
        if reading.measured_at >= start - timedelta(hours=1):
            break
    return previous


def load(
    n_days: int = 1,
    until: date | None = None,
    seed: int | None = None,
    reset: bool = False,
) -> None:
    """
    Loads `n_days` of simulated data, ending with `until`, into the four tables.

    Args:
        n_days (int): How many days to load.
        until (date, optional): The last day to load. Defaults to yesterday, UTC.
        seed (int, optional): The seed for the generated values. Defaults to the stored
            seed, or to the default seed when the tables are recreated.
        reset (bool): Whether to recreate the tables first.

    Raises:
        SystemExit: If the window starts before the stored first hour.
    """
    until = until or days.resolve("yesterday")
    start = datetime.combine(until, time(), UTC) - timedelta(days=n_days - 1)
    cat = catalog()
    reset = reset or not cat.table_exists(TABLES[Observation])
    if reset:
        seed = DEFAULT_SEED if seed is None else seed
        origin = start
        recreate_tables(
            cat, {ORIGIN_PROPERTY: origin.isoformat(), SEED_PROPERTY: str(seed)}
        )
    else:
        properties = cat.load_table(TABLES[Observation]).properties
        origin = datetime.fromisoformat(properties[ORIGIN_PROPERTY])
        if start < origin:
            raise SystemExit(
                f"{start.date()} is before the first day, {origin.date()}."
            )
        seed = int(properties[SEED_PROPERTY]) if seed is None else seed

    records = generate(origin, seed)
    previous = advance(records, start) if start > origin else []
    simulate(cat, records, start, n_days * 24, upsert=not reset, previous=previous)
    for identifier in TABLES.values():
        logger.info(
            "%s: %d rows", identifier, cat.load_table(identifier).scan().count()
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--n-days", type=int, default=1, help="days to load (default: 1)"
    )
    parser.add_argument(
        "--until",
        type=days.argument,
        default="yesterday",
        help=f"last day to load: {days.FORMS} (default: yesterday, UTC)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="seed for the generated values (default: the stored one, or 42 on reset)",
    )
    parser.add_argument(
        "--reset", action="store_true", help="recreate the tables before loading"
    )
    args = parser.parse_args()
    load(args.n_days, args.until, args.seed, args.reset)

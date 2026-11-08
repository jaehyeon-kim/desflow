"""Replay: arrivals taken from a local file or a list of rows, at their recorded times.

`read_trace` reads and checks the rows once, before the run: each becomes one
arrival at its time, carrying the row's fields as the entity's attributes. The
builder's `add_arrival_replay` and a YAML arrival's `replay` use it; low-level code
loops over its rows itself.
"""

import csv
import math
import re
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

logger = logging.getLogger(__name__)

# Seconds in one unit of a numeric time column
UNITS: Dict[str, float] = {"ms": 0.001, "s": 1.0, "min": 60.0, "h": 3600.0}
TIMESTAMP = "timestamp"


def check_fields(fields: Any) -> None:
    """
    Refuses `fields` given as one string other than `all`, as a blueprint's
    `replay.fields` is refused, because a string would be read one letter at a time.

    Args:
        fields: The columns each arrival carries, or `all`.

    Raises:
        ValueError: If `fields` is a string other than `all`.
    """
    if isinstance(fields, str) and fields != "all":
        raise ValueError(
            f"fields: Input should be 'all'; Input should be a valid list, "
            f"got {fields!r}"
        )


@dataclass
class Row:
    """
    One arrival of a trace.

    Attributes:
        time (float): Its simulation time.
        number (int): Its row number in the source, counting the first row as 1.
        fields (Dict[str, Any]): The fields it carries, which become the entity's
            attributes.
    """

    time: float
    number: int
    fields: Dict[str, Any]


def read_trace(
    source: Union[str, Path, Iterable[Mapping[str, Any]]],
    time: str = "time",
    unit: str = "s",
    fields: Union[str, Sequence[str]] = "all",
    on_invalid: str = "error",
    start: Optional[datetime] = None,
    initial_time: float = 0.0,
) -> List[Row]:
    """
    Reads and checks the rows of a trace, in their order.

    Args:
        source: A CSV file, a Parquet file (which needs the `parquet` extra), or an
            iterable of mappings, such as a list of dicts.
        time: The column that holds each row's time.
        unit: `ms`, `s`, `min` or `h` for a number of that unit from the start of
            the run, or `timestamp` for a date and time, read against `start` so
            that a row arrives at the time its records are stamped with.
        fields: The columns each arrival carries as attributes, or `all`.
        on_invalid: `error` refuses a row whose time is missing or not a time;
            `skip` leaves it out, with a warning naming it.
        start: The date and time of simulation time 0, as for every record's
            `timestamp`, needed with `unit="timestamp"`, such as
            `env.start_datetime`. When it has a time zone, a timestamp without an
            offset is read in that zone.
        initial_time: The simulation clock's start.

    Returns:
        List[Row]: The rows, each with its simulation time.

    Raises:
        ValueError: If the unit or `on_invalid` is unknown; `fields` is a string
            other than `all`; a row is out of time order, naming the first one; a
            row's time is before the start; a field is missing; or a row's time is
            invalid and `on_invalid` is `error`.
        ImportError: If a Parquet file is read without pyarrow.
    """
    if unit not in UNITS and unit != TIMESTAMP:
        raise ValueError(
            f"unit must be one of {', '.join([*UNITS, TIMESTAMP])}, got '{unit}'"
        )
    if on_invalid not in ("error", "skip"):
        raise ValueError(f"on_invalid must be error or skip, got '{on_invalid}'")
    if unit == TIMESTAMP and start is None:
        raise ValueError("a timestamp column needs the run's logical_start_time")
    check_fields(fields)
    rows: List[Row] = []
    last: Optional[float] = None
    for number, record in enumerate(read_records(source), start=1):
        when = _time(record.get(time), unit, start, initial_time)
        if when is None:
            message = f"row {number} has no valid {time}: {record.get(time)!r}"
            if on_invalid == "error":
                raise ValueError(message)
            logger.warning(
                f"Skipped row {number}, which has no valid {time}: {record.get(time)!r}"
            )
            continue
        if when < initial_time:
            raise ValueError(
                f"row {number} is at {when:g} s, before the start of the run at "
                f"{initial_time:g} s; remove the row, or start the run earlier"
            )
        if last is not None and when < last:
            raise ValueError(
                f"row {number} is at {when:g} s, before the row above it at {last:g} s; "
                f"the rows must be in time order"
            )
        last = when
        if fields == "all":
            kept = {key: plain_value(value) for key, value in record.items()}
        else:
            missing = [name for name in fields if name not in record]
            if missing:
                raise ValueError(
                    f"row {number} has no field {', '.join(missing)}; add it to the "
                    f"row, or leave it out of fields"
                )
            kept = {name: plain_value(record[name]) for name in fields}
        rows.append(Row(when, number, kept))
    return rows


def read_records(source: Any) -> Iterable[Mapping[str, Any]]:
    """The rows of a source, as mappings.

    Args:
        source: A CSV or Parquet file, or an iterable of mappings.
    """
    if isinstance(source, (str, Path)):
        path = Path(source)
        if path.suffix.lower() == ".parquet":
            import pyarrow.parquet as pq

            return pq.read_table(path).to_pylist()
        with path.open(newline="", encoding="utf-8") as file:
            return list(csv.DictReader(file))
    return list(source)


# A CSV number: a sign, digits, a point and an exponent, and nothing else, so `1_0`,
# `nan`, `Infinity` and `0x1` stay text
_NUMBER = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")


def _time(
    value: Any, unit: str, start: Optional[datetime], initial_time: float
) -> Optional[float]:
    """A row's simulation time, or None when the value is not a time (NaN and
    infinity are not times)."""
    when = _time_of(value, unit, start, initial_time)
    if when is not None and not math.isfinite(when):
        return None
    return when


def _time_of(
    value: Any, unit: str, start: Optional[datetime], initial_time: float
) -> Optional[float]:
    if value is None or value == "":
        return None
    if unit == TIMESTAMP:
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                return None
        if not isinstance(value, datetime):
            return None
        assert start is not None
        if start.tzinfo is not None:
            # A time without an offset is read in the run's zone, and the clock
            # runs in UTC, so a daylight-saving change counts as real time
            if value.tzinfo is None:
                value = value.replace(tzinfo=start.tzinfo)
            value = value.astimezone(timezone.utc)
            start = start.astimezone(timezone.utc)
        elif value.tzinfo is not None:
            raise ValueError(
                "a timestamp has a time zone but logical_start_time has none; give "
                "logical_start_time a time zone, or a timezone setting"
            )
        # As records do: logical_start_time is simulation time 0, whatever the
        # initial time, so a row's timestamp is the timestamp its arrival gets
        return (value - start).total_seconds()
    if isinstance(value, str) and not _NUMBER.fullmatch(value.strip()):
        return None  # such as `1_0`, `nan` or `0x1`: not a CSV number
    try:
        return initial_time + float(value) * UNITS[unit]
    except (TypeError, ValueError):
        return None


def plain_value(value: Any) -> Any:
    """A CSV field as a number when it reads as one; other values as they are.

    Args:
        value: The field's value.
    """
    if not isinstance(value, str) or not _NUMBER.fullmatch(value.strip()):
        return value
    for kind in (int, float):
        try:
            return kind(value)
        except ValueError:
            pass
    return value

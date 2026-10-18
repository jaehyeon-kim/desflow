"""Replay: arrivals taken from a local file or a list of rows, at their recorded times.

`read_trace` reads and checks the rows once, before the run: each becomes one
arrival at its time, carrying the row's fields as the entity's attributes. The
builder's `add_arrival_replay` and a YAML arrival's `replay` use it; low-level code
loops over its rows itself.
"""

import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

logger = logging.getLogger(__name__)

# Seconds in one unit of a numeric time column
UNITS: Dict[str, float] = {"ms": 0.001, "s": 1.0, "min": 60.0, "h": 3600.0}
TIMESTAMP = "timestamp"


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
            the run, or `timestamp` for a date and time, read from
            `logical_start_time` (`start`).
        fields: The columns each arrival carries as attributes, or `all`.
        on_invalid: `error` refuses a row whose time is missing or not a time;
            `skip` leaves it out, with a warning naming it.
        start: The date and time simulation time `initial_time` stands for, needed
            with `unit="timestamp"`.
        initial_time: The simulation clock's start.

    Returns:
        List[Row]: The rows, each with its simulation time.

    Raises:
        ValueError: If the unit or `on_invalid` is unknown; a row is out of time
            order, naming the first one; a row's time is before the start; a
            field is missing; or a row's time is invalid and `on_invalid` is
            `error`.
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
    rows: List[Row] = []
    last: Optional[float] = None
    for number, record in enumerate(_records(source), start=1):
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
                f"{initial_time:g} s"
            )
        if last is not None and when < last:
            raise ValueError(
                f"row {number} is at {when:g} s, before the row above it at {last:g} s; "
                f"the rows must be in time order"
            )
        last = when
        if fields == "all":
            kept = {key: _value(value) for key, value in record.items()}
        else:
            missing = [name for name in fields if name not in record]
            if missing:
                raise ValueError(f"row {number} has no field {', '.join(missing)}")
            kept = {name: _value(record[name]) for name in fields}
        rows.append(Row(when, number, kept))
    return rows


def _records(source: Any) -> Iterable[Mapping[str, Any]]:
    """The rows of a source, as mappings."""
    if isinstance(source, (str, Path)):
        path = Path(source)
        if path.suffix.lower() == ".parquet":
            import pyarrow.parquet as pq

            return pq.read_table(path).to_pylist()
        with path.open(newline="", encoding="utf-8") as file:
            return list(csv.DictReader(file))
    return list(source)


def _time(
    value: Any, unit: str, start: Optional[datetime], initial_time: float
) -> Optional[float]:
    """A row's simulation time, or None when the value is not a time."""
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
        if (value.tzinfo is None) != (start.tzinfo is None):
            raise ValueError(
                "a timestamp and logical_start_time must both have a time zone or "
                "both have none"
            )
        return initial_time + (value - start).total_seconds()
    try:
        return initial_time + float(value) * UNITS[unit]
    except (TypeError, ValueError):
        return None


def _value(value: Any) -> Any:
    """A CSV field as a number when it reads as one; other values as they are."""
    if not isinstance(value, str):
        return value
    for kind in (int, float):
        try:
            return kind(value)
        except ValueError:
            pass
    return value

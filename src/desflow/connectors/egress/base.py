import logging
import queue
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


def extract_dict(data: Any) -> dict:
    """
    Returns a dict from a Pydantic V1 or V2 object, or from a dict.

    Args:
        data (Any): A raw dictionary or a Pydantic model.

    Returns:
        dict: The extracted dictionary representation of the data.
    """
    if isinstance(data, dict):
        return data
    if hasattr(data, "model_dump"):
        return data.model_dump(mode="json")
    if hasattr(data, "dict"):
        return data.dict()
    return data


def is_reserved(key: Any) -> bool:
    """Returns True for a payload key that starts and ends with two underscores.

    Such keys, such as `__table__`, belong to desflow, not to the data.

    Args:
        key: A key of an event's `value`.

    Returns:
        bool: True when the key is reserved.
    """
    return type(key) is str and len(key) > 4 and key[:2] == "__" and key[-2:] == "__"


def drop_reserved_keys(batch: list, sink: Any, keep: tuple = ()) -> list:
    """
    Returns the batch with reserved keys taken out of each record's `value`.

    A sink that does not route by a reserved key writes none of them. The first
    time a sink meets a key, it logs a warning naming the key and the fix. Records
    without one are passed on as they are; a record with one is copied, because
    every egress provider receives the same dictionary objects.

    Args:
        batch (list): The published records.
        sink (Any): The sink, which remembers the keys it has warned about.
        keep (tuple): Reserved keys the sink routes by. They are left in the value
            for the sink to read and remove, and never warned about.

    Returns:
        list: The batch itself when no record has a reserved key, else a copy.
    """
    result = batch
    for index, record in enumerate(batch):
        value = record.get("value") if type(record) is dict else None
        if type(value) is not dict:
            continue
        reserved = [key for key in value if is_reserved(key) and key not in keep]
        if not reserved:
            continue
        if result is batch:
            result = list(batch)
        warned = sink.__dict__.setdefault("_reserved_warned", set())
        for key in reserved:
            if key not in warned:
                warned.add(key)
                logger.warning(
                    "%s drops the payload key %r: keys that start and end with two "
                    "underscores are reserved for desflow, and only a sink that "
                    "routes by one uses it: __topic__ for Kafka, __table__ for "
                    "Iceberg and PostgreSQL, __path__ for Parquet and JSONL, "
                    "__stream__ for Redis. Rename the key to keep it as data.",
                    type(sink).__name__,
                    key,
                )
        kept = {
            key: item
            for key, item in value.items()
            if not is_reserved(key) or key in keep
        }
        result[index] = {**record, "value": kept}
    return result


def flatten_record(record: dict) -> dict:
    """
    Returns a copy of `record` with an event's `value` mapping merged into it.

    Storage sinks use this to give each payload field its own column. It returns a
    copy because every egress provider receives the same dictionary objects, so
    changing the record in place would also change what another sink writes. A
    payload field with the same name as a record field replaces it.

    Args:
        record (dict): A published record.

    Returns:
        dict: The flat row. A record whose `value` is not a mapping is copied as it is.
    """
    value = record.get("value")
    if not isinstance(value, dict):
        return dict(record)
    row = {key: item for key, item in record.items() if key != "value"}
    row.update(value)
    return row


def default_row(data: Any) -> Optional[dict]:
    """
    Returns the row a storage sink writes for a record when no router is given.

    Telemetry records are left out, and an event's `value` mapping is unpacked into
    columns. This is what the routers in the examples did by hand, so a sink
    configured with only a path or a table name writes one flat row per event.

    Args:
        data (Any): A published record, as a dictionary or a Pydantic model.

    Returns:
        Optional[dict]: The flat row, or None for a telemetry record.
    """
    record = extract_dict(data)
    if record.get("stream_type") == "telemetry":
        return None
    return flatten_record(record)


def take_route(record: dict, route_key: str) -> tuple:
    """
    Returns the record without its routing key, and the key's value.

    A record without the key is returned as it is. A record with it is copied, because
    every egress provider receives the same dictionary objects.

    Args:
        record (dict): A published record.
        route_key (str): A reserved key of the record's `value`, such as `__table__`.

    Returns:
        tuple: The record, and the key's value or None.
    """
    value = record.get("value")
    if type(value) is not dict or route_key not in value:
        return record, None
    kept = {key: item for key, item in value.items() if key != route_key}
    return {**record, "value": kept}, value[route_key]


def warn_skipped(sink: Any, skipped: Counter, route_key: Optional[str] = None) -> None:
    """
    Logs one warning for what a sink skipped in one flush, with the counts.

    Args:
        sink (Any): The sink, named in the warning.
        skipped (Counter): The records skipped, by reason: `no_route` for a record
            without its routing key, `no_key` for a record without a key column of
            its table, and `not_mapping` for a value that is not a mapping, which is
            warned about once per sink.
        route_key (Optional[str]): The routing key a `no_route` record lacks.
    """
    reasons = {
        "no_route": f"had no {route_key}",
        "no_key": "had none of the key columns of their table",
        "not_mapping": "had a value that is not a mapping, which this sink cannot "
        "write without the record around it",
    }
    once = sink.__dict__.setdefault("_not_mapping_warned", False)
    if skipped.get("not_mapping") and once:
        skipped = Counter({k: v for k, v in skipped.items() if k != "not_mapping"})
    elif skipped.get("not_mapping"):
        sink.__dict__["_not_mapping_warned"] = True
    parts = [f"{count} {reasons[name]}" for name, count in skipped.items() if count]
    if parts:
        logger.warning(
            "%s skipped records in this flush: %s.",
            type(sink).__name__,
            "; ".join(parts),
        )


def group_rows(
    batch: list,
    router: Optional[Callable[[Any], Optional[str]]],
    default_target: Optional[str],
    envelope: bool = True,
    route_key: Optional[str] = None,
    require_route: bool = False,
    skipped: Optional[Counter] = None,
) -> Dict[str, List[dict]]:
    """
    Groups a batch into the rows each destination receives.

    With a router, the router names the destination of each record, or returns None
    to drop it, and the record is written as the router left it. Without one, every
    row goes to the destination the record's `route_key` names, or to
    `default_target`, in the shape `default_row` gives it. The `route_key` is
    removed from the record either way, and a router wins over it.

    With `envelope` off the row is the event's `value` alone, whichever way the
    destination was chosen, and an event whose value is not a mapping is skipped.

    Args:
        batch (list): The published records, as dictionaries or Pydantic models.
        router (Optional[Callable]): The caller's routing function, if any.
        default_target (Optional[str]): The destination used when there is no router.
        envelope (bool): False writes each event's value as the row.
        route_key (Optional[str]): The reserved key of the value that names the
            destination, such as `__path__`.
        require_route (bool): True counts, in `skipped`, an event that has no
            destination: no `route_key`, no router and no `default_target`.
        skipped (Optional[Counter]): Counts the records skipped, by reason.

    Returns:
        Dict[str, List[dict]]: The rows per destination, in the order they arrived.
    """
    grouped: Dict[str, List[dict]] = {}
    for data in batch:
        route = None
        row: Any
        if route_key is not None:
            data, route = take_route(extract_dict(data), route_key)
        if router is not None:
            target = router(data)
            if not target:
                continue
            record = extract_dict(data)
            # A label record keeps its envelope: it is not an event of the model
            plain = not envelope and record.get("stream_type") != "label"
            row = record.get("value") if plain else record
        else:
            target = route or default_target
            if not target:
                if (
                    require_route
                    and skipped is not None
                    and extract_dict(data).get("stream_type") != "telemetry"
                ):
                    skipped["no_route"] += 1
                continue
            record = extract_dict(data)
            if record.get("stream_type") == "telemetry":
                continue
            plain = not envelope and record.get("stream_type") != "label"
            row = record.get("value") if plain else flatten_record(record)
        if plain:
            if not isinstance(row, dict):
                if skipped is not None:
                    skipped["not_mapping"] += 1
                continue
            row = dict(row)
        grouped.setdefault(str(target), []).append(row)
    return grouped


def rows_table(records: List[dict], pa: Any) -> Any:
    """
    Builds an Arrow table with a column for every key of every row.

    `pa.Table.from_pylist` takes its columns from the first row only, so a key that
    first appears in a later row would be dropped. A row without a column holds null.

    Args:
        records (List[dict]): The rows of one destination.
        pa (Any): Injected reference to the `pyarrow` module.

    Returns:
        Any: The `pyarrow.Table`, its columns in the order they first appear.
    """
    columns = dict.fromkeys(key for record in records for key in record)
    return pa.Table.from_pydict(
        {column: [record.get(column) for record in records] for column in columns}
    )


def widen_schema(known: Any, batch: Any, pa: Any) -> Any:
    """
    Returns `known` with the columns of `batch` it lacks added at the end.

    A column `known` has only as null, because every value so far was missing,
    takes the batch's type, and an integer column that the batch gives a float
    becomes a double. Keys nested inside a struct are not widened: the first batch
    with the struct fixes them.

    Args:
        known (Any): The schema written so far, or None before the first write.
        batch (Any): The schema of the batch to write.
        pa (Any): Injected reference to the `pyarrow` module.

    Returns:
        Any: The `pyarrow.Schema` to cast the batch to.
    """
    if known is None:
        return batch
    fields = []
    for field in known:
        if field.name in batch.names:
            new = batch.field(field.name).type
            if pa.types.is_null(field.type):
                field = batch.field(field.name)
            elif pa.types.is_integer(field.type) and pa.types.is_floating(new):
                # A float after integers widens the column to double
                field = field.with_type(pa.float64())
        fields.append(field)
    fields += [field for field in batch if field.name not in known.names]
    return pa.schema(fields)


def to_schema(table: Any, schema: Any, pa: Any) -> Any:
    """
    Casts `table` to `schema`, adding a null column for each one it lacks.

    Args:
        table (Any): The batch's `pyarrow.Table`.
        schema (Any): The target `pyarrow.Schema`, a superset of its columns.
        pa (Any): Injected reference to the `pyarrow` module.

    Returns:
        Any: The cast `pyarrow.Table`, its columns in the schema's order.
    """
    for field in schema:
        if field.name not in table.column_names:
            table = table.append_column(
                field.name, pa.nulls(table.num_rows, type=field.type)
            )
    return table.select(schema.names).cast(schema)


def parse_iso_time(value: Any, kind: str) -> Any:
    """
    Converts an ISO time string into the Python value a time column takes.

    Records carry their logical time as an ISO string, and sinks with typed columns
    (PyArrow, asyncpg) reject a string for a timestamp or date column. Anything
    that is not a string, or a string that is not an ISO time, is returned
    unchanged, so the sink reports it as it would have done before.

    Args:
        value (Any): The field's value.
        kind (str): `timestamp` for a column without a time zone, `timestamptz`
            for one with a time zone, or `date`.

    Returns:
        Any: A `datetime` or `date`, or `value` itself when it is not converted.
    """
    if not isinstance(value, str):
        return value
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    if kind == "date":
        return parsed.date()
    if kind == "timestamp" and parsed.tzinfo is not None:
        # A column without a time zone holds UTC wall time.
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def parse_iso_columns(records: List[dict], columns: Dict[str, str]) -> List[dict]:
    """
    Applies `parse_iso_time` to the time columns of each record.

    A record that needs a conversion is copied first, because other egress
    providers hold the same dictionary.

    Args:
        records (List[dict]): The rows bound for one table.
        columns (Dict[str, str]): Column name to kind, as `parse_iso_time` takes it.

    Returns:
        List[dict]: The rows, with ISO strings in those columns converted.
    """
    if not columns:
        return records
    parsed = []
    for record in records:
        if any(isinstance(record.get(name), str) for name in columns):
            record = dict(record)
            for name, kind in columns.items():
                if name in record:
                    record[name] = parse_iso_time(record[name], kind)
        parsed.append(record)
    return parsed


class BaseEgress:
    """
    Base class for all egress providers in the simulation.

    Egress providers act as asynchronous bridges that consume processed
    simulation data (telemetry and events) from a thread-safe internal queue
    and transmit it to external destinations such as Kafka, databases,
    or the console.

    Attributes:
        envelope (bool): False when the provider was added with `envelope=False`: it
            writes each event's `value` alone, and the environment sends it no
            telemetry record and no `queued` or `started` record.
    """

    envelope: bool = True

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        Listens to the internal queue and pushes data to an external sink.

        This method should contain an asynchronous loop that polls the
        provided queue and handles the networking/I/O logic specific
        to the destination system.

        Args:
            egress_queue: A thread-safe queue containing batches of
                dictionaries to be exported.

        Raises:
            NotImplementedError: If the subclass does not override this method.
        """
        raise NotImplementedError("Subclasses must implement the run method.")

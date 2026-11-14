import contextlib
import importlib
import importlib.util
import logging
import queue
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, cast

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


def _is_integer(value: Any) -> bool:
    """Whether `value` is an integer and not a boolean."""
    return isinstance(value, int) and not isinstance(value, bool)


_READING_TIMES = ("now", "peek")
_READING_PATHS = ("system.simulation.step", "system.simulation.segment")


def as_doubles(record: dict) -> dict:
    """
    Returns a row with the integers of the fields that were floats at 0.28.0 as floats.

    A number keeps its written type in a record, so a statistic such as `capacity` or
    `level`, the `usage_since` of a `preempted` event, and the `now` and `peek` of a
    `run.steps` or `run.segments` reading are integers when their inputs are. A table
    whose columns come from its first batch would then fix the telemetry `value`, the
    `usage_since` or a time as an integer, and a later fraction would not fit. The
    schema a sink infers therefore reads the `value` of a telemetry row when it is a
    number, the `now` and `peek` of the `system.simulation.step` and
    `system.simulation.segment` readings, and the `usage_since` of a `preempted` row
    (flat or inside `value`) as doubles. The row is copied, because every egress
    provider receives the same dictionary objects.

    Args:
        record (dict): A row of a table.

    Returns:
        dict: A copy with those fields as floats, or the row itself when it has none
            to change.
    """
    value = record.get("value")
    if record.get("stream_type") == "telemetry":
        if _is_integer(value):
            return {**record, "value": float(cast(int, value))}
        if (
            isinstance(value, dict)
            and record.get("path_id") in _READING_PATHS
            and any(_is_integer(value.get(key)) for key in _READING_TIMES)
        ):
            return {
                **record,
                "value": {
                    **value,
                    **{
                        key: float(value[key])
                        for key in _READING_TIMES
                        if _is_integer(value.get(key))
                    },
                },
            }
        return record
    if record.get("status") == "preempted" and _is_integer(record.get("usage_since")):
        return {**record, "usage_since": float(record["usage_since"])}
    if (
        isinstance(value, dict)
        and value.get("status") == "preempted"
        and _is_integer(value.get("usage_since"))
    ):
        return {
            **record,
            "value": {**value, "usage_since": float(value["usage_since"])},
        }
    return record


def _conversion_errors() -> tuple:
    """The Arrow errors that a value which does not fit a column raises."""
    import pyarrow as pa

    return (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError)


def write_error(exc: Exception, records: List[dict], known: Any = None) -> str:
    """
    Returns the text of a write error, with the cause when it is a mix of statistics
    and mappings.

    A table or a file has one type for its `value` column. A statistic gives a number,
    and an event or a `run.steps` or `run.segments` reading gives a mapping, so a
    router that sends both to one destination makes the second kind unwritable. The
    hint is added only to an Arrow conversion error, when the rows of the batch that
    hold a `value` that is not null hold both a number and a mapping, or hold one kind
    that is not the kind (a mapping or not) of the `value` column already written.
    Any other error, or a batch with no such value, gets no hint.

    Args:
        exc (Exception): The error.
        records (List[dict]): The rows that were not written.
        known (Any): The Arrow schema already written for the destination, or None.

    Returns:
        str: The error text, with a hint when the cause is the mix.
    """
    text = str(exc).rstrip(".")
    if not isinstance(exc, _conversion_errors()):
        return text
    values = [
        record["value"]
        for record in records
        if isinstance(record, dict)
        and record.get("stream_type") in ("telemetry", "event")
        and record.get("value") is not None
    ]
    if not values:
        return text
    mapping = [isinstance(value, dict) for value in values]
    mixed = any(mapping) and not all(mapping)
    if not mixed and known is not None and "value" in known.names:
        import pyarrow as pa

        written = pa.types.is_struct(known.field("value").type)
        mixed = written != all(mapping)
    if mixed:
        text += (
            ". A statistic has a number as its value, and an event or a step or "
            "segment reading has a mapping, and one table or file cannot hold both, "
            "so route them to different tables or paths"
        )
    return text


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
            without its routing key, `no_key` for a record whose table has a key
            column it lacks or holds as null, `no_namespace` for a table name with no
            namespace, `not_text` for a record whose routing key holds something other
            than text, and `not_mapping` for a value that is not a mapping, which is
            warned about once per sink.
        route_key (Optional[str]): The routing key a `no_route` record lacks.
    """
    reasons = {
        "no_route": f"had no {route_key}",
        "no_key": "had a null or missing key column of their table, which a rerun "
        "would insert again because a null key never matches",
        "no_namespace": "named a table with no namespace",
        "not_text": f"had a {route_key} that is not text",
        "not_mapping": "had a value that is not a mapping, which this sink cannot "
        "write as a row",
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


def keep_keyed(records: List[dict], keys: List[str], skipped: Counter) -> List[dict]:
    """
    Returns the records whose key columns are all present and not null.

    A null key never matches in a unique index or an upsert, so a rerun would insert
    the record again, and under a primary key it would fail the whole statement. The
    records left out are counted in `skipped` as `no_key`, for `warn_skipped`.

    Args:
        records (List[dict]): The rows bound for one table.
        keys (List[str]): The table's key columns.
        skipped (Counter): Counts the records skipped, by reason.

    Returns:
        List[dict]: The records with a value in every key column, in their order.
    """
    kept = [r for r in records if all(r.get(key) is not None for key in keys)]
    skipped["no_key"] += len(records) - len(kept)
    return kept


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
    The `value` of a telemetry row and the `usage_since` of a `preempted` row are
    doubles even when the number is an integer (see `as_doubles`), so a table fixed
    from the first batch takes a later fraction.

    Args:
        records (List[dict]): The rows of one destination.
        pa (Any): Injected reference to the `pyarrow` module.

    Returns:
        Any: The `pyarrow.Table`, its columns in the order they first appear.
    """
    records = [as_doubles(record) for record in records]
    columns = dict.fromkeys(key for record in records for key in record)
    return pa.Table.from_pydict(
        {column: [record.get(column) for record in records] for column in columns}
    )


def require_packages(owner: str, extra: str, *packages: str) -> None:
    """
    Refuses to build a connector whose packages are not installed.

    A storage connector imports its packages only when it writes, so without this
    a run with a missing extra would start, write nothing, and stop its sink with
    a log record that no handler may print.

    Args:
        owner (str): The connector's class name, for the message.
        extra (str): The extra that installs the packages, such as `parquet`.
        *packages (str): The top-level packages the connector writes with.

    Raises:
        ImportError: If any of `packages` is not installed. The message names
            the extra to install.
    """
    missing = []
    for package in packages:
        if importlib.util.find_spec(package) is None:
            missing.append(package)
    if missing:
        raise ImportError(
            f"{owner} needs {' and '.join(missing)}, which is not installed. "
            f'Install it with pip install "desflow[{extra}]"'
        )


def load_arrow(*modules: str, tables: bool = True) -> None:
    """
    Imports `pyarrow` and `modules`, and with `tables` builds one small table, so
    that what `pyarrow` loads on its first table (pandas, when it is installed) is
    loaded too.

    A storage sink calls it from `_load_libraries`. A missing package is left to
    the sink's `run`, which raises the `ImportError` that names its extra.

    Args:
        *modules (str): Further modules to import, such as `pyarrow.parquet`.
        tables (bool): Whether the sink builds Arrow tables, so the first one is
            built here.
    """
    try:
        import pyarrow as pa

        for module in modules:
            importlib.import_module(module)
    except ImportError:
        return
    if tables:
        rows_table([{"x": 1}], pa)


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
    # Set by the environment when teardown begins and cleared when a run starts. A sink
    # whose service is down drops its queued batches then, because each one would wait
    # for the service in turn and be lost anyway.
    _ending: bool = False

    def _drop_backlog(
        self, egress_queue: queue.Queue, name: str, service_unavailable: bool
    ) -> bool:
        """Drops every queued batch after a failed write if the service is down at teardown.

        A write that the service refused for that batch alone, while the service is up,
        drops nothing more than itself: the other batches can still be written.

        Args:
            egress_queue: The queue the sink reads.
            name: The sink's name for the log line.
            service_unavailable: Whether the error means the service cannot be
                reached, not that it refused this batch.

        Returns:
            bool: True when teardown has begun, the service is unavailable and the queue
                was emptied; the batch that failed counts as lost too. False means the
                sink waits and goes on as it does while the run is going.
        """
        if not (self._ending and service_unavailable):
            return False
        lost = 1
        while True:
            try:
                egress_queue.get_nowait()
            except queue.Empty:
                break
            lost += 1
        logger.warning(
            "%s could not write at teardown, so %d batches were dropped.", name, lost
        )
        return True

    def _load_libraries(self) -> None:
        """Imports the libraries the provider writes with, before the run starts.

        `setup_egress` calls it in the thread that sets up the egress, before the
        provider's own thread starts. A library imported for the first time holds
        the interpreter lock while it loads its compiled code, and a first load of
        `pyarrow` can take seconds. Loaded in the provider's thread during a
        paced run, it stops the simulation for that time, and a strict run fails
        with "Simulation too slow for real time". The default imports nothing.
        """

    @contextlib.contextmanager
    def _writing(self) -> Iterator[None]:
        """Counts a batch in `active_tasks` while the block writes it.

        Teardown waits until every provider's `active_tasks` is 0, or it would cancel
        the last batch. Teardown reads the count with `getattr` and a default of 0, so
        it does not wait for a provider that has never written or a provider of the
        user's own that keeps no count.
        """
        self.active_tasks = getattr(self, "active_tasks", 0) + 1
        try:
            yield
        finally:
            self.active_tasks -= 1

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

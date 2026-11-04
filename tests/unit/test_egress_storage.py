import json
import re
from pathlib import Path

import pytest

# Attempt to import PyArrow, skip tests if not installed
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow import fs

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

from desflow.connectors.egress.storage import (
    JsonlStorageEgress,
    ParquetStorageEgress,
    _generate_chunk_filename,
)

pytestmark = pytest.mark.skipif(
    not HAS_PYARROW, reason="pyarrow is required for storage egress tests"
)


def test_generate_chunk_filename():
    """Test that the UUID chunking suffix is correctly injected before the extension."""
    base_path = "data/simulation_run/events.parquet"
    chunked_path = _generate_chunk_filename(base_path)

    # Use regex to verify it looks like 'data/simulation_run/events_a1b2c3d4.parquet'
    match = re.match(
        r"^data/simulation_run/events_([a-f0-9]{8})\.parquet$", chunked_path
    )

    assert match is not None, (
        f"Filename {chunked_path} did not match expected chunk pattern."
    )
    assert len(match.group(1)) == 8  # Verify it's an 8-character hex


def test_jsonl_storage_routing_and_writing(tmp_path: Path):
    """Test JSONL egress routing, dropping, and file writing."""

    # 1. Define a router that splits events/telemetry and drops lag
    def test_router(data: dict) -> str | None:
        if data.get("path_id") == "lag":
            return None
        if data.get("stream_type") == "telemetry":
            return str(tmp_path / "telemetry.jsonl")
        return str(tmp_path / "events.jsonl")

    # 2. Initialize egress with local file system
    egress = JsonlStorageEgress(path_router=test_router)
    egress.filesystem = fs.LocalFileSystem()

    # 3. Create a dummy batch with mixed data types
    batch = [
        {"stream_type": "event", "val": 1},
        {"stream_type": "telemetry", "val": 2},
        {"stream_type": "telemetry", "path_id": "lag", "val": 3},  # Should be dropped
        {"stream_type": "event", "val": 4},
    ]

    # 4. Execute the batch write
    egress._write_batch(batch)

    # 5. Verify the files were created with UUID chunks
    written_files = list(tmp_path.glob("*.jsonl"))
    assert len(written_files) == 2, (
        "Expected exactly two chunked files (events and telemetry)"
    )

    events_files = list(tmp_path.glob("events_*.jsonl"))
    telemetry_files = list(tmp_path.glob("telemetry_*.jsonl"))

    assert len(events_files) == 1
    assert len(telemetry_files) == 1

    # 6. Verify contents
    with open(events_files[0], "r") as f:
        events_data = [json.loads(line) for line in f]
        assert len(events_data) == 2
        assert events_data[0]["val"] == 1
        assert events_data[1]["val"] == 4

    with open(telemetry_files[0], "r") as f:
        telemetry_data = [json.loads(line) for line in f]
        assert len(telemetry_data) == 1
        assert (
            telemetry_data[0]["val"] == 2
        )  # The lag metric (val 3) was correctly dropped


def test_parquet_storage_schema_drift(tmp_path: Path):
    """Test Parquet egress prevents schema drift between consecutive batches."""

    # Send everything to one file prefix
    base_path = str(tmp_path / "metrics.parquet")

    egress = ParquetStorageEgress(default_path=base_path)
    egress.filesystem = fs.LocalFileSystem()

    # BATCH 1 (Defines the Schema)
    # The 'value' is a FLOAT here
    batch_1 = [{"metric": "temp", "value": 98.6}, {"metric": "speed", "value": 104.2}]
    egress._write_batch(batch_1, pa, pq)

    # BATCH 2 (Introduces Schema Drift)
    # The 'value' is an INTEGER here!
    # Without drift protection, pyarrow would crash or create incompatible files.
    batch_2 = [{"metric": "temp", "value": 99}, {"metric": "speed", "value": 105}]
    egress._write_batch(batch_2, pa, pq)

    # Verification
    written_files = list(tmp_path.glob("metrics_*.parquet"))
    assert len(written_files) == 2, "Expected two separate Parquet chunks."

    # Read the files back into a single PyArrow Dataset
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(tmp_path), format="parquet")
    table = dataset.to_table()

    # 1. Ensure all 4 rows survived
    assert table.num_rows == 4

    # 2. Ensure strict schema typing was enforced!
    # The 'value' column should be purely DOUBLE/FLOAT64, successfully casting the integers from Batch 2.
    value_field = table.schema.field("value")
    assert pa.types.is_float64(value_field.type), (
        f"Expected float64, got {value_field.type}"
    )

    # 3. Verify the actual casted data
    val_list = table.column("value").to_pylist()
    assert sorted(val_list) == sorted([98.6, 104.2, 99.0, 105.0])


EVENT = {
    "stream_type": "event",
    "sim_ts": 1.0,
    "timestamp": "2026-01-01T00:00:01.000",
    "key": "task-1",
    "value": {"path_id": "Shop.service.packing", "status": "finished"},
}
TELEMETRY = {
    "stream_type": "telemetry",
    "sim_ts": 1.0,
    "timestamp": "2026-01-01T00:00:01.000",
    "path_id": "Shop.packers.utilization",
    "value": 0.5,
}


def test_parquet_without_router_writes_flat_events_only(tmp_path: Path):
    """Without a router, telemetry is left out and the event payload becomes columns."""
    egress = ParquetStorageEgress(default_path=str(tmp_path / "events.parquet"))
    egress.filesystem = fs.LocalFileSystem()

    egress._write_batch([EVENT, TELEMETRY], pa, pq)

    [chunk] = tmp_path.glob("events_*.parquet")
    rows = pq.read_table(chunk).to_pylist()
    assert rows == [
        {
            "stream_type": "event",
            "sim_ts": 1.0,
            "timestamp": "2026-01-01T00:00:01.000",
            "key": "task-1",
            "path_id": "Shop.service.packing",
            "status": "finished",
        }
    ]
    # Other sinks receive the same record, so it must not change.
    assert EVENT["value"] == {"path_id": "Shop.service.packing", "status": "finished"}


def test_jsonl_without_router_writes_flat_events_only(tmp_path: Path):
    """The JSONL writer applies the same default as the Parquet writer."""
    egress = JsonlStorageEgress(default_path=str(tmp_path / "events.jsonl"))
    egress.filesystem = fs.LocalFileSystem()

    egress._write_batch([EVENT, TELEMETRY])

    [chunk] = tmp_path.glob("events_*.jsonl")
    rows = [json.loads(line) for line in chunk.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["status"] == "finished" and "value" not in rows[0]


def test_parquet_with_router_writes_records_as_the_router_leaves_them(tmp_path: Path):
    """A router keeps today's behaviour: nothing is flattened or left out for it."""
    target = str(tmp_path / "all.parquet")
    egress = ParquetStorageEgress(path_router=lambda data: target)
    egress.filesystem = fs.LocalFileSystem()

    egress._write_batch([EVENT, dict(EVENT, key="task-2")], pa, pq)

    [chunk] = tmp_path.glob("all_*.parquet")
    rows = pq.read_table(chunk).to_pylist()
    assert [r["value"]["status"] for r in rows] == ["finished", "finished"]


def test_parse_iso_time_handles_each_kind():
    """Each column kind gets the Python value its sink accepts."""
    from datetime import date, datetime

    from desflow.connectors.egress.base import parse_iso_time

    assert parse_iso_time("2026-01-01T10:00:00+02:00", "timestamp") == datetime(
        2026, 1, 1, 8, 0
    )
    assert parse_iso_time("2026-01-01T10:00:00", "date") == date(2026, 1, 1)
    assert parse_iso_time("not a time", "timestamp") == "not a time"
    assert parse_iso_time(5, "timestamp") == 5


@pytest.mark.parametrize(
    ("endpoint", "extra", "host", "scheme"),
    [
        ("http://localhost:8333", {}, "localhost:8333", "http"),
        ("https://storage.example.com", {}, "storage.example.com", "https"),
        ("http://localhost:8333", {"scheme": "https"}, "localhost:8333", "https"),
    ],
    ids=["http", "https", "given-scheme-kept"],
)
def test_filesystem_mapping_builds_an_s3_filesystem(endpoint, extra, host, scheme):
    """An S3 mapping passes its keys to S3FileSystem; the endpoint sets the scheme unless one is given."""
    from desflow.connectors.egress.storage import _build_filesystem

    built = _build_filesystem(
        {
            "type": "s3",
            "endpoint_override": endpoint,
            "access_key": "user",
            "secret_key": "password",
            "region": "us-east-1",
            **extra,
        },
        fs,
    )

    assert isinstance(built, fs.S3FileSystem)
    options = built.__reduce__()[1][0]
    assert options["endpoint_override"] == host
    assert options["scheme"] == scheme
    assert options["access_key"] == "user"
    assert options["region"] == "us-east-1"


def test_filesystem_mapping_builds_a_gcs_filesystem():
    """A GCS mapping splits the endpoint scheme and converts the two values a blueprint gives as a string or a number."""
    from datetime import datetime, timedelta, timezone

    from desflow.connectors.egress.storage import _build_filesystem

    built = _build_filesystem(
        {
            "type": "gcs",
            "anonymous": True,
            "endpoint_override": "http://localhost:4443",
            "retry_time_limit": 30,
        },
        fs,
    )
    assert isinstance(built, fs.GcsFileSystem)
    options = built.__reduce__()[1][0]
    assert options["endpoint_override"] == "localhost:4443"
    assert options["scheme"] == "http"
    assert options["retry_time_limit"] == timedelta(seconds=30)

    token = _build_filesystem(
        {
            "type": "gcs",
            "access_token": "token",
            "credential_token_expiration": "2030-01-01T00:00:00+00:00",
        },
        fs,
    )
    expiration = token.__reduce__()[1][0]["credential_token_expiration"]
    assert expiration == datetime(2030, 1, 1, tzinfo=timezone.utc)

    given = datetime(2030, 1, 1, tzinfo=timezone.utc)
    kept = _build_filesystem(
        {"type": "gcs", "access_token": "token", "credential_token_expiration": given},
        fs,
    )
    assert kept.__reduce__()[1][0]["credential_token_expiration"] == given


@pytest.mark.parametrize(
    "written",
    [
        "2030-01-01T00:00:00+00:00",
        "2030-01-01T10:00:00+10:00",
        '"2030-01-01T10:00:00+10:00"',
    ],
    ids=["unquoted-utc", "unquoted-offset", "quoted-offset"],
)
def test_an_iso_time_in_a_blueprint_reaches_gcs_in_utc(written):
    """PyArrow ignores an offset, so a time with one is converted to UTC first."""
    from datetime import datetime, timezone

    import yaml

    from desflow.blueprint.loader import BlueprintLoader
    from desflow.connectors.egress.storage import _build_filesystem

    mapping = yaml.load(
        f"type: gcs\naccess_token: token\ncredential_token_expiration: {written}\n",
        Loader=BlueprintLoader,
    )

    built = _build_filesystem(mapping, fs)
    expiration = built.__reduce__()[1][0]["credential_token_expiration"]
    assert expiration == datetime(2030, 1, 1, tzinfo=timezone.utc)


def test_filesystem_mapping_builds_an_azure_filesystem():
    """An Azure mapping passes its keys as they are, including Azurite's endpoint."""
    from desflow.connectors.egress.storage import _build_filesystem

    built = _build_filesystem(
        {
            "type": "azure",
            "account_name": "devstoreaccount1",
            "account_key": "key",
            "blob_storage_authority": "127.0.0.1:10000",
            "blob_storage_scheme": "http",
        },
        fs,
    )

    assert isinstance(built, fs.AzureFileSystem)
    options = built.__reduce__()[1][0]
    assert options["account_name"] == "devstoreaccount1"
    assert options["blob_storage_authority"] == "127.0.0.1:10000"
    assert options["blob_storage_scheme"] == "http"


def test_filesystem_defaults_and_objects():
    """None is the local disk, a mapping without a type is local, an object is kept."""
    from desflow.connectors.egress.storage import _build_filesystem

    given = fs.LocalFileSystem()
    assert isinstance(_build_filesystem(None, fs), fs.LocalFileSystem)
    assert isinstance(_build_filesystem({}, fs), fs.LocalFileSystem)
    assert _build_filesystem(given, fs) is given


def test_empty_filesystem_values_are_left_out():
    """Values an unset environment variable leaves empty fall back to the defaults."""
    from desflow.connectors.egress.storage import _build_filesystem

    empty = {"type": "", "endpoint_override": "", "access_key": None}
    assert isinstance(_build_filesystem(empty, fs), fs.LocalFileSystem)
    ParquetStorageEgress(default_path="x.parquet", filesystem=empty)

    built = _build_filesystem({"type": "s3", "endpoint_override": ""}, fs)
    assert isinstance(built, fs.S3FileSystem)
    assert built.__reduce__()[1][0]["endpoint_override"] == ""


def test_unknown_filesystem_type_is_refused_at_construction():
    """A misspelt type fails when the egress is built, not on the first write."""
    message = "'gs'.*local, s3, gcs, azure"
    with pytest.raises(ValueError, match=message):
        ParquetStorageEgress(default_path="x.parquet", filesystem={"type": "gs"})
    with pytest.raises(ValueError, match=message):
        JsonlStorageEgress(default_path="x.jsonl", filesystem={"type": "gs"})


@pytest.mark.asyncio
async def test_run_builds_the_mapping_and_creates_the_folder(tmp_path: Path):
    """With a mapping and a folder that does not exist, the first write still lands."""
    import asyncio
    import queue

    target = tmp_path / "new" / "deeper" / "events.parquet"
    egress = ParquetStorageEgress(
        default_path=str(target), filesystem={"type": "local"}
    )
    q: queue.Queue = queue.Queue()
    q.put([EVENT])
    # The first table imports pandas, which takes seconds in a new environment;
    # `setup_egress` loads it before the run, and so does the test,
    # so the wait below times the write alone
    egress._load_libraries()

    task = asyncio.create_task(egress.run(q))
    for _ in range(50):
        await asyncio.sleep(0.05)
        if list(target.parent.glob("events_*.parquet")):
            break
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert isinstance(egress.filesystem, fs.LocalFileSystem)
    [chunk] = target.parent.glob("events_*.parquet")
    assert pq.read_table(chunk).to_pylist()[0]["status"] == "finished"


def test_jsonl_creates_the_folder_once(tmp_path: Path):
    """The folder is created on the first write and not asked for again."""
    from unittest.mock import MagicMock

    local = fs.LocalFileSystem()
    spy = MagicMock(wraps=local)
    egress = JsonlStorageEgress(default_path=str(tmp_path / "a" / "events.jsonl"))
    egress.filesystem = spy

    egress._write_batch([EVENT])
    egress._write_batch([EVENT])

    spy.create_dir.assert_called_once_with((tmp_path / "a").as_posix(), recursive=True)
    assert len(list((tmp_path / "a").glob("events_*.jsonl"))) == 2


def test_a_folder_that_cannot_be_created_does_not_stop_the_write():
    """Object storage takes a key under a prefix that was never created."""
    from unittest.mock import MagicMock

    store = MagicMock()
    store.create_dir.side_effect = OSError("bucket creation is disabled")
    egress = JsonlStorageEgress(default_path="bucket/events.jsonl")
    egress.filesystem = store

    egress._write_batch([EVENT])

    store.open_output_stream.assert_called_once()


def test_storage_sinks_drop_reserved_keys(tmp_path: Path, caplog):
    """Parquet and JSONL write no reserved payload key, and warn once per key."""
    record: dict = {
        "stream_type": "event",
        "sim_ts": 1.0,
        "timestamp": "2026-01-01T00:00:01.000+00:00",
        "key": "orders-1",
        "value": {"__stream__": "orders", "status": "placed"},
    }
    parquet = ParquetStorageEgress(default_path=str(tmp_path / "events.parquet"))
    parquet.filesystem = fs.LocalFileSystem()
    parquet._write_batch([record, record], pa, pq)
    jsonl = JsonlStorageEgress(default_path=str(tmp_path / "events.jsonl"))
    jsonl.filesystem = fs.LocalFileSystem()
    jsonl._write_batch([record, record])

    [chunk] = tmp_path.glob("events_*.parquet")
    assert "__stream__" not in pq.read_table(chunk).column_names
    [lines] = tmp_path.glob("events_*.jsonl")
    assert all(
        "__stream__" not in json.loads(line) for line in lines.read_text().splitlines()
    )
    warnings = [
        r.getMessage()
        for r in caplog.records
        if "drops the payload key" in r.getMessage()
    ]
    assert [w.split(" drops")[0] for w in warnings] == [
        "ParquetStorageEgress",
        "JsonlStorageEgress",
    ]
    assert record["value"]["__stream__"] == "orders"


def test_parquet_writes_every_column_of_every_batch(tmp_path: Path):
    """Columns come from every row, not the first, and a column that first appears
    in a later batch widens the file schema; earlier rows read it as null."""
    import pyarrow.dataset as ds

    egress = ParquetStorageEgress(default_path=str(tmp_path / "events.parquet"))
    egress.filesystem = fs.LocalFileSystem()
    egress._write_batch([{"a": 1}, {"a": 2, "b": "x"}], pa, pq)
    egress._write_batch([{"a": 3, "c": 1.5}], pa, pq)

    # The file names carry a random id, so each file is told by its first value
    files = sorted(
        tmp_path.glob("events_*.parquet"),
        key=lambda f: pq.read_table(f).column("a")[0].as_py(),
    )
    assert len(files) == 2
    assert pq.read_table(files[0]).column_names == ["a", "b"]
    assert pq.read_table(files[1]).column_names == ["a", "b", "c"]
    schema = pa.unify_schemas([pq.read_schema(f) for f in files])
    rows = ds.dataset(files, schema=schema).to_table().to_pylist()
    assert sorted(rows, key=lambda r: r["a"]) == [
        {"a": 1, "b": None, "c": None},
        {"a": 2, "b": "x", "c": None},
        {"a": 3, "b": None, "c": 1.5},
    ]


def test_parquet_write_error_is_logged_and_the_sink_goes_on(tmp_path: Path, caplog):
    """A batch that cannot be written is logged at ERROR; later batches still are."""
    egress = ParquetStorageEgress(default_path=str(tmp_path / "events.parquet"))
    egress.filesystem = fs.LocalFileSystem()
    egress._write_batch([{"a": 1}], pa, pq)
    with caplog.at_level("ERROR"):
        egress._write_batch([{"a": "not a number"}], pa, pq)
    assert any(
        r.levelname == "ERROR" and "events.parquet" in r.getMessage()
        for r in caplog.records
    )
    egress._write_batch([{"a": 2}], pa, pq)
    assert len(list(tmp_path.glob("events_*.parquet"))) == 2


def test_the_shut_down_message_counts_only_batches_written(tmp_path: Path, caplog):
    """A batch that fails is not counted among the chunks written."""
    import asyncio
    import queue

    egress = ParquetStorageEgress(default_path=str(tmp_path / "events.parquet"))
    egress.filesystem = fs.LocalFileSystem()
    q: queue.Queue = queue.Queue()
    for batch in ([{"a": 1}], [{"a": "not a number"}], [{"a": 2}]):
        q.put(batch)

    async def drain():
        task = asyncio.create_task(egress.run(q))
        while not q.empty() or getattr(egress, "active_tasks", 0):
            await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    with caplog.at_level("INFO"):
        asyncio.run(drain())
    assert "Successfully wrote 2 total chunks" in caplog.text


def test_the_jsonl_heartbeat_names_the_jsonl_writer(tmp_path: Path, caplog):
    """Every 10 batches the JSONL sink logs its progress under its own name."""
    import asyncio
    import queue

    egress = JsonlStorageEgress(default_path=str(tmp_path / "events.jsonl"))
    egress.filesystem = fs.LocalFileSystem()
    q: queue.Queue = queue.Queue()
    for i in range(10):
        q.put([{"a": i}])

    async def drain():
        task = asyncio.create_task(egress.run(q))
        while not q.empty() or getattr(egress, "active_tasks", 0):
            await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    with caplog.at_level("INFO"):
        asyncio.run(drain())
    assert "JSONL Writer: Processed 10 batches." in caplog.text
    assert "Parquet Writer" not in caplog.text


def test_an_integer_column_widens_to_float(tmp_path: Path):
    """A column written as integers takes a later batch's floats: the later files
    hold doubles, and the earlier integers read back as the same numbers."""
    import pyarrow.dataset as ds

    egress = ParquetStorageEgress(default_path=str(tmp_path / "events.parquet"))
    egress.filesystem = fs.LocalFileSystem()
    assert egress._write_batch([{"a": 1}], pa, pq) == 1
    assert egress._write_batch([{"a": 1.5}], pa, pq) == 1
    files = list(tmp_path.glob("events_*.parquet"))
    schema = pa.unify_schemas(
        [pq.read_schema(f) for f in files], promote_options="permissive"
    )
    values = ds.dataset(files, schema=schema).to_table().column("a").to_pylist()
    assert sorted(values) == [1.0, 1.5]


@pytest.mark.parametrize(
    "batches",
    [[[15], [15.5], [16.25]], [[15, 16, 17], [15.5, 16.25]]],
    ids=["batches of one row", "batches of several rows"],
)
def test_parquet_telemetry_value_is_a_double_in_every_file(tmp_path: Path, batches):
    """An integer statistic first does not write an int64 file: the folder reads as
    one table whatever the order of its files, and keeps a later fraction."""
    egress = ParquetStorageEgress(path_router=lambda data: str(tmp_path / "t.parquet"))
    egress.filesystem = fs.LocalFileSystem()
    record = {"stream_type": "telemetry", "path_id": "tank.level"}

    for batch in batches:
        rows = [dict(record, value=v) for v in batch]
        assert egress._write_batch(rows, pa, pq) == 1

    files = list(tmp_path.glob("t_*.parquet"))
    assert len(files) == len(batches)
    assert {pq.read_schema(f).field("value").type for f in files} == {pa.float64()}
    values = pq.read_table(tmp_path).column("value").to_pylist()
    assert sorted(values) == sorted(float(v) for b in batches for v in b)


def test_parquet_usage_since_is_a_double_in_every_file(tmp_path: Path):
    """The time a preempted unit was granted is an integer when the clock is: the
    first file is a double too, so the folder reads as one table."""
    egress = ParquetStorageEgress(default_path=str(tmp_path / "e.parquet"))
    egress.filesystem = fs.LocalFileSystem()
    row = {"stream_type": "event", "key": "a-1", "task": "t", "value": {}}
    status = {"status": "preempted", "resource": "m"}

    for since in (3, 3.5):
        record = dict(row, value=dict(status, usage_since=since))
        assert egress._write_batch([record], pa, pq) == 1

    files = list(tmp_path.glob("e_*.parquet"))
    assert {pq.read_schema(f).field("usage_since").type for f in files} == {
        pa.float64()
    }
    assert sorted(pq.read_table(tmp_path).column("usage_since").to_pylist()) == [
        3.0,
        3.5,
    ]


def test_a_file_sink_without_pyarrow_says_so_at_error(tmp_path: Path, caplog):
    """Without pyarrow, JsonlStorageEgress stops at once; the run logs why at ERROR,
    with the install hint, rather than only that the queues stopped draining."""
    import sys
    from unittest import mock

    from desflow import DynamicRealtimeEnvironment

    env = DynamicRealtimeEnvironment(factor=0)
    with mock.patch.dict(sys.modules, {"pyarrow": None}):
        env.setup_egress(
            [JsonlStorageEgress(default_path=str(tmp_path / "events.jsonl"))]
        )
        with caplog.at_level("ERROR"):
            env.run(until=1)
            env.teardown()
    errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
    assert any("pip install desflow[parquet]" in m for m in errors), errors


def _event(value, key="orders-1", stream_type="event"):
    return {
        "stream_type": stream_type,
        "sim_ts": 1.0,
        "timestamp": "2026-01-01T00:00:01.000+00:00",
        "key": key,
        "value": value,
    }


def test_path_field_selects_the_file_and_is_removed(tmp_path: Path):
    """`__path__` in an event's value is the file it is written to, in a folder that
    is created for it, and the column is not written. A record without it goes to
    `default_path`."""
    batch = [
        _event({"__path__": f"{tmp_path}/packed/a.jsonl", "n": 1}),
        _event({"n": 2}),
    ]
    jsonl = JsonlStorageEgress(default_path=str(tmp_path / "rest.jsonl"))
    jsonl.filesystem = fs.LocalFileSystem()
    jsonl._write_batch(batch)
    parquet = ParquetStorageEgress(default_path=str(tmp_path / "rest.parquet"))
    parquet.filesystem = fs.LocalFileSystem()
    parquet._write_batch(
        [
            _event({"__path__": f"{tmp_path}/deep/er/a.parquet", "n": 1}),
            _event({"n": 2}),
        ],
        pa,
        pq,
    )

    [routed] = (tmp_path / "packed").glob("a_*.jsonl")
    assert json.loads(routed.read_text())["n"] == 1
    assert "__path__" not in routed.read_text()
    [rest] = tmp_path.glob("rest_*.jsonl")
    assert json.loads(rest.read_text())["n"] == 2
    [table] = (tmp_path / "deep" / "er").glob("a_*.parquet")
    assert pq.read_table(table).column_names == [
        "stream_type",
        "sim_ts",
        "timestamp",
        "key",
        "n",
    ]
    [rest_table] = tmp_path.glob("rest_*.parquet")
    assert pq.read_table(rest_table).to_pylist()[0]["n"] == 2


def test_path_router_wins_over_the_path_field(tmp_path: Path):
    """A `path_router` is the user's explicit function. The key is still removed."""
    egress = JsonlStorageEgress(path_router=lambda d: str(tmp_path / "routed.jsonl"))
    egress.filesystem = fs.LocalFileSystem()

    egress._write_batch([_event({"__path__": "elsewhere.jsonl", "n": 1})])

    [written] = tmp_path.glob("routed_*.jsonl")
    assert "__path__" not in written.read_text()
    assert not list(tmp_path.glob("elsewhere*"))


def test_envelope_false_writes_the_value_as_the_row(tmp_path: Path, caplog):
    """With `envelope` off a row is the event's value. A value that is not a mapping,
    null included, is skipped, and one warning per sink says so, however many
    flushes meet one."""
    batch = [
        _event({"n": 1}),
        _event("text"),
        _event(None),
        _event({"__path__": f"{tmp_path}/by_path.jsonl", "n": 2}),
    ]
    jsonl = JsonlStorageEgress(default_path=str(tmp_path / "plain.jsonl"))
    jsonl.filesystem = fs.LocalFileSystem()
    jsonl.envelope = False

    jsonl._write_batch(batch)
    jsonl._write_batch(batch)

    plain = [
        json.loads(line)
        for path in tmp_path.glob("plain_*.jsonl")
        for line in path.read_text().splitlines()
    ]
    assert plain == [{"n": 1}, {"n": 1}]
    written = [
        json.loads(line)
        for path in tmp_path.glob("by_path_*.jsonl")
        for line in path.read_text().splitlines()
    ]
    assert written == [{"n": 2}, {"n": 2}]
    warnings = [r for r in caplog.records if "skipped records" in r.getMessage()]
    assert len(warnings) == 1
    assert "2 had a value that is not a mapping" in warnings[0].getMessage()

    parquet = ParquetStorageEgress(default_path=str(tmp_path / "plain.parquet"))
    parquet.filesystem = fs.LocalFileSystem()
    parquet.envelope = False
    parquet._write_batch([_event({"n": 1, "m": "x"})], pa, pq)
    [table] = tmp_path.glob("plain_*.parquet")
    assert pq.read_table(table).to_pylist() == [{"n": 1, "m": "x"}]


def test_envelope_on_writes_a_null_value_in_a_value_column(tmp_path: Path):
    """With `envelope` on, a null value is not a mapping to flatten, so Parquet
    writes it in a `value` column, which every row of that file then has, and JSONL
    writes `"value": null` in that record's line only."""
    batch = [_event({"id": 7}), _event(None, key="orders-2")]
    parquet = ParquetStorageEgress(default_path=str(tmp_path / "rows.parquet"))
    parquet.filesystem = fs.LocalFileSystem()
    jsonl = JsonlStorageEgress(default_path=str(tmp_path / "rows.jsonl"))
    jsonl.filesystem = fs.LocalFileSystem()

    parquet._write_batch(batch, pa, pq)
    jsonl._write_batch(batch)

    [table] = tmp_path.glob("rows_*.parquet")
    rows = pq.read_table(table).to_pylist()
    assert [(row["key"], row["id"], row["value"]) for row in rows] == [
        ("orders-1", 7, None),
        ("orders-2", None, None),
    ]
    [chunk] = tmp_path.glob("rows_*.jsonl")
    lines = [json.loads(line) for line in chunk.read_text().splitlines()]
    assert [(line["key"], line.get("id"), "value" in line) for line in lines] == [
        ("orders-1", 7, False),
        ("orders-2", None, True),
    ]
    assert lines[1]["value"] is None


def _label():
    return {
        "stream_type": "label",
        "key": "k",
        "sim_ts": 1.0,
        "value": {"fault": "slow_packing"},
    }


def test_envelope_false_leaves_a_label_record_whole(tmp_path: Path):
    """A label record is not an event of the model, so `envelope: false` keeps its
    envelope on the file sinks, as on Kafka, Redis, Console and Postgres."""
    jsonl = JsonlStorageEgress(default_path=str(tmp_path / "labels.jsonl"))
    jsonl.filesystem = fs.LocalFileSystem()
    jsonl.envelope = False
    jsonl._write_batch([_label()])
    [written] = tmp_path.glob("labels_*.jsonl")
    row = json.loads(written.read_text().splitlines()[0])
    assert row["stream_type"] == "label" and row["fault"] == "slow_packing"
    assert row["key"] == "k"

    parquet = ParquetStorageEgress(default_path=str(tmp_path / "labels.parquet"))
    parquet.filesystem = fs.LocalFileSystem()
    parquet.envelope = False
    parquet._write_batch([_label()], pa, pq)
    [table] = tmp_path.glob("labels_*.parquet")
    [stored] = pq.read_table(table).to_pylist()
    assert stored["stream_type"] == "label" and stored["fault"] == "slow_packing"

    routed = JsonlStorageEgress(path_router=lambda d: str(tmp_path / "routed.jsonl"))
    routed.filesystem = fs.LocalFileSystem()
    routed.envelope = False
    routed._write_batch([_label()])
    [again] = tmp_path.glob("routed_*.jsonl")
    assert json.loads(again.read_text().splitlines()[0])["stream_type"] == "label"


@pytest.mark.parametrize(
    "sink, loaded, not_loaded",
    [
        (
            "ParquetStorageEgress(default_path='x.parquet')",
            ["pyarrow.parquet", "pyarrow.fs", "pandas"],
            [],
        ),
        ("JsonlStorageEgress(default_path='x.jsonl')", ["pyarrow.fs"], ["pandas"]),
        (
            "IcebergStorageEgress(catalog={'type': 'sql'}, default_table='a.b')",
            ["pyarrow", "pyiceberg.io.pyarrow", "pyiceberg.catalog.sql", "pandas"],
            [],
        ),
        (
            "IcebergStorageEgress(catalog={'uri': 'sqlite:///x.db'}, "
            "default_table='a.b')",
            ["pyiceberg.io.pyarrow", "pyiceberg.catalog.sql"],
            ["pyiceberg.catalog.rest"],
        ),
    ],
)
def test_a_storage_sink_loads_its_libraries_before_the_run(sink, loaded, not_loaded):
    """In a new interpreter, `_load_libraries` imports what the sink's thread would
    import on its first write: `pyarrow`'s modules, and pandas, which `pyarrow` loads
    on its first table when it is installed. The JSONL sink builds no table, so it
    does not load pandas. The Iceberg sink also loads `pyiceberg.io.pyarrow` and the
    module of its catalog's type, given as `type` or read from the `uri` as PyIceberg
    reads it."""
    import importlib.util
    import subprocess
    import sys

    if importlib.util.find_spec("pandas") is None:
        loaded = [module for module in loaded if module != "pandas"]

    code = (
        "import sys\n"
        "from desflow.connectors.egress.storage import ParquetStorageEgress, "
        "JsonlStorageEgress\n"
        "from desflow.connectors.egress.iceberg import IcebergStorageEgress\n"
        f"sink = {sink}\n"
        f"before = [m for m in {loaded + not_loaded!r} if m in sys.modules]\n"
        "sink._load_libraries()\n"
        "print(before, sorted(m for m in "
        f"{loaded + not_loaded!r} if m in sys.modules))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert out.strip() == f"[] {sorted(loaded)!r}"

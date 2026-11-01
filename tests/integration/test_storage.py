import asyncio
import queue
import uuid

import orjson
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from desflow.connectors.egress.storage import (
    JsonlStorageEgress,
    ParquetStorageEgress,
    _build_filesystem,
)
from tests.integration.helpers import (
    check_round_trip,
    drain,
    write_and_read_back,
)

pytestmark = [pytest.mark.integration, pytest.mark.storage]


def test_parquet_and_jsonl_round_trip_on_seaweedfs(seaweedfs):
    """Both storage sinks write two batches to SeaweedFS's S3 API and read back whole."""
    rows = write_and_read_back(
        {
            "type": "s3",
            "endpoint_override": seaweedfs["endpoint"],
            "access_key": seaweedfs["access_key"],
            "secret_key": seaweedfs["secret_key"],
            "region": seaweedfs["region"],
            "allow_bucket_creation": True,
        },
        "desflow-storage",
    )

    check_round_trip(rows)


def _filesystem(seaweedfs):
    return {
        "type": "s3",
        "endpoint_override": seaweedfs["endpoint"],
        "access_key": seaweedfs["access_key"],
        "secret_key": seaweedfs["secret_key"],
        "region": seaweedfs["region"],
        "allow_bucket_creation": True,
    }


def _events(rows: list[dict]) -> list[dict]:
    return [
        {
            "stream_type": "event",
            "sim_ts": float(number),
            "timestamp": "2026-01-01T00:00:01.000",
            "key": f"task-{number}",
            "value": row,
        }
        for number, row in enumerate(rows)
    ]


def _read(store, folder: str, stem: str, suffix: str) -> list[dict]:
    """The rows of every file in `folder` whose name starts with `stem`."""
    rows: list[dict] = []
    for info in store.get_file_info(pafs.FileSelector(folder)):
        name = info.path.rsplit("/", 1)[-1]
        if not (name.startswith(stem) and name.endswith(suffix)):
            continue
        if suffix == ".parquet":
            rows += pq.read_table(info.path, filesystem=store).to_pylist()
        else:
            with store.open_input_stream(info.path) as stream:
                rows += [orjson.loads(line) for line in stream.read().splitlines()]
    return rows


def _write(seaweedfs, envelope: bool, jobs, filesystem=None):
    """Writes through the sinks `jobs(folder, filesystem)` returns, each with its
    rows, and returns the store and the folder."""
    filesystem = filesystem or _filesystem(seaweedfs)
    store = _build_filesystem(filesystem, pafs)
    store.create_dir("desflow-storage")
    folder = f"desflow-storage/desflow-{uuid.uuid4().hex[:8]}"

    async def write() -> None:
        for egress, rows in jobs(folder, filesystem):
            egress.envelope = envelope
            q: queue.Queue = queue.Queue()
            q.put(_events(rows))
            await drain(egress, q)

    asyncio.run(write())
    return store, folder


def test_path_field_routes(seaweedfs):
    """`__path__` in an event's value names its file, on both sinks; the key is not
    written, and an event without it goes to `default_path`."""

    def jobs(folder, filesystem):
        for sink, ext in (
            (ParquetStorageEgress, "parquet"),
            (JsonlStorageEgress, "jsonl"),
        ):
            yield (
                sink(default_path=f"{folder}/rest.{ext}", filesystem=filesystem),
                [
                    {"__path__": f"{folder}/a.{ext}", "n": 1},
                    {"__path__": f"{folder}/b.{ext}", "n": 2},
                    {"n": 3},
                ],
            )

    store, folder = _write(seaweedfs, True, jobs)
    try:
        for suffix in (".parquet", ".jsonl"):
            for stem, number in (("a", 1), ("b", 2), ("rest", 3)):
                found = _read(store, folder, stem, suffix)
                assert [row["n"] for row in found] == [number], (stem, suffix)
                assert "__path__" not in found[0]
    finally:
        store.delete_dir_contents(folder, missing_dir_ok=True)


def test_format_time_path(seaweedfs, tmp_path):
    """DX-20: a blueprint builds the `__path__` of each event with `{gen: format_time}`
    from the time of the run, so that the events of each day go to their own file on
    both sinks. The blueprint runs on simulation time (factor 0) and its events are
    written after the run, so nothing here depends on the clock or on timing."""
    from desflow import SimulationContext
    from tests.unit.test_blueprint import _Capture

    def events_of(folder, ext):
        path = tmp_path / f"{ext}.yaml"
        path.write_text(
            "simulation: {sim_id: Shop, factor: 0, timezone: UTC, "
            "logical_start_time: '2026-01-01T00:00:00+00:00'}\n"
            "arrivals: {a: {dist: constant, value: 50000, spawn: t}}\n"
            "tasks:\n  t:\n    payload:\n      n: {gen: sequence}\n"
            f"      __path__: {{gen: format_time, format: '{folder}/%Y-%m-%d.{ext}'}}\n"
        )
        app = SimulationContext.from_yaml(path)
        capture = _Capture()
        app.add_egress(capture)
        app.run(until=120000)
        return [
            record["value"]
            for record in capture.records
            if record["stream_type"] == "event" and "__path__" in record["value"]
        ]

    def jobs(folder, filesystem):
        for sink, ext in (
            (ParquetStorageEgress, "parquet"),
            (JsonlStorageEgress, "jsonl"),
        ):
            rows = events_of(folder, ext)
            assert [row["__path__"] for row in rows] == [
                f"{folder}/2026-01-01.{ext}",
                f"{folder}/2026-01-02.{ext}",
            ]
            yield sink(default_path=f"{folder}/rest.{ext}", filesystem=filesystem), rows

    store, folder = _write(seaweedfs, True, jobs)
    try:
        for suffix in (".parquet", ".jsonl"):
            for stem in ("2026-01-01", "2026-01-02"):
                found = _read(store, folder, stem, suffix)
                assert len(found) == 1, (stem, suffix)
                assert "__path__" not in found[0]
            first = _read(store, folder, "2026-01-01", suffix)[0]["n"]
            second = _read(store, folder, "2026-01-02", suffix)[0]["n"]
            assert (first, second) == (0, 1)
    finally:
        store.delete_dir_contents(folder, missing_dir_ok=True)


def test_envelope_false_writes_the_value(seaweedfs):
    """With `envelope` off a Parquet row and a JSONL line are the event's value and
    nothing else."""
    rows = [{"n": 1, "m": "x"}, {"n": 2, "m": "y"}]

    def jobs(folder, filesystem):
        yield (
            ParquetStorageEgress(
                default_path=f"{folder}/plain.parquet", filesystem=filesystem
            ),
            rows,
        )
        yield (
            JsonlStorageEgress(
                default_path=f"{folder}/plain.jsonl", filesystem=filesystem
            ),
            rows,
        )

    store, folder = _write(seaweedfs, False, jobs)
    try:
        assert _read(store, folder, "plain", ".parquet") == rows
        assert _read(store, folder, "plain", ".jsonl") == rows
    finally:
        store.delete_dir_contents(folder, missing_dir_ok=True)

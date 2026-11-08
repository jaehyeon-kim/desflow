"""A sink writes every record that can be written, and skips only the record at fault.

The events of each builder model that `scripts/regression.py capture` runs (the cases of
`test_second_run.py`) are put into one batch with a route that is None or empty on some
records, a table name with no namespace, and an upsert key that is null or missing on
some records. Each sink, with its client replaced, must write every other record, skip
the bad one with a warning, send a null route to its default, and report a count equal
to what it wrote. A file sink or an Iceberg sink with no destination is refused.
"""

import asyncio
import contextlib
import importlib.util
import logging
import os
import queue
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import scipy.optimize  # noqa: F401  # imported before the capture restores sys.modules
import scipy.stats  # noqa: F401

from desflow.connectors.egress.base import BaseEgress
from desflow.connectors.egress.iceberg import IcebergStorageEgress
from desflow.connectors.egress.postgres import PostgresEgress
from desflow.connectors.egress.redis import RedisEgress
from desflow.connectors.egress.storage import (
    JsonlStorageEgress,
    ParquetStorageEgress,
)
from desflow.core.sinks import Sink

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "second_run_cases", ROOT / "tests" / "unit" / "test_second_run.py"
)
assert _spec is not None and _spec.loader is not None
second_run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(second_run)
regression = second_run.regression

# The builder models of the capture, without the three-form SimPy examples, whose
# plain models add nothing to what the others hold
MODELS = [
    case
    for case in second_run.CASES
    if case[0].endswith(":builder") and "simpy/" not in case[0]
]
IDS = [case[0] for case in MODELS]


class _Capture(BaseEgress):
    def __init__(self) -> None:
        self.records: List[dict] = []

    async def run(self, egress_queue: queue.Queue) -> None:
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _events(make: Any, until: Any, run: Dict[str, Any]) -> List[dict]:
    """Up to six events of a model's run whose value is a mapping, each with an `id`
    and a `k` field added, so a sink has key columns to read."""
    logging.disable(logging.CRITICAL)
    try:
        with (
            regression._restored_imports(),
            contextlib.chdir(ROOT),
            patch.dict(os.environ, {"PWD": str(ROOT)}),
            patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
        ):
            app, capture = make(), _Capture()
            app.factor = 0.0
            app.go_live_at = None
            if app.random_seed is None:
                app.random_seed = 11
            app._sinks.ingress = []
            app._sinks.egress = [Sink(capture)]
            app.run(until=until, **run)
    finally:
        logging.disable(logging.NOTSET)
    found = [
        r
        for r in capture.records
        if r.get("stream_type") == "event" and isinstance(r.get("value"), dict)
    ][:6]
    if len(found) < 6:
        pytest.skip("fewer than six events")
    return found


def _routed(events: List[dict], key: str, routes: List[Any], **fields: Any) -> list:
    """A batch of copies of the events, each with its route (a route that is the
    string `missing` is left out) and the fields given as {name: [value per event]}."""
    batch = []
    for index, event in enumerate(events):
        value = {k: v for k, v in event["value"].items() if k not in ("id", "k")}
        value["id"] = index
        value["k"] = index
        for name, values in fields.items():
            if values[index] == "missing":
                value.pop(name, None)
            else:
                value[name] = values[index]
        if routes[index] != "missing":
            value[key] = routes[index]
        batch.append({**event, "value": value})
    return batch


def _drive(egress: Any, batch: list, done: Any) -> None:
    """Runs an egress provider on one batch until `done()` is true."""
    egress_queue: queue.Queue = queue.Queue()
    egress_queue.put(batch)

    async def main() -> None:
        task = asyncio.create_task(egress.run(egress_queue))
        for _ in range(500):
            if egress_queue.empty() and done():
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(main())


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_redis_sends_a_null_route_to_the_default_stream(name, make, until, run):
    events = _events(make, until, run)
    batch = _routed(events, "__stream__", [None, "s", "", "s", None, "s"])
    targets: List[str] = []
    client = MagicMock()
    pipe = MagicMock()
    pipe.xadd.side_effect = lambda target, fields: targets.append(target)
    executed: List[int] = []

    async def execute() -> None:
        executed.append(len(targets))

    pipe.execute = execute
    client.pipeline.return_value = pipe
    with patch("desflow.connectors.egress.redis.redis.from_url", return_value=client):
        _drive(RedisEgress("redis://h", stream_name="d"), batch, lambda: executed)
    assert sorted(targets) == ["d", "d", "d", "s", "s", "s"], f"{name}: {targets}"


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_postgres_sends_a_null_route_to_its_own_table(name, make, until, run, caplog):
    events = _events(make, until, run)
    routes = [None, "orders", "", "other", "orders", None]
    batch = _routed(events, "__table__", routes)
    rows = PostgresEgress("postgresql://u:p@h/db", table_name="orders")._rows(batch)
    assert sorted(row["id"] for row in rows) == [0, 1, 2, 4, 5], name


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_postgres_skips_a_record_with_a_null_or_missing_upsert_key(
    name, make, until, run, caplog
):
    events = _events(make, until, run)
    batch = _routed(
        events,
        "__table__",
        [None] * 6,
        k=[0, None, 2, "missing", 4, 5],
    )
    egress = PostgresEgress(
        "postgresql://u:p@h/db", table_name="orders", upsert_keys=["id", "k"]
    )
    with caplog.at_level(logging.WARNING):
        rows = egress._rows(batch)
    assert sorted(row["id"] for row in rows) == [0, 2, 4, 5], name
    assert "null or missing" in caplog.text


def _iceberg(tmp_path: Path, **options: Any) -> Any:
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog(
        "c", uri=f"sqlite:///{tmp_path}/c.db", warehouse=f"file://{tmp_path}/wh"
    )
    return catalog, IcebergStorageEgress(catalog=catalog, **options)


def _table_rows(catalog: Any) -> Dict[str, int]:
    return {
        f"sim.{table[1]}": catalog.load_table(table).scan().to_arrow().num_rows
        for table in catalog.list_tables("sim")
    }


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_iceberg_skips_only_the_record_with_no_namespace(
    name, make, until, run, tmp_path, caplog
):
    events = _events(make, until, run)
    routes = ["plain", "sim.a", "sim.b", "sim.a", "sim.b", "sim.a"]
    catalog, egress = _iceberg(tmp_path, default_table="sim.d")
    with caplog.at_level(logging.WARNING):
        egress._write_batch(_routed(events, "__table__", routes), pa)
    assert _table_rows(catalog) == {"sim.a": 3, "sim.b": 2}, name
    assert "no namespace" in caplog.text


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_iceberg_skips_only_the_record_with_a_null_or_missing_upsert_key(
    name, make, until, run, tmp_path, caplog
):
    events = _events(make, until, run)
    routes = ["sim.b", "sim.a", "sim.b", "sim.a", "sim.b", "sim.a"]
    batch = _routed(events, "__table__", routes, k=[0, 1, 2, None, 4, "missing"])
    catalog, egress = _iceberg(tmp_path, upsert_keys={"sim.a": ["id", "k"]})
    with caplog.at_level(logging.WARNING):
        egress._write_batch(batch, pa)
    assert _table_rows(catalog) == {"sim.a": 1, "sim.b": 3}, name
    assert "null or missing" in caplog.text


@pytest.mark.parametrize("kind", ["jsonl", "parquet"])
@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_a_file_sink_sends_a_null_route_to_its_default_and_counts_its_files(
    kind, name, make, until, run, tmp_path
):
    events = _events(make, until, run)
    other_b, other_c = (str(tmp_path / f"{name}.{kind}") for name in ("b", "c"))
    routes = [None, "", other_b, other_b, other_c, None]
    batch = _routed(events, "__path__", routes)
    sink_class = JsonlStorageEgress if kind == "jsonl" else ParquetStorageEgress
    sink = sink_class(default_path=str(tmp_path / f"a.{kind}"))
    sink.filesystem = _local_filesystem()
    written = sink._write_batch(batch, *((pa, pq) if kind == "parquet" else ()))
    files = [p for p in tmp_path.iterdir() if p.is_file() or p.is_dir()]
    paths = [p for p in tmp_path.rglob("*") if p.is_file()]
    assert written == len(paths) == 3, f"{name}: counted {written}, wrote {len(paths)}"
    lines = 0
    for path in paths:
        if kind == "jsonl":
            lines += len(path.read_bytes().splitlines())
        else:
            lines += pq.read_metadata(path).num_rows
    assert lines == 6, f"{name}: {lines} rows written, files {files}"


def _local_filesystem() -> Any:
    from pyarrow import fs

    return fs.LocalFileSystem()


@pytest.mark.parametrize("kind", ["jsonl", "parquet"])
def test_a_file_sink_with_no_default_writes_routed_records_and_warns_for_the_rest(
    kind, tmp_path, caplog
):
    # With no default_path and no path_router, a record that names its file in
    # __path__ is written there, and one that does not is skipped with a warning
    events = _events(*MODELS[0][1:])
    routes = [str(tmp_path / f"a.{kind}"), None, str(tmp_path / f"b.{kind}"), None]
    batch = _routed(events[:4], "__path__", routes)
    sink = (JsonlStorageEgress if kind == "jsonl" else ParquetStorageEgress)()
    sink.filesystem = _local_filesystem()
    with caplog.at_level(logging.WARNING):
        written = sink._write_batch(batch, *((pa, pq) if kind == "parquet" else ()))
    paths = [p for p in tmp_path.rglob("*") if p.is_file()]
    assert written == len(paths) == 2
    assert "2 had no __path__" in caplog.text


def test_a_blueprint_iceberg_egress_with_only_its_type_is_refused_at_load(tmp_path):
    kind = "Iceberg"
    from desflow.core.context import SimulationContext

    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0}\n"
        f"egress: [{{type: {kind}}}]\n"
        "run: {until: 1}\n"
    )
    with pytest.raises(Exception, match=r"egress|m\.yaml:2"):
        SimulationContext.from_yaml(path)

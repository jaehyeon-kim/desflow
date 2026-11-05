"""Every YAML example builds, runs at factor 0 and produces the records it describes.

Each file in examples/yaml/ is built as `desflow run` builds it, with the
environment variables it reads left unset so the defaults apply. The connectors are
checked as constructed, then replaced by capturing sinks for a short run at factor 0.
The captured records are passed to the real connector code where that needs no
server: Parquet writes to a temporary folder, Iceberg appends to a fake catalog, and
Postgres and Redis write through a mocked client.

Nothing here contacts a broker, database or catalog.
"""

import asyncio
import importlib.util
import itertools
import queue
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import fs

from desflow import DynamicRealtimeEnvironment, SimulationContext
from desflow.connectors.egress.base import BaseEgress
from desflow.connectors.egress.storage import _build_filesystem
from tests.conformance.harness import run_low_level

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples" / "yaml"
START = datetime(2026, 1, 1)

# Every variable an example reads, cleared so each test sees the file's defaults.
VARIABLES = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "DEST_PATH",
    "PARQUET_FILESYSTEM",
    "S3_ENDPOINT",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
    "S3_REGION",
    "ICEBERG_URI",
    "ICEBERG_WAREHOUSE",
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)


class _Capture(BaseEgress):
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _build(name):
    # A logic module left over from another test would be reused rather than
    # imported again, so each build starts clean.
    for module in [m for m in sys.modules if m.endswith("_logic")]:
        del sys.modules[module]
    return SimulationContext.from_yaml(EXAMPLES / name)


def _run(context, until, start=START):
    """Runs at factor 0 into one capture per sink, and returns what each received.

    The `when` predicates are kept, so each capture receives what its sink would.
    """
    context.factor = 0.0
    context.logical_start_time = start
    # The run stays unpaced, so a live tail costs no real time.
    context.go_live_at = None
    context._before_run = []
    context._ingress_providers = []
    context._egress_providers = [_Capture() for _ in context._egress_providers]
    context.run(until=until)
    return [capture.records for capture in context._egress_providers]


def _events(records):
    return [r for r in records if r["stream_type"] == "event"]


def _finished(records):
    """The values of the finished events, which are the task payloads."""
    return [
        r["value"]
        for r in _events(records)
        if r["value"].get("status") not in ("queued", "started")
    ]


def _telemetry_paths(records):
    return {r["path_id"] for r in records if r["stream_type"] == "telemetry"}


async def _drain(egress, records):
    """Runs an egress over one batch until it has handled it."""
    egress_queue: queue.Queue = queue.Queue()
    egress_queue.put(records)
    task = asyncio.create_task(egress.run(egress_queue))
    for _ in range(100):
        await asyncio.sleep(0.01)
        if egress_queue.empty():
            break
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def test_every_example_is_plain_yaml():
    """Only the advanced examples reference Python."""
    from desflow.examples import EXAMPLES as LISTED

    plain = sorted([*EXAMPLES.glob("*.yaml"), *EXAMPLES.glob("simpy/*.yaml")])
    # Every example's blueprint outside advanced/ is here, SimPy's examples included,
    # and nothing else
    assert {p.relative_to(EXAMPLES).as_posix() for p in plain} == {
        f.removeprefix("yaml/")
        for example in LISTED.values()
        for f in example.files["yaml"]
        if f.endswith(".yaml") and not f.startswith("yaml/advanced/")
    }
    for path in plain:
        assert "!python" not in path.read_text(encoding="utf-8"), path.name
    assert not list(EXAMPLES.glob("*.py"))


def test_local():
    app = _build("local.yaml")
    assert app._default_until == 30.0
    [records] = _run(app, 30)

    finished = _finished(records)
    assert finished
    # Several packers work at once from 10 seconds, so a later order can finish first
    assert {"event_type": "order_packed", "shipping": "standard", "order_id": 0} in (
        finished
    )
    assert {
        "Shop.stats.resources.packers.utilization",
        "Shop.stats.resources.packers.queue_length",
    } <= _telemetry_paths(records)
    # The scenario adds packers at 10 seconds and removes one at 20 seconds.
    capacity = {
        r["sim_ts"]: r["value"]
        for r in records
        if r.get("path_id") == "Shop.stats.resources.packers.capacity"
    }
    assert (capacity[8.0], capacity[12.0], capacity[22.0]) == (1, 3, 2)


def test_kafka():
    app = _build("kafka.yaml")
    [ingress] = app._ingress_providers
    [egress] = app._egress_providers
    assert app._default_until is None
    assert ingress.topic == "sim-config"
    assert ingress.bootstrap_servers == "127.0.0.1:9092"
    assert (egress.event_topic, egress.telemetry_topic) == (
        "sim-events",
        "sim-telemetry",
    )
    assert egress.producer_config["bootstrap_servers"] == "127.0.0.1:9092"
    # No router, so the egress creates both topics when it starts.
    assert egress.topic_router is None

    [records] = _run(app, 120)
    finished = _finished(records)
    assert finished
    assert all(
        value == {"path_id": "Shop.service.packing", "status": "finished"}
        for value in finished
    )
    # The names kafka_dashboard.py reads
    assert {
        "Shop.stats.resources.packers.capacity",
        "Shop.stats.resources.packers.in_use",
        "Shop.stats.resources.packers.queue_length",
        "Shop.stats.resources.packers.utilization",
    } <= _telemetry_paths(records)
    capacity = [
        r["value"]
        for r in records
        if r.get("path_id") == "Shop.stats.resources.packers.capacity"
    ]
    assert set(capacity) == {1}


def test_kafka_reads_the_broker_from_the_environment(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    app = _build("kafka.yaml")
    assert app._egress_providers[0].producer_config["bootstrap_servers"] == (
        "broker:29092"
    )


def test_parquet(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    app = _build("parquet.yaml")
    [egress] = app._egress_providers
    assert app._default_until == 604800.0
    assert egress.default_path == "data/events.parquet"
    assert egress.path_router is None
    assert isinstance(_build_filesystem(egress.filesystem, fs), fs.LocalFileSystem)
    assert (app._batch_size, app._flush_interval) == (5000, 86400)

    [records] = _run(app, 3600)
    egress.filesystem = _build_filesystem(egress.filesystem, fs)
    egress._write_batch(records, pa, pq)

    # The folder did not exist, and the first write created it.
    [chunk] = (tmp_path / "data").glob("events_*.parquet")
    rows = pq.read_table(chunk).to_pylist()
    assert len(rows) == len(_events(records))
    assert set(rows[0]) == {
        "stream_type",
        "sim_ts",
        "timestamp",
        "key",
        "task",
        "instance",
        "path_id",
        "status",
    }
    assert {row["status"] for row in rows} == {"queued", "started", "finished"}


def test_parquet_on_s3(monkeypatch):
    """The variables in the file's header switch the same file to S3."""
    monkeypatch.setenv("PARQUET_FILESYSTEM", "s3")
    monkeypatch.setenv("DEST_PATH", "odctl-dev/history")
    monkeypatch.setenv("S3_ENDPOINT", "http://127.0.0.1:8333")
    monkeypatch.setenv("S3_ACCESS_KEY", "user")
    monkeypatch.setenv("S3_SECRET_KEY", "password")
    [egress] = _build("parquet.yaml")._egress_providers

    assert egress.default_path == "odctl-dev/history/events.parquet"
    built = _build_filesystem(egress.filesystem, fs)
    assert isinstance(built, fs.S3FileSystem)
    options = built.__reduce__()[1][0]
    assert (options["endpoint_override"], options["scheme"]) == (
        "127.0.0.1:8333",
        "http",
    )
    assert (options["access_key"], options["secret_key"]) == ("user", "password")


class _Table:
    def __init__(self, schema):
        self._schema = schema
        self.appended = []

    def schema(self):
        return MagicMock(as_arrow=lambda: self._schema)

    def append(self, table):
        self.appended.append(table)


class _Catalog:
    """Stands in for the REST catalog: creates tables in memory."""

    def __init__(self):
        self.tables = {}

    def create_namespace_if_not_exists(self, namespace):
        pass

    def create_table_if_not_exists(self, identifier, schema, location=None):
        return self.tables.setdefault(identifier, _Table(schema))


def test_iceberg():
    app = _build("iceberg.yaml")
    [egress] = app._egress_providers
    assert app._default_until == 86400.0
    assert app._egress_batch_sizes == [200000]
    # The catalog is a mapping until the first write, so loading contacts nothing.
    assert egress.catalog is None
    assert egress.catalog_properties == {
        "name": "odctl",
        "type": "rest",
        "uri": "http://127.0.0.1:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://127.0.0.1:8333",
        "s3.access-key-id": "user",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
    }
    assert egress.default_table == "simulation.events"
    assert egress.table_router is None
    assert egress.schemas["simulation.events"] == pa.schema(
        [
            ("sim_ts", pa.float64()),
            ("timestamp", pa.timestamp("us")),
            ("key", pa.string()),
            ("path_id", pa.string()),
            ("status", pa.string()),
        ]
    )

    [records] = _run(app, 3600)
    egress.catalog = _Catalog()
    egress._write_batch(records, pa)

    [table] = egress.catalog.tables["simulation.events"].appended
    assert table.num_rows == len(_events(records))
    rows = table.to_pylist()
    assert all(isinstance(row["timestamp"], datetime) for row in rows)
    assert rows[0]["timestamp"] >= START
    assert {row["status"] for row in rows} == {"queued", "started", "finished"}


# How information_schema names the SQL types the examples use.
DATA_TYPES = {"TIMESTAMP": "timestamp without time zone", "INT": "integer"}


@pytest.fixture
def postgres():
    """A mocked asyncpg pool whose tables have the columns the examples create.

    Set `egresses` to the egresses under test, so a column lookup finds the table.
    """
    conn = AsyncMock()
    acquire = AsyncMock()
    acquire.__aenter__.return_value = conn
    pool = MagicMock()
    pool.acquire.return_value = acquire
    state = SimpleNamespace(created={}, conn=conn, egresses=[])

    async def execute(sql, *args):
        if sql.startswith("CREATE TABLE IF NOT EXISTS "):
            state.created[sql.split()[5]] = sql

    async def fetch(sql, table_name):
        egress = next(e for e in state.egresses if e.table_name == table_name)
        columns, _ = egress.tables[table_name]
        return [
            {"column_name": column, "data_type": DATA_TYPES.get(sql_type, sql_type)}
            for column, sql_type in columns.items()
        ]

    conn.execute.side_effect = execute
    conn.fetch.side_effect = fetch
    with patch(
        "desflow.connectors.egress.postgres.asyncpg.create_pool",
        new=AsyncMock(return_value=pool),
    ):
        yield state


def _written(conn, table):
    """The rows the egress inserted into `table`, as dictionaries."""
    rows = []
    for call in conn.executemany.call_args_list:
        query, values = call.args
        if query.startswith(f"INSERT INTO {table} ("):
            columns = query.split("(", 1)[1].split(")", 1)[0].split(", ")
            rows.extend(dict(zip(columns, row)) for row in values)
    return rows


@pytest.mark.asyncio
async def test_postgres(postgres):
    app = _build("postgres.yaml")
    [ingress] = app._ingress_providers
    [egress] = app._egress_providers
    assert ingress.table_name == "simulation_params"
    assert egress.table_name == "orders"
    postgres.egresses = [egress]

    [records] = await asyncio.to_thread(_run, app, 120)
    await _drain(egress, records)

    assert postgres.created["orders"] == (
        "CREATE TABLE IF NOT EXISTS orders (order_id INT, customer_id INT, "
        "total_amount REAL, status TEXT, timestamp TIMESTAMP, PRIMARY KEY (order_id))"
    )
    rows = _written(postgres.conn, "orders")
    assert len(rows) == len(_finished(records)) > 0
    assert [row["order_id"] for row in rows] == list(range(len(rows)))
    assert {(r["customer_id"], r["total_amount"], r["status"]) for r in rows} == {
        (42, 59.97, "pending")
    }
    # The ISO time of the record is converted for the TIMESTAMP column.
    assert all(isinstance(row["timestamp"], datetime) for row in rows)


@pytest.mark.asyncio
async def test_redis():
    app = _build("redis.yaml")
    [ingress] = app._ingress_providers
    [egress] = app._egress_providers
    assert ingress.channel_name == "simulation_params"
    assert egress.stream_name == "events"

    [records] = await asyncio.to_thread(_run, app, 120)
    finished = _finished(records)
    assert finished
    # The task has no service or resource, so it emits no queued or started event.
    assert len(finished) == len(_events(records))
    assert finished[:2] == [
        {
            "__stream__": "order_events",
            "channel": "web",
            "status": "placed",
            "order_id": 0,
        },
        {
            "__stream__": "order_events",
            "channel": "web",
            "status": "placed",
            "order_id": 1,
        },
    ]

    client = MagicMock()
    pipe = client.pipeline.return_value
    pipe.execute = AsyncMock()
    with patch("desflow.connectors.egress.redis.redis.from_url", return_value=client):
        await _drain(egress, records)

    streams = [call.args[0] for call in pipe.xadd.call_args_list]
    assert streams.count("order_events") == len(finished)
    # The test runs unpaced, so no lag telemetry goes to the default stream, events
    assert set(streams) == {"order_events"}


def test_backfill_live():
    app = _build("backfill_live.yaml")
    history, live = app._egress_providers
    assert app._default_until == 660.0
    assert (app.go_live_at - app.logical_start_time).total_seconds() == 600
    assert history.default_path == "data/backfill/events.parquet"
    assert live.producer_config["bootstrap_servers"] == "127.0.0.1:9092"
    assert (app._batch_size, app._flush_interval) == (2000, 10.0)

    go_live_iso = app.go_live_at.isoformat(timespec="milliseconds")
    history_records, live_records = _run(app, 660, start=app.logical_start_time)

    assert history_records and live_records
    assert all(r["timestamp"] < go_live_iso for r in history_records)
    assert all(r["timestamp"] >= go_live_iso for r in live_records)
    assert max(r["sim_ts"] for r in history_records) < 600
    assert min(r["sim_ts"] for r in live_records) >= 600


@pytest.mark.asyncio
async def test_advanced_postgres_orders(postgres):
    app = _build("advanced/postgres_orders.yaml")
    orders, items = app._egress_providers
    assert (orders.table_name, items.table_name) == ("orders", "order_items")
    assert app.random_seed == 42
    postgres.egresses = [orders, items]

    [first, second] = await asyncio.to_thread(_run, app, 120)
    assert first == second, "both sinks receive every record"
    await _drain(orders, first)
    await _drain(items, first)

    assert set(postgres.created) == {"orders", "order_items"}
    order_rows = _written(postgres.conn, "orders")
    item_rows = _written(postgres.conn, "order_items")
    assert order_rows and item_rows
    assert [row["order_id"] for row in order_rows] == list(
        range(1, len(order_rows) + 1)
    )

    for order in order_rows:
        lines = [item for item in item_rows if item["order_id"] == order["order_id"]]
        assert 1 <= len(lines) <= 5
        for item in lines:
            assert 10.0 <= item["unit_price"] <= 50.0
            assert 1 <= item["quantity"] <= 3
        expected = sum(item["unit_price"] * item["quantity"] for item in lines)
        assert order["total_amount"] == round(expected, 2)
        assert 1 <= order["customer_id"] <= 100
        assert order["status"] == "pending"


def test_advanced_postgres_orders_repeats_with_the_seed():
    def run():
        [records, _] = _run(_build("advanced/postgres_orders.yaml"), 60)
        return [r["value"] for r in _events(records)]

    assert run() == run()


# ---------------------------------------------------------------------------
# The gallery, `examples/gallery/`: each example is `<slug>.yaml`, `<slug>_builder.py`
# and `<slug>_low_level.py`, and publishes the same records in all three.
# ---------------------------------------------------------------------------

GALLERY = ROOT / "examples" / "gallery"
_TESTED: set = set()


def _gallery(slug):
    """Marks the test that asserts what the gallery example `slug` shows."""

    def mark(test):
        _TESTED.add(slug)
        return test

    return mark


def _gallery_module(slug, form):
    """Loads `<slug>_<form>.py`, a file that imports nothing from the folder."""
    path = GALLERY / f"{slug}_{form}.py"
    spec = importlib.util.spec_from_file_location(f"gallery_{slug}_{form}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _gallery_yaml(slug):
    return SimulationContext.from_yaml(GALLERY / f"{slug}.yaml")


# The lists of a context that hold one entry for each egress sink
_SINK_LISTS = (
    "_egress_providers",
    "_egress_predicates",
    "_egress_labels",
    "_egress_traces",
    "_egress_batch_sizes",
    "_egress_flush_intervals",
    "_egress_envelopes",
)


def _gallery_records(slug, form):
    """The records of one form of a gallery example, run unpaced into a capture.

    The YAML file runs to the end its `run` section sets. The Builder file runs its
    `build()` to its `UNTIL`, and the Low-level file its `build(env)` to its `UNTIL`.
    """
    if form == "low_level":
        module = _gallery_module(slug, form)
        records, _ = run_low_level(
            getattr(module, "PARAMS", None),
            module.build,
            module.UNTIL,
            stats=getattr(module, "STATS", None),
            environment=getattr(module, "ENVIRONMENT", {"logical_start_time": START}),
        )
        return records
    if form == "yaml":
        context = _gallery_yaml(slug)
        end = None
    else:
        module = _gallery_module(slug, form)
        context = module.build()
        end = module.UNTIL
    context.factor = 0.0
    context._before_run = []
    context._ingress_providers = []
    for name in _SINK_LISTS:  # every sink, with its `when`, gives way to one capture
        setattr(context, name, [])
    capture = _Capture()
    context.add_egress(capture)
    context.run(until=end)
    return capture.records


def _sinks(providers, predicates, tasks):
    """The sinks a form sets up: each one's class, and the tasks whose records its `when`
    lets through (all of them when it has none)."""
    predicates = predicates or [None] * len(providers)
    return [
        (
            type(provider).__name__,
            sorted(t for t in tasks if predicate is None or predicate({"task": t})),
        )
        for provider, predicate in zip(providers, predicates, strict=True)
    ]


def _low_level_run(slug):
    """What the Low-level file's own `run()` sets up and runs: the sinks it gives
    `setup_egress`, with its other options, and the time it runs to. The environment's
    `setup_egress`, `setup_stats` and `run` are replaced by recorders, so no sink
    connects and nothing runs. `stats` is the level and interval it gives
    `setup_stats`, or None when it does not call it."""
    seen = {"stats": None}

    def setup_stats(self, sim_id, *level_and_interval):
        seen["stats"] = level_and_interval

    def setup_egress(self, providers, predicates=None, **options):
        seen["sinks"] = (providers, predicates)
        seen["options"] = options

    def run(self, until=None):
        seen["until"] = until
        seen["factor"] = self.factor
        seen["start"] = self.start_datetime

    module = _gallery_module(slug, "low_level")
    with (
        patch.object(DynamicRealtimeEnvironment, "setup_egress", setup_egress),
        patch.object(DynamicRealtimeEnvironment, "setup_stats", setup_stats),
        patch.object(DynamicRealtimeEnvironment, "run", run),
    ):
        module.run()
    return seen


def _stream(records):
    """Every record without its wall-clock fields, and without the lag readings."""
    return [
        (
            r["stream_type"],
            r["sim_ts"],
            r.get("key", r.get("path_id")),
            r.get("task"),
            r.get("instance"),
            r["value"],
        )
        for r in records
        if not str(r.get("path_id", "")).startswith("system.")
    ]


def _gallery_slugs():
    return sorted(p.stem for p in GALLERY.glob("*.yaml"))


def _known_case_examples():
    """The gallery examples that a known case runs: the `EXAMPLE` of its `original.py`,
    whose checks are that case's test of the example."""
    found = set()
    for original in (ROOT / "tests" / "known_cases").glob("*/original.py"):
        found.update(
            re.findall(r'^EXAMPLE = "(\w+)"', original.read_text(), re.MULTILINE)
        )
    return found


@pytest.mark.parametrize("slug", _gallery_slugs())
def test_gallery_forms_publish_the_same_records(slug):
    """The Builder form publishes the YAML form's whole record stream, and the
    Low-level form the same events and readings. `run()` of the Low-level file sets up
    the YAML form's sinks and runs to the end of its `run` section."""
    records = _gallery_records(slug, "yaml")
    yaml_records = _stream(records)
    assert yaml_records
    assert _stream(_gallery_records(slug, "builder")) == yaml_records
    assert _stream(_gallery_records(slug, "low_level")) == yaml_records
    tasks = {r["task"] for r in records if r.get("task")}
    context = _gallery_yaml(slug)
    sinks = _sinks(context._egress_providers, context._egress_predicates, tasks)
    built = _gallery_module(slug, "builder").build()
    assert _sinks(built._egress_providers, built._egress_predicates, tasks) == sinks
    seen = _low_level_run(slug)
    assert _sinks(*seen["sinks"], tasks) == sinks
    assert seen["until"] == context._default_until
    stats = getattr(_gallery_module(slug, "low_level"), "STATS", None)
    if stats is None:
        assert seen["stats"] is None
    else:
        assert seen["stats"] == ((stats,) if isinstance(stats, str) else tuple(stats))


def _utc(moment):
    """A start time as an instant: one with no offset is read as UTC."""
    return moment.replace(tzinfo=timezone.utc) if moment.tzinfo is None else moment


def test_every_gallery_file_has_its_forms_and_a_test():
    """A new example cannot land with a form missing or untested, and nothing is in the
    folder but the three forms of each example. Each form is unpaced, starts at the
    fixed time and ends where the others end."""
    slugs = _gallery_slugs()
    assert slugs
    for slug in slugs:
        text = (GALLERY / f"{slug}.yaml").read_text(encoding="utf-8")
        assert "!python" not in text
        assert f"# Run it with: desflow run examples/gallery/{slug}.yaml" in text
        assert "# Needs: " in text
        assert "random_seed: 42" in text
        yaml_context = _gallery_yaml(slug)
        built = _gallery_module(slug, "builder").build()
        seen = _low_level_run(slug)
        assert yaml_context.factor == 0, slug
        assert built.factor == 0, slug
        assert seen["factor"] == 0, slug
        for start in (
            yaml_context.logical_start_time,
            built.logical_start_time,
            seen["start"],
        ):
            assert _utc(start) == _utc(START), slug
        for form in ("builder", "low_level"):
            source = (GALLERY / f"{slug}_{form}.py").read_text(encoding="utf-8")
            assert f"Run it with: python examples/gallery/{slug}_{form}.py" in source
            assert "Needs: nothing." not in source, slug
            if "type: Console" in text:  # the `log` format prints at INFO only
                assert "logging.basicConfig(level=logging.INFO" in source, slug
            if form == "builder":  # a blueprint keeps 1_000_000 rows, as in the YAML
                assert set(re.findall(r"history=(\S+?)[,)]", source)) <= {"1_000_000"}
        assert not re.search(r"^#? ?Needs: nothing\.", text, re.MULTILINE), slug
        end = yaml_context._default_until
        assert _gallery_module(slug, "builder").UNTIL == end, slug
        assert _gallery_module(slug, "low_level").UNTIL == end, slug
    assert set(slugs) <= _TESTED | _known_case_examples(), sorted(
        set(slugs) - _TESTED - _known_case_examples()
    )
    assert {p.name for p in GALLERY.iterdir() if p.name != "__pycache__"} == {
        f"{slug}{suffix}"
        for slug in slugs
        for suffix in (".yaml", "_builder.py", "_low_level.py")
    }


def _rows(records, task):
    """The finished payloads of one task, in order, without its queued and started."""
    return [
        r["value"]
        for r in _events(records)
        if r["task"] == task and "status" not in r["value"]
    ]


def _events_of(records, task):
    return [
        r for r in _events(records) if r["task"] == task and "status" not in r["value"]
    ]


@_gallery("retail_records_to_three_sinks")
def test_gallery_retail_records_to_three_sinks():
    """Each sink gets the records of its own tasks only, and the records agree: an
    order names a customer and a product that exist, and its invoice repeats it."""
    context = _gallery_yaml("retail_records_to_three_sinks")
    assert [type(p).__name__ for p in context._egress_providers] == [
        "PostgresEgress",
        "PostgresEgress",
        "KafkaEgress",
        "ParquetStorageEgress",
    ]
    assert [p.table_name for p in context._egress_providers[:2]] == [
        "customers",
        "products",
    ]
    assert context._egress_providers[2].event_topic == "retail-orders"
    customers, products, orders, invoices = _run(context, 600)
    for sink, task in zip(
        (customers, products, orders, invoices),
        (
            "customer",
            "product",
            "order",
            "invoice",
        ),
    ):
        assert sink and {r["task"] for r in _events(sink)} == {task}
    customer_ids = [row["customer_id"] for row in _finished(customers)]
    assert customer_ids == list(range(1, 21))
    price = {row["product_id"]: row["price"] for row in _finished(products)}
    assert sorted(price) == list(range(1, 11))
    placed = _finished(orders)
    assert len(placed) > 200
    for order in placed:
        assert order["customer_id"] in customer_ids
        assert order["total"] == round(
            price[order["product_id"]] * order["quantity"], 2
        )
    billed = _finished(invoices)
    assert [(row["order_id"], row["amount"]) for row in billed] == [
        (row["order_id"], row["total"]) for row in placed
    ]


@_gallery("film_rental_store")
def test_gallery_film_rental_store():
    """A rental names a film and a customer that exist, is paid for at the film's rate
    and is returned 1 to 7 days after it."""
    records = _gallery_records("film_rental_store", "yaml")
    films = {row["film_id"]: row for row in _rows(records, "film")}
    customers = {row["customer_id"] for row in _rows(records, "customer")}
    assert len(films) == 20 and len(customers) == 15
    assert {film["rating"] for film in films.values()} <= {
        "G",
        "PG",
        "PG-13",
        "R",
        "NC-17",
    }
    rentals = {row["rental_id"]: row for row in _rows(records, "rental")}
    assert len(rentals) > 800
    for rental in rentals.values():
        assert rental["film_id"] in films and rental["customer_id"] in customers
        assert rental["rental_rate"] == films[rental["film_id"]]["rental_rate"]
    payments = _rows(records, "payment")
    assert [p["rental_id"] for p in payments] == list(rentals)
    for payment in payments:
        assert payment["amount"] == rentals[payment["rental_id"]]["rental_rate"]
    returns = _rows(records, "return")
    assert returns
    for returned in returns:
        rented = datetime.fromisoformat(rentals[returned["rental_id"]]["rental_date"])
        kept = (
            datetime.fromisoformat(returned["return_date"]) - rented
        ).total_seconds()
        assert 86400 <= kept <= 7 * 86400


@_gallery("auction_streams")
def test_gallery_auction_streams():
    """An auction names a seller who exists, a bid names an auction and a bidder who
    exist, and the bids are the great majority of the records."""
    records = _gallery_records("auction_streams", "yaml")
    people = _events_of(records, "person")
    auctions = _events_of(records, "auction")
    bids = _events_of(records, "bid")
    assert len(people) >= 10 and len(auctions) >= 5
    assert 400 < len(bids) < 700, len(bids)
    assert len(bids) > 5 * len(auctions) > 5 * len(people)
    person_ids = {r["value"]["person_id"] for r in people}
    assert all(r["value"]["seller"] in person_ids for r in auctions)
    opened = {}
    for r in auctions:
        opened[r["value"]["auction_id"]] = r["sim_ts"]
    for r in bids:
        assert opened[r["value"]["auction"]] <= r["sim_ts"]
        assert r["value"]["bidder"] in person_ids


@_gallery("group_by_table")
def test_gallery_group_by_table():
    """1,000 rows, with the keys and measures in the ranges the file gives."""
    rows = _rows(_gallery_records("group_by_table", "yaml"), "row")
    assert len(rows) == 1000
    for column, width in (("id1", 3), ("id2", 3), ("id3", 10)):
        assert all(
            row[column].startswith("id") and len(row[column]) == 2 + width
            for row in rows
        )
    assert {row["id1"] for row in rows} <= {f"id{n:03d}" for n in range(1, 101)}
    assert len({row["id1"] for row in rows}) > 90
    assert all(1 <= row["v1"] <= 5 and 1 <= row["v2"] <= 15 for row in rows)
    assert all(1 <= row["id4"] <= 100 and 1 <= row["id6"] <= 100000 for row in rows)
    assert all(0 <= row["v3"] <= 100 for row in rows)


@_gallery("taxi_rides")
def test_gallery_taxi_rides():
    """The fare follows from the distance and the duration, the drop-off follows the
    pickup by the duration, and every field is in its range."""
    rides = _rows(_gallery_records("taxi_rides", "yaml"), "ride")
    assert len(rides) > 30
    for ride in rides:
        minutes = (
            datetime.fromisoformat(ride["dropoff_time"])
            - datetime.fromisoformat(ride["pickup_time"])
        ).total_seconds() / 60
        assert 3 <= round(minutes, 1) <= 40
        assert 0.5 <= ride["distance_km"] <= 20
        assert (
            abs(ride["fare"] - (3 + 2.2 * ride["distance_km"] + 0.4 * minutes)) < 0.05
        )
        assert 40.7 <= ride["pickup_latitude"] <= 40.8
        assert -74.02 <= ride["pickup_longitude"] <= -73.93
        assert ride["passengers"] in (1, 2, 3, 4, 5)
        assert ride["payment"] in ("card", "cash", "dispute")
    card = sum(ride["payment"] == "card" for ride in rides) / len(rides)
    assert 0.4 <= card <= 0.9


@_gallery("labelled_text")
def test_gallery_labelled_text():
    """Every text is one of the templates of its label, and the labels are drawn with
    the weights the file gives."""
    records = _gallery_records("labelled_text", "yaml")
    templates = {
        "order_status": [
            r"Where is my order \d{4}\?",
            r"Has my parcel been sent yet\?",
        ],
        "refund": [
            r"I want my money back for this order\.",
            r"Please refund order \d{4}\.",
        ],
        "cancel": [
            r"Cancel my order, I changed my mind\.",
            r"Please stop my subscription\.",
        ],
        "hello": [r"Hi, I have a question\.", r"Hello, is anyone there\?"],
        "positive": [
            r"The \w+ is great and arrived early\.",
            r"I love this \w+, I would buy it again\.",
        ],
        "neutral": [r"The \w+ is fine, nothing special\."],
        "negative": [
            r"The \w+ broke after a week\.",
            r"I would not recommend this \w+\.",
        ],
    }
    messages = _rows(records, "message")
    reviews = _rows(records, "review")
    assert len(messages) > 200 and len(reviews) > 80
    for row, label in [(m, m["intent"]) for m in messages] + [
        (r, r["sentiment"]) for r in reviews
    ]:
        assert any(
            re.fullmatch(pattern, row["text"]) for pattern in templates[label]
        ), row
    share = sum(m["intent"] == "order_status" for m in messages) / len(messages)
    assert abs(share - 0.4) < 0.08
    positive = sum(r["sentiment"] == "positive" for r in reviews) / len(reviews)
    assert abs(positive - 0.6) < 0.15


@_gallery("point_in_time_training_set")
def test_gallery_point_in_time_training_set():
    """The join the file describes gives each label the feature record it follows: the
    newest of its customer at or before the start of the label's window, and no later
    one. Few sessions make a customer leave more often."""
    records = _gallery_records("point_in_time_training_set", "yaml")
    features = _events_of(records, "features")
    labels = _events_of(records, "label")
    assert len(features) == 5 * 39 and labels
    window = 7 * 86400
    joined = []
    for label in labels:
        customer = label["value"]["customer_id"]
        earlier = [
            f
            for f in features
            if f["value"]["customer_id"] == customer
            and f["sim_ts"] <= label["sim_ts"] - window
        ]
        assert earlier, "a label with no feature record before its window"
        newest = max(earlier, key=lambda f: f["sim_ts"])
        assert newest["value"]["as_of"] == label["value"]["as_of"]
        assert all(
            f["sim_ts"] <= label["sim_ts"] - window
            for f in features
            if f["value"]["as_of"] == label["value"]["as_of"]
            and f["value"]["customer_id"] == customer
        )
        joined.append((newest["value"]["sessions_30d"], label["value"]["churned"]))
    few = [churned for sessions, churned in joined if sessions < 5]
    many = [churned for sessions, churned in joined if sessions >= 5]
    assert few and many
    assert sum(few) / len(few) > sum(many) / len(many)


@_gallery("orders_carry_the_customers_tier")
def test_gallery_orders_carry_the_customers_tier(tmp_path, monkeypatch):
    events = _events(_gallery_records("orders_carry_the_customers_tier", "yaml"))
    signups = {
        e["value"]["customer_id"]: (e["sim_ts"], e["value"]["tier"])
        for e in events
        if e["task"] == "sign_up"
    }
    orders = [e for e in events if e["task"] == "place_order"]
    assert len(signups) > 10 and len(orders) > 100
    for order in orders:
        when, tier = signups[order["value"]["customer_id"]]
        assert when <= order["sim_ts"]
        assert order["value"]["tier"] == tier
    assert {tier for _, tier in signups.values()} == {"basic", "plus", "pro"}

    # With seed 1 the first orders come before the first sign-up. All three forms drop
    # them and publish the same records.
    slug = "orders_carry_the_customers_tier"
    for name, old, new in (
        (f"{slug}.yaml", "random_seed: 42", "random_seed: 1"),
        (f"{slug}_builder.py", "random_seed=42", "random_seed=1"),
        (f"{slug}_low_level.py", "SEED = 42", "SEED = 1"),
    ):
        text = (GALLERY / name).read_text(encoding="utf-8")
        assert old in text
        (tmp_path / name).write_text(text.replace(old, new), encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "GALLERY", tmp_path)
    seeded = _stream(_gallery_records(slug, "yaml"))
    assert seeded[1][2] != "orders-1" and seeded[0][2] == "signups-1"
    assert [r for r in seeded if r[2] == "orders-1"] == []
    assert _stream(_gallery_records(slug, "builder")) == seeded
    assert _stream(_gallery_records(slug, "low_level")) == seeded


@_gallery("stolen_card_makes_three_large_payments")
def test_gallery_stolen_card_makes_three_large_payments():
    events = _events(_gallery_records("stolen_card_makes_three_large_payments", "yaml"))
    payments = [e for e in events if e["task"] == "payment"]
    large = [e for e in events if e["task"] == "large_payment"]
    stolen = [e for e in payments if e["value"]["stolen"]]
    assert len(payments) > 400 and stolen
    # Three large payments follow each stolen payment, two seconds later, on its card
    assert len(large) == 3 * len(stolen)
    for first in stolen:
        later = [
            e
            for e in large
            if e["key"] == first["key"] and e["sim_ts"] == first["sim_ts"] + 2
        ]
        assert len(later) == 3
    # Every record is keyed by its card, and a large payment is large
    assert all(e["key"] == e["value"]["card"] for e in events)
    assert all(500 <= e["value"]["amount"] < 2000 for e in large)
    assert all(5 <= e["value"]["amount"] < 120 for e in payments)


@_gallery("stock_queries_read_the_latest_level")
def test_gallery_stock_queries_read_the_latest_level():
    events = _events(_gallery_records("stock_queries_read_the_latest_level", "yaml"))
    updates = [e for e in events if e["task"] == "update_stock"]
    queries = [e for e in events if e["task"] == "read_stock"]
    assert len({e["key"] for e in updates}) == 5 and len(queries) > 50
    # Each level is the product's last level plus a change of -12 to 10, kept at 0 or more
    last = {}
    for update in updates:
        sku, level = update["value"]["sku"], update["value"]["level"]
        assert sku == update["key"] and level >= 0
        assert -12 <= level - last.get(sku, 100) <= 10 or level == 0
        last[sku] = level
    # A query gets the level of the latest update of its product at or before its time
    for query in queries:
        sku = query["value"]["sku"]
        before = [
            e
            for e in updates
            if e["value"]["sku"] == sku and e["sim_ts"] <= query["sim_ts"]
        ]
        assert query["value"]["level"] == before[-1]["value"]["level"]
    # Over the run the queries ask for every product
    assert {q["value"]["sku"] for q in queries} == {f"sku-{n}" for n in range(1, 6)}


@_gallery("requests_become_traces_of_nested_spans")
def test_gallery_requests_become_traces_of_nested_spans():
    events = _events(_gallery_records("requests_become_traces_of_nested_spans", "yaml"))
    spans = [e for e in events if "trace_id" in e["value"]]
    traces = {}
    for event in spans:
        traces.setdefault(event["value"]["trace_id"], []).append(event)
    assert len(traces) > 30
    complete = 0
    for trace in traces.values():
        assert len({e["key"] for e in trace}) == 1
        by_name = {e["value"]["name"]: e for e in trace}
        if "GET /orders" not in by_name:  # the run ended before the request did
            continue
        complete += 1
        assert len(trace) == 3
        root = by_name["GET /orders"]
        assert root["value"]["parent_span_id"] is None
        root_start, root_end = root["value"]["start"], root["sim_ts"]
        # The root span's duration is the time from its start to its end
        assert abs((root_end - root_start) * 1000 - root["value"]["duration_ms"]) < 1e-3
        children = [by_name["check token"], by_name["SELECT orders"]]
        for child in children:
            value = child["value"]
            assert value["parent_span_id"] == root["value"]["span_id"]
            # A child runs inside its parent: it starts after the parent starts and
            # ends before the parent ends
            assert value["start"] >= root_start - 1e-9
            assert child["sim_ts"] <= root_end + 1e-9
            # The duration a span reports is the time between its start and its end
            took = (child["sim_ts"] - value["start"]) * 1000
            assert abs(took - value["duration_ms"]) < 1e-6
        # The two calls run one after the other, and the parent's duration covers them
        assert children[0]["sim_ts"] <= children[1]["value"]["start"] + 1e-9
        assert root["value"]["duration_ms"] >= sum(
            c["value"]["duration_ms"] for c in children
        )
    assert complete > 30
    assert len({e["value"]["span_id"] for e in spans}) == len(spans)


@_gallery("customer_deletes_become_kafka_tombstones")
def test_gallery_customer_deletes_become_kafka_tombstones():
    slug = "customer_deletes_become_kafka_tombstones"
    # The sink is a Kafka sink that writes each event's value alone, in the YAML, the
    # Builder and the Low-level form
    for app in (_gallery_yaml(slug), _gallery_module(slug, "builder").build()):
        [egress] = app._egress_providers
        assert type(egress).__name__ == "KafkaEgress"
        assert egress.event_topic == "customers-cdc"
        assert app._egress_envelopes == [False]
    seen = _low_level_run(slug)
    [egress] = seen["sinks"][0]
    assert type(egress).__name__ == "KafkaEgress"
    assert egress.event_topic == "customers-cdc"
    assert seen["options"]["envelopes"] == [False]

    events = _events(_gallery_records(slug, "yaml"))
    live, shares = (
        [],
        {"insert_customer": 0, "update_customer": 0, "delete_customer": 0},
    )
    for event in events:
        key, value = event["key"], event["value"]
        shares[event["task"]] += 1
        if event["task"] == "insert_customer":
            assert key == str(value["customer_id"]) and key not in live
            live.append(key)
        elif event["task"] == "update_customer":
            # An update names a customer that exists and has not been deleted
            assert key == str(value["customer_id"]) and key in live
        else:
            # A delete is a record with a key and no value, for the oldest customer
            assert value is None and key == live.pop(0)
    assert shares["delete_customer"] > 5
    assert shares["insert_customer"] > shares["update_customer"] * 0.5
    assert sum(shares.values()) == len(events)


@_gallery("callers_hang_up_after_waiting_too_long")
def test_gallery_callers_hang_up_after_waiting_too_long():
    events = _events(_gallery_records("callers_hang_up_after_waiting_too_long", "yaml"))
    status = {}
    for event in events:
        if event["task"] == "answer_call" and "status" in event["value"]:
            status.setdefault(event["key"], {})[event["value"]["status"]] = event[
                "sim_ts"
            ]
    hung_up = [e for e in events if e["task"] == "hang_up"]
    answered = [e for e in events if e["value"].get("event_type") == "answered"]
    assert len(hung_up) > 5 and len(answered) > 20
    # A caller hangs up between 1 and 5 minutes after the call joined the queue
    for event in hung_up:
        times = status[event["key"]]
        assert times["reneged"] == event["sim_ts"]
        assert 60 <= times["reneged"] - times["queued"] <= 300
    # A caller who hung up was never answered, and an answered one was not left waiting
    assert not {e["key"] for e in hung_up} & {e["key"] for e in answered}
    for times in status.values():
        if "started" in times:
            assert times["started"] - times["queued"] <= 300
    # With four agents from 30 minutes, fewer than half as many callers hang up as in
    # the first 30 minutes (with two agents throughout, 9 hang up after against 10 before)
    after = [e for e in hung_up if e["sim_ts"] >= 1800]
    assert 2 * len(after) < len(hung_up) - len(after)


@_gallery("urgent_tickets_are_served_first")
def test_gallery_urgent_tickets_are_served_first():
    events = _events(_gallery_records("urgent_tickets_are_served_first", "yaml"))
    queued, waits, priority = {}, {}, {}
    for event in events:
        value = event["value"]
        if value.get("status") == "queued":
            queued[event["key"]] = event["sim_ts"]
        elif value.get("status") == "started":
            waits[event["key"]] = event["sim_ts"] - queued[event["key"]]
        elif value.get("event_type") == "solved":
            priority[event["key"]] = value["priority"]
    mean = {}
    for level in (1, 2, 3):
        wanted = [waits[k] for k, p in priority.items() if p == level]
        assert len(wanted) > 20
        mean[level] = sum(wanted) / len(wanted)
    # The lower the priority number, the shorter the wait
    assert mean[1] < mean[2] < mean[3]
    assert mean[3] > 5 * mean[1]
    # About 10, 30 and 60 percent of the tickets have priority 1, 2 and 3; the lowest
    # priority is a little under its share because some of its tickets are still waiting
    share = [sum(p == n for p in priority.values()) / len(priority) for n in (1, 2, 3)]
    assert all(abs(s - t) < 0.1 for s, t in zip(share, (0.1, 0.3, 0.6)))


@_gallery("shoppers_add_items_then_pay_or_abandon")
def test_gallery_shoppers_add_items_then_pay_or_abandon():
    events = _events(_gallery_records("shoppers_add_items_then_pay_or_abandon", "yaml"))
    carts = {}
    for event in events:
        if "event_type" in event["value"]:
            carts.setdefault(event["key"], []).append(event)
    ended = {
        k: c for k, c in carts.items() if c[-1]["value"]["event_type"] != "item_added"
    }
    assert len(ended) > 100
    for events_of_cart in ended.values():
        *added, last = events_of_cart
        # The count goes up by one with each item, and the last record shows the cart
        assert [e["value"]["items"] for e in added] == list(range(1, len(added) + 1))
        assert last["value"]["items"] == len(added) and len(added) >= 1
        # An item takes 5 to 30 seconds
        times = [e["sim_ts"] for e in added]
        assert all(5 <= b - a <= 30 for a, b in itertools.pairwise(times))
    paid = sum(c[-1]["value"]["event_type"] == "paid" for c in ended.values())
    # 25 of every 40 shoppers who stop pay, which is 62.5 percent
    assert abs(paid / len(ended) - 0.625) < 0.12
    mean_items = sum(len(c) - 1 for c in ended.values()) / len(ended)
    assert abs(mean_items - 2.5) < 0.5
    # Payment uses two tills: some shopper waits for a till, and never more than two pay
    # at once
    queued, started, done = {}, {}, {}
    for event in events:
        if event["task"] != "pay":
            continue
        if event["value"].get("status") == "queued":
            queued[event["key"]] = event["sim_ts"]
        elif event["value"].get("status") == "started":
            started[event["key"]] = event["sim_ts"]
        else:
            done[event["key"]] = event["sim_ts"]
    assert any(started[key] > queued[key] for key in started)
    changes = sorted(
        [(t, 1) for t in started.values()] + [(t, -1) for t in done.values()]
    )
    busy = peak = 0
    for _, change in changes:
        busy += change
        peak = max(peak, busy)
    assert peak == 2


@_gallery("visitors_drop_out_at_each_signup_step")
def test_gallery_visitors_drop_out_at_each_signup_step():
    events = _events(_gallery_records("visitors_drop_out_at_each_signup_step", "yaml"))
    stages = [e["value"] for e in events if "stage" in e["value"]]
    count = {
        stage: sum(v["stage"] == stage for v in stages)
        for stage in ("visited", "signed_up", "trial_started", "paid")
    }
    left = {
        after: sum(v["stage"] == "left" and v["after"] == after for v in stages)
        for after in ("visited", "signed_up", "trial_started")
    }
    assert count["visited"] > 500
    # 40 percent sign up, then 50 percent start a trial, then 25 percent pay
    assert abs(count["signed_up"] / count["visited"] - 0.4) < 0.07
    assert abs(count["trial_started"] / count["signed_up"] - 0.5) < 0.1
    assert abs(count["paid"] / count["trial_started"] - 0.25) < 0.12
    # Each visitor who stopped left after the step they were at. A visitor who has
    # not left or moved on is waiting in a step, which takes up to a minute, when
    # the run ends.
    assert count["visited"] - count["signed_up"] - left["visited"] in range(15)
    assert count["signed_up"] - count["trial_started"] - left["signed_up"] in range(15)
    assert count["trial_started"] - count["paid"] - left["trial_started"] in range(15)
    # A visitor's records are keyed by the visitor, and each ends in paid or left
    ends = {}
    for event in events:
        if event["value"].get("stage") in ("paid", "left"):
            assert event["key"] not in ends
            ends[event["key"]] = event["value"]["stage"]
    assert len(ends) == count["paid"] + sum(left.values())


@_gallery("bets_are_settled_and_winners_paid")
def test_gallery_bets_are_settled_and_winners_paid():
    events = _events(_gallery_records("bets_are_settled_and_winners_paid", "yaml"))
    placed = {e["value"]["bet_id"]: e for e in events if e["task"] == "place_bet"}
    settled = [e for e in events if "result" in e["value"]]
    assert len(settled) > 150
    for event in settled:
        value = event["value"]
        bet = placed[value["bet_id"]]
        # The settled record joins to its bet by id and the entity key, one to two
        # minutes after the bet was placed
        assert event["key"] == bet["key"]
        assert 60 <= event["sim_ts"] - bet["sim_ts"] <= 120
        if value["result"] == "won":
            assert value["payout"] == round(
                bet["value"]["stake"] * bet["value"]["odds"], 2
            )
        else:
            assert value["result"] == "lost" and value["payout"] == 0
    won = sum(e["value"]["result"] == "won" for e in settled)
    assert abs(won / len(settled) - 0.45) < 0.08
    assert {b["value"]["stake"] for b in placed.values()} == {5, 10, 50, 100}


@_gallery("audits_with_findings_need_a_follow_up")
def test_gallery_audits_with_findings_need_a_follow_up():
    events = _events(_gallery_records("audits_with_findings_need_a_follow_up", "yaml"))
    queued, started, finished = {}, {}, {}
    for event in events:
        spot = (event["key"], event["task"])
        status = event["value"].get("status")
        if status == "queued":
            queued[spot] = event["sim_ts"]
        elif status == "started":
            started[spot] = event["sim_ts"]
        else:
            finished[spot] = event
    audits = [e for spot, e in finished.items() if spot[1] == "audit"]
    follow_ups = [e for spot, e in finished.items() if spot[1] == "follow_up"]
    issues = [e for e in audits if e["value"]["finding"] == "issue"]
    assert len(audits) > 100 and len(follow_ups) > 10
    # Each issue is followed up once, for the same merchant and by the same entity, and
    # the audits the run ended before they were followed up make up the difference
    assert len(issues) - len(follow_ups) in range(4)
    for follow_up in follow_ups:
        audit = finished[(follow_up["key"], "audit")]
        assert audit["value"]["finding"] == "issue"
        assert follow_up["value"]["merchant"] == audit["value"]["merchant"]
        assert queued[(follow_up["key"], "follow_up")] == audit["sim_ts"]
    # A follow-up joins the queue behind the audits that wait, so a follow-up waits
    assert any(
        started[spot] > queued[spot] for spot in queued if spot[1] == "follow_up"
    )
    # Audits and follow-ups share two auditors: never more than two work at once
    changes = sorted(
        [(started[spot], 1) for spot in started]
        + [(e["sim_ts"], -1) for e in finished.values()]
    )
    busy = peak = 0
    for _, change in changes:
        busy += change
        peak = max(peak, busy)
    assert peak == 2
    assert 0.05 < len(issues) / len(audits) < 0.3

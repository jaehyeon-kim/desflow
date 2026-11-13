"""A connector gives a second run what a fresh connector gives.

The builder models that `scripts/regression.py capture` runs (the cases of
`test_second_run.py`) are run twice on one context with one connector added, and then
on a context with a new connector after a first run. The destination after the second
run must be the same. A connector that keeps something of its first run, such as a
table it already made, the folders it made, or a client bound to the event loop of the
first run, makes the two differ. Every client is a fake that records the event loop it
was made in and refuses use from another loop, as asyncpg and redis-py do.
"""

import asyncio
import contextlib
import importlib.util
import logging
import os
import shutil
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest
import scipy.optimize  # noqa: F401  # imported before the capture restores sys.modules
import scipy.stats  # noqa: F401

from desflow.connectors.egress.iceberg import IcebergStorageEgress
from desflow.connectors.egress.kafka import KafkaEgress
from desflow.connectors.egress.postgres import PostgresEgress
from desflow.connectors.egress.redis import RedisEgress
from desflow.connectors.egress.storage import (
    JsonlStorageEgress,
    ParquetStorageEgress,
)
from desflow.connectors.ingress.kafka import KafkaIngress
from desflow.connectors.ingress.postgres import PostgresIngress
from desflow.core.environment import IngressMixIn
from desflow.core.sinks import Sink

# It runs every model the regression capture runs, so CI runs it on one Python
# version on each system (scripts/ci_plan.py).
pytestmark = pytest.mark.every_model

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "second_run_cases", ROOT / "tests" / "unit" / "test_second_run.py"
)
assert _spec is not None and _spec.loader is not None
second_run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(second_run)
regression = second_run.regression

# The builder models of the capture: the connectors are added to their context
# The builder models of the capture, without the three-form SimPy examples, whose
# plain models add nothing to what the others hold
MODELS = [
    case
    for case in second_run.CASES
    if case[0].endswith(":builder") and "simpy/" not in case[0]
]
IDS = [case[0] for case in MODELS]


@contextlib.contextmanager
def _capture_environment():
    """The environment `test_second_run.py` runs a capture model in."""
    logging.disable(logging.CRITICAL)
    try:
        with (
            regression._restored_imports(),
            contextlib.chdir(ROOT),
            patch.dict(os.environ, {"PWD": str(ROOT)}),
            patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
        ):
            yield
    finally:
        logging.disable(logging.NOTSET)


def _run(app: Any, until: Any, run: Dict[str, Any], egress: Any = None, ingress=()):
    """Runs a context unpaced with one egress connector and any ingress connectors."""
    app.factor = 0.0
    app.go_live_at = None
    if app.random_seed is None:
        app.random_seed = 11
    app._sinks.ingress = list(ingress)
    app._sinks.egress = [] if egress is None else [Sink(egress)]
    app.run(until=until, **run)
    # The run stops the loops and returns; each thread closes its clients after
    for name in ("_egress_thread", "_ingress_thread"):
        thread = getattr(app.env, name, None)
        if thread is not None:
            thread.join(10)


def _second_and_fresh(
    make: Callable[[], Any],
    until: Any,
    run: Dict[str, Any],
    connector: Callable[[], Any],
    read: Callable[[], Any],
    between: Callable[[], None] = lambda: None,
    reset: Callable[[], None] = lambda: None,
) -> Tuple[Any, Any]:
    """What `read` gives after one connector served two runs of one context, and after
    a first run and then a run of a fresh context with a new connector. `between`
    runs after every first run, and `reset` before the fresh pair of runs."""
    with _capture_environment():
        app, kept = make(), connector()
        _run(app, until, run, kept)
        between()
        _run(app, until, run, kept)
        second = read()
    reset()
    with _capture_environment():
        _run(make(), until, run, connector())
        between()
        _run(make(), until, run, connector())
        fresh = read()
    return second, fresh


def _files(folder: Path) -> Tuple[int, int]:
    """The number of files under a folder and the rows in them."""
    paths = [path for path in folder.rglob("*") if path.is_file()]
    rows = 0
    for path in paths:
        if path.suffix == ".parquet":
            rows += pq.read_metadata(path).num_rows
        else:
            rows += len(path.read_bytes().splitlines())
    return len(paths), rows


@pytest.mark.parametrize("kind", ["jsonl", "parquet"])
@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_a_file_sink_writes_again_after_its_folder_is_removed(
    kind, name, make, until, run, tmp_path
):
    folder = tmp_path / "out"
    sink_class = JsonlStorageEgress if kind == "jsonl" else ParquetStorageEgress
    path = str(folder / "events" / f"events.{kind}")

    def connector():
        return sink_class(default_path=path)

    second, fresh = _second_and_fresh(
        make,
        until,
        run,
        connector,
        lambda: _files(folder),
        between=lambda: shutil.rmtree(folder, ignore_errors=True),
    )
    assert second == fresh, f"{name}: files and rows {second}, fresh {fresh}"


def _iceberg_rows(catalog: Any) -> int:
    """The rows of `sim.t`, or 0 when the table does not exist."""
    if not catalog.table_exists("sim.t"):
        return 0
    return catalog.load_table("sim.t").scan().to_arrow().num_rows


@pytest.mark.parametrize("policy", ["create", "drop_and_create"])
@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_iceberg_follows_its_table_policy_on_every_run(
    policy, name, make, until, run, tmp_path
):
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog(
        "c", uri=f"sqlite:///{tmp_path}/c.db", warehouse=f"file://{tmp_path}/wh"
    )

    def connector():
        return IcebergStorageEgress(
            catalog=catalog, default_table="sim.t", table_policy=policy
        )

    def read():
        rows = _iceberg_rows(catalog)
        for table in catalog.list_tables("sim") if rows else []:
            catalog.drop_table(table)
        return rows

    second, fresh = _second_and_fresh(make, until, run, connector, read)
    assert second == fresh, f"{name}: {second} rows, fresh {fresh}"


class FakeAsyncpg:
    """A stand-in for `asyncpg.create_pool` that records the loop of each pool and
    refuses a pool used from another loop, as asyncpg does."""

    def __init__(self, columns: List[str], seen: threading.Event):
        self.columns = columns
        self.seen = seen
        self.pools: List[Any] = []
        self.statements: List[str] = []
        self.errors: List[str] = []

    async def create_pool(self, *args: Any, **kwargs: Any) -> Any:
        pool = _FakePool(self, asyncio.get_running_loop())
        self.pools.append(pool)
        self.seen.set()
        return pool


class _FakePool:
    def __init__(self, owner: FakeAsyncpg, loop: asyncio.AbstractEventLoop):
        self.owner = owner
        self.loop = loop
        self.closed = False

    def acquire(self) -> Any:
        return _Acquire(self)

    async def close(self) -> None:
        self.closed = True


class _Acquire:
    def __init__(self, pool: _FakePool):
        self.pool = pool

    async def __aenter__(self) -> Any:
        if asyncio.get_running_loop() is not self.pool.loop:
            self.pool.owner.errors.append("a pool used in another event loop")
            raise RuntimeError("attached to a different loop")
        return _FakeConn(self.pool.owner)

    async def __aexit__(self, *exc: Any) -> None:
        return None


class _FakeConn:
    def __init__(self, owner: FakeAsyncpg):
        self.owner = owner

    async def execute(self, sql: str, *args: Any) -> None:
        self.owner.statements.append(" ".join(sql.split()))

    async def executemany(self, sql: str, rows: list) -> None:
        self.owner.statements.append(" ".join(sql.split()) + f" x{len(rows)}")

    async def fetch(self, sql: str, *args: Any) -> list:
        if "WHERE is_applied" in sql:
            return []
        return [{"column_name": c, "data_type": "text"} for c in self.owner.columns]

    async def fetchval(self, sql: str, *args: Any) -> int:
        return 1

    async def fetchrow(self, sql: str, *args: Any) -> dict:
        # The schemas `PostgresEgress` reads at start, for the default `search_path`
        return {"table_schema": "public", "bare_schema": "public"}


@contextlib.contextmanager
def _hold_ingress_until(event: threading.Event):
    """Makes the end of a run wait until the ingress connector has made its client."""
    original = IngressMixIn.teardown_ingress

    def teardown(self: Any) -> None:
        event.wait(10)
        original(self)

    with patch.object(IngressMixIn, "teardown_ingress", teardown):
        yield


@pytest.mark.parametrize("policy", ["create", "drop_and_create"])
@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_postgres_egress_makes_its_pool_and_tables_on_every_run(
    policy, name, make, until, run
):
    fake = FakeAsyncpg(["sim_ts", "path_id"], threading.Event())

    def connector():
        return PostgresEgress(
            "postgresql://u:p@h/db",
            table_name="t",
            tables={"t": {"columns": {"sim_ts": "DOUBLE PRECISION"}}},
            table_policy=policy,
        )

    def read():
        # The writes of a run are not compared: only the set-up statements are
        return [s for s in fake.statements if "INSERT" not in s]

    with patch(
        "desflow.connectors.egress.postgres.asyncpg.create_pool", fake.create_pool
    ):
        second, fresh = _second_and_fresh(
            make, until, run, connector, read, reset=fake.statements.clear
        )
    assert second == fresh, f"{name}: {second}, fresh {fresh}"
    assert fake.errors == [], f"{name}: {fake.errors[:1]}"
    assert all(pool.closed for pool in fake.pools), f"{name}: a pool was left open"


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_postgres_ingress_makes_its_pool_on_every_run(name, make, until, run):
    seen = threading.Event()
    fake = FakeAsyncpg(["path"], seen)

    def connector():
        seen.clear()
        return PostgresIngress("postgresql://u:p@h/db", poll_interval=0.01)

    def run_with(app: Any, ingress: Any) -> None:
        with _hold_ingress_until(seen):
            _run(app, until, run, None, [ingress])

    with (
        _capture_environment(),
        patch(
            "desflow.connectors.ingress.postgres.asyncpg.create_pool", fake.create_pool
        ),
    ):
        app, kept = make(), connector()
        run_with(app, kept)
        seen.clear()
        run_with(app, kept)
        second = [s for s in fake.statements if "CREATE" in s]
        pools = len(fake.pools)
    assert pools == 2, f"{name}: {pools} pools made in two runs"
    assert len(second) == 2, f"{name}: the table set-up ran {len(second)} times"
    assert fake.errors == [], f"{name}: {fake.errors[:1]}"
    assert all(pool.closed for pool in fake.pools), f"{name}: a pool was left open"


class FakeRedis:
    """A stand-in for a redis-py client that refuses use from another loop."""

    def __init__(self, seen: threading.Event):
        self.seen = seen
        self.loop: Any = None
        self.clients: List[Any] = []
        self.errors: List[str] = []
        self.commands: List[str] = []

    def from_url(self, *args: Any, **kwargs: Any) -> Any:
        client = MagicMock()
        client.loop = asyncio.get_running_loop()
        client.closed = False

        pipe = MagicMock()

        async def execute() -> None:
            if asyncio.get_running_loop() is not client.loop:
                self.errors.append("a client used in another event loop")
                raise RuntimeError("attached to a different loop")
            self.commands.append("execute")

        async def aclose() -> None:
            client.closed = True

        pipe.execute = execute
        client.pipeline.return_value = pipe
        client.aclose = aclose
        self.clients.append(client)
        self.seen.set()
        return client


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_redis_egress_makes_its_client_on_every_run(name, make, until, run):
    fake = FakeRedis(threading.Event())

    def connector():
        return RedisEgress("redis://h:6379/0", stream_name="s")

    with patch("desflow.connectors.egress.redis.redis.from_url", fake.from_url):
        second, fresh = _second_and_fresh(
            make,
            until,
            run,
            connector,
            lambda: list(fake.commands),
            reset=fake.commands.clear,
        )
    assert second == fresh, f"{name}: {len(second)} writes, fresh {len(fresh)}"
    assert fake.errors == [], f"{name}: {fake.errors[:1]}"


class FakeKafka:
    """Stand-ins for the aiokafka producer and consumer that record their loops."""

    def __init__(self) -> None:
        self.made: List[Tuple[str, Any]] = []
        self.topics = 0
        self.seen = threading.Event()

    def producer(self, **config: Any) -> Any:
        client = MagicMock()
        self.made.append(("producer", asyncio.get_running_loop()))

        async def noop(*args: Any, **kwargs: Any) -> None:
            return None

        client.start = noop
        client.stop = noop
        client.send = noop
        return client

    def consumer(self, *topics: Any, **config: Any) -> Any:
        fake = self
        loop = asyncio.get_running_loop()
        fake.made.append(("consumer", loop))
        fake.seen.set()

        class Consumer:
            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                return None

            def __aiter__(self) -> Any:
                return self

            async def __anext__(self) -> Any:
                await asyncio.sleep(3600)

        return Consumer()

    def create_topics(self, *args: Any) -> None:
        self.topics += 1


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_kafka_connectors_make_their_clients_and_topics_on_every_run(
    name, make, until, run
):
    fake = FakeKafka()
    with (
        patch("desflow.connectors.egress.kafka.AIOKafkaProducer", fake.producer),
        patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer", fake.consumer),
        patch.object(KafkaEgress, "_create_topics", lambda self: fake.create_topics()),
        patch.object(KafkaIngress, "_create_topic", lambda self: fake.create_topics()),
        _capture_environment(),
    ):
        app = make()
        egress = KafkaEgress(bootstrap_servers="b:9092", topic_router=lambda d: "t")
        ingress = KafkaIngress("t", bootstrap_servers="b:9092")
        for _ in range(2):
            fake.seen.clear()
            with _hold_ingress_until(fake.seen):
                _run(app, until, run, egress, [ingress])
    kinds = [kind for kind, _loop in fake.made]
    assert kinds.count("consumer") == 2, f"{name}: {kinds}"
    assert len({id(loop) for kind, loop in fake.made if kind == "consumer"}) == 2

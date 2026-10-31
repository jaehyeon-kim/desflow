"""Every example with an odctl profile runs against its profile and is read back.

Each example in `desflow.examples.EXAMPLES` with a `profile` runs in its YAML, builder
and low-level versions, from a folder laid out as `desflow examples get` lays it out.
A version runs for a set simulation time and must exit with 0. Then its records are
read from the service: messages on the Kafka topics, rows in the PostgreSQL tables,
entries in the Redis stream, rows in the Iceberg table. The record content is checked
by the twin tests in `tests/docs`, so a count above zero and one record that parses
are enough here.

The tests start nothing. The `service-examples` job in `pipeline.yml` starts the
profile with `odctl up <profile>`, and a developer does the same before running one
example: `pytest tests/integration/test_service_examples.py -m examples -k <example>`.

The Parquet example has no profile, and one test runs it against `storage`, in the
`parquet-s3-example` job.
"""

import asyncio
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

import pytest

from desflow.examples import APIS, EXAMPLES, Example

pytestmark = [pytest.mark.integration, pytest.mark.examples]

EXAMPLES_FOLDER = Path(__file__).resolve().parents[2] / "examples"
BOOTSTRAP_SERVERS = "127.0.0.1:9092"
POSTGRES_DSN = "postgresql://user:password@127.0.0.1:5432/odctl"
REDIS_URL = "redis://user:password@127.0.0.1:6379/0"
# The port to check for each profile before a version runs
PROFILE_PORTS = {"kafka-lite": 9092, "postgres": 5432, "valkey": 6379, "catalog": 8181}


@dataclass(frozen=True)
class Plan:
    """How long an example runs, and what its Python versions read to do it."""

    # Simulation seconds: `--until` of the YAML version, and EXAMPLE_UNTIL of the
    # builder and low-level versions that read it. None leaves the run to the files.
    until: Optional[float] = None
    # Settings of the versions that end on their own and take no EXAMPLE_UNTIL
    env: Dict[str, str] = field(default_factory=dict)


PLANS = {
    "kafka": Plan(until=10),
    "postgres": Plan(until=10),
    "related-records": Plan(until=10),
    "redis": Plan(until=10),
    # Ten minutes of history, then ten seconds live instead of a minute. The YAML
    # version has `logical_start_time: -10m`; the Python versions read the same
    # ten minutes from HISTORY_MINUTES and the live tail from LIVE_SECONDS.
    "backfill-live": Plan(until=610, env={"LIVE_SECONDS": "10"}),
    # A day, generated unpaced as the files say
    "iceberg": Plan(),
}
SERVICE_EXAMPLES = {name: e for name, e in EXAMPLES.items() if e.profile}


# ==========================================
# Reading the service
# ==========================================
KAFKA_TOPICS = {
    "kafka": ("sim-events", "sim-telemetry"),
    "backfill-live": ("sim-events",),
}


async def _kafka_end_offsets(topics: Tuple[str, ...]) -> dict:
    """The end offset of every partition of `topics`, 0 for a topic not created yet."""
    from aiokafka import AIOKafkaConsumer, TopicPartition
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        # Only topics that exist are described: describing a missing topic would
        # create it, with the broker's partition count instead of the example's
        existing = set(await admin.list_topics()) & set(topics)
        described = await admin.describe_topics(sorted(existing)) if existing else []
    finally:
        await admin.close()
    found = [
        TopicPartition(d["topic"], p["partition"])
        for d in described
        for p in d["partitions"]
    ]
    if not found:
        return {}
    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS, group_id=None, enable_auto_commit=False
    )
    await consumer.start()
    try:
        return await consumer.end_offsets(found)
    finally:
        await consumer.stop()


async def _kafka_first_message(partition, offset: int) -> dict:
    """The message at `offset` of one partition, parsed as JSON."""
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS, group_id=None, enable_auto_commit=False
    )
    await consumer.start()
    try:
        consumer.assign([partition])
        consumer.seek(partition, offset)
        message = await asyncio.wait_for(consumer.getone(), timeout=10)
        return json.loads(message.value)
    finally:
        await consumer.stop()


def _kafka_before(example: Example) -> dict:
    return asyncio.run(_kafka_end_offsets(KAFKA_TOPICS[example.name]))


def _kafka_check(example: Example, before: dict) -> None:
    topics = KAFKA_TOPICS[example.name]
    after = asyncio.run(_kafka_end_offsets(topics))
    for topic in topics:
        grown = [
            p for p, end in after.items() if p.topic == topic and end > before.get(p, 0)
        ]
        assert grown, f"No message was added to the Kafka topic {topic}."
        record = asyncio.run(_kafka_first_message(grown[0], before.get(grown[0], 0)))
        assert isinstance(record, dict) and record, f"A {topic} message is empty."


async def _postgres_drop(tables: Tuple[str, ...]) -> None:
    import asyncpg

    conn = await asyncpg.connect(POSTGRES_DSN)
    try:
        for table in tables:
            await conn.execute(f"DROP TABLE IF EXISTS {table}")
    finally:
        await conn.close()


async def _postgres_rows(table: str) -> list:
    import asyncpg

    conn = await asyncpg.connect(POSTGRES_DSN)
    try:
        return [dict(r) for r in await conn.fetch(f"SELECT * FROM {table} LIMIT 5")]
    finally:
        await conn.close()


POSTGRES_TABLES = {
    "postgres": ("orders",),
    "related-records": ("orders", "order_items"),
}


def _postgres_before(example: Example) -> None:
    # The tables are dropped so that a version starts from none, as the egress creates them
    asyncio.run(_postgres_drop(POSTGRES_TABLES[example.name]))


def _postgres_check(example: Example, before: None) -> None:
    for table in POSTGRES_TABLES[example.name]:
        rows = asyncio.run(_postgres_rows(table))
        assert rows, f"No row was written to the PostgreSQL table {table}."
        assert rows[0].get("order_id") is not None, f"{table} has no order_id."


REDIS_STREAM = "order_events"


async def _redis_delete() -> None:
    import redis.asyncio as redis

    client = redis.from_url(REDIS_URL)
    try:
        await client.delete(REDIS_STREAM)
    finally:
        await client.aclose()


async def _redis_entries() -> list:
    import redis.asyncio as redis

    client = redis.from_url(REDIS_URL)
    try:
        return await client.xrange(REDIS_STREAM, count=5) or []
    finally:
        await client.aclose()


def _redis_before(example: Example) -> None:
    asyncio.run(_redis_delete())


def _redis_check(example: Example, before: None) -> None:
    import orjson

    entries = asyncio.run(_redis_entries())
    assert entries, f"No entry was added to the Redis stream {REDIS_STREAM}."
    payload = orjson.loads(entries[0][1][b"payload"])
    assert isinstance(payload, dict) and payload, "A stream entry is empty."


ICEBERG_TABLE = "simulation.events"


def _iceberg_catalog():
    from pyiceberg.catalog.rest import RestCatalog

    # The catalog and credentials the example files default to
    return RestCatalog(
        "odctl",
        **{
            "uri": "http://127.0.0.1:8181",
            "warehouse": "s3://warehouse/",
            "s3.endpoint": "http://127.0.0.1:8333",
            "s3.access-key-id": "user",
            "s3.secret-access-key": "password",
            "s3.region": "us-east-1",
        },
    )


def _iceberg_before(example: Example) -> None:
    from pyiceberg.exceptions import NoSuchTableError

    try:
        _iceberg_catalog().drop_table(ICEBERG_TABLE)
    except NoSuchTableError:
        pass


def _iceberg_check(example: Example, before: None) -> None:
    table = _iceberg_catalog().load_table(ICEBERG_TABLE)
    rows = table.scan().to_arrow()
    assert rows.num_rows > 0, (
        f"No row was written to the Iceberg table {ICEBERG_TABLE}."
    )
    assert rows.column("path_id")[0].as_py() == "Shop.service.packing"


# Each example's way to empty its target before a version and to read it after
READERS: Dict[str, Tuple[Callable, Callable]] = {
    "kafka": (_kafka_before, _kafka_check),
    "backfill-live": (_kafka_before, _kafka_check),
    "postgres": (_postgres_before, _postgres_check),
    "related-records": (_postgres_before, _postgres_check),
    "redis": (_redis_before, _redis_check),
    "iceberg": (_iceberg_before, _iceberg_check),
}


# ==========================================
# Running a version
# ==========================================
def _lay_out(example: Example, api: str, folder: Path) -> Path:
    """Copies a version's files into `folder/api` and the shared files into `folder`,
    as `desflow examples get` does."""
    target = folder / api
    target.mkdir(parents=True)
    for path in example.files[api]:
        shutil.copy(EXAMPLES_FOLDER / path, target / Path(path).name)
    for path in example.shared:
        shutil.copy(EXAMPLES_FOLDER / path, folder / Path(path).name)
    return target


def _command(example: Example, api: str, plan: Plan) -> list:
    """The command the example's page gives for one version, with the run length."""
    main_file = Path(example.files[api][0]).name
    if api != "yaml":
        return [sys.executable, main_file]
    # The page's command, as `run_hint` gives it, run with the `desflow` script of
    # the environment that runs pytest
    words = shlex.split(example.yaml_command or f"desflow run {main_file}")
    command = [str(Path(sys.executable).with_name("desflow")), *words[1:]]
    return command + (["--until", str(plan.until)] if plan.until else [])


def _versions() -> list:
    return [
        pytest.param(name, api, id=f"{name}-{api}")
        for name in SERVICE_EXAMPLES
        for api in APIS
    ]


@pytest.mark.parametrize("name, api", _versions())
def test_version_runs_and_its_records_are_in_the_service(
    name, api, tmp_path, profile_running
):
    example, plan = SERVICE_EXAMPLES[name], PLANS[name]
    profile_running(example.profile, PROFILE_PORTS[example.profile])
    before_run, check = READERS[name]
    state = before_run(example)

    env = {**os.environ, **plan.env}
    if plan.until:
        env["EXAMPLE_UNTIL"] = str(plan.until)
    else:
        env.pop("EXAMPLE_UNTIL", None)
    run = subprocess.run(
        _command(example, api, plan),
        cwd=_lay_out(example, api, tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
    )
    tail = "\n".join((run.stdout + run.stderr).strip().splitlines()[-15:])
    assert run.returncode == 0, (
        f"The {api} version exited with {run.returncode}:\n{tail}"
    )

    check(example, state)


# ==========================================
# The read-back commands of the pages
# ==========================================
READ_BACK_PAGES = {
    "kafka": "kafka.md",
    "backfill-live": "backfill-live.md",
    "postgres": "postgres.md",
    "related-records": "advanced-postgres-orders.md",
    "redis": "redis.md",
}


def _read_back_commands(page: str) -> list:
    """The `docker exec` commands of the page's Check the Result section, as the page
    writes them, one list of words for each command."""
    text = (EXAMPLES_FOLDER.parent / "docs" / "how-to" / "examples" / page).read_text(
        encoding="utf-8"
    )
    section = text.split("## Check the Result", 1)[1].split("\n## ", 1)[0]
    commands = []
    for block in re.findall(r"^ *```bash\n(.*?)\n *```", section, re.S | re.M):
        for line in block.replace("\\\n", " ").splitlines():
            if line.startswith("docker exec"):
                commands.append(shlex.split(line))
    return commands


def _python_read_back(page: str, marker: str) -> str:
    """The `python -c` command of the page's Check the Result section that holds
    `marker`, as the pip reader runs it."""
    text = (EXAMPLES_FOLDER.parent / "docs" / "how-to" / "examples" / page).read_text(
        encoding="utf-8"
    )
    section = text.split("## Check the Result", 1)[1].split("\n## ", 1)[0]
    for block in re.findall(r"^ *```bash\n(.*?)\n *```", section, re.S | re.M):
        command = textwrap.dedent(block)
        if command.startswith("python -c") and marker in command:
            return command
    raise AssertionError(f"{page} gives no python read-back with {marker}")


def _run_python_read_back(command: str) -> str:
    path = f"{Path(sys.executable).parent}{os.pathsep}{os.environ['PATH']}"
    output = subprocess.run(
        ["bash", "-c", command],
        env={**os.environ, "PATH": path},
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert output.returncode == 0, output.stderr[-400:]
    return output.stdout.strip()


def test_the_iceberg_page_read_back_prints_the_stated_answer(tmp_path, profile_running):
    """After the YAML version has run, the page's command prints its row and snapshot
    counts through the REST catalog."""
    example, plan = SERVICE_EXAMPLES["iceberg"], PLANS["iceberg"]
    profile_running(example.profile, PROFILE_PORTS[example.profile])
    _iceberg_before(example)
    run = subprocess.run(
        _command(example, "yaml", plan),
        cwd=_lay_out(example, "yaml", tmp_path),
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert run.returncode == 0, "\n".join(run.stderr.splitlines()[-15:])
    command = _python_read_back("iceberg.md", "load_catalog")
    assert _run_python_read_back(command) == "515786 3"


def test_the_parquet_page_s3_read_back_prints_the_stated_answer(
    tmp_path, profile_running
):
    """With the page's S3 variables the YAML version writes to the object store, and
    the page's command counts the same rows there."""
    from pyarrow import fs

    example = EXAMPLES["parquet"]
    profile_running("storage", 8333)
    # The count covers the whole folder, so an earlier run must not be in it
    store = fs.S3FileSystem(
        endpoint_override="127.0.0.1:8333",
        scheme="http",
        access_key="user",
        secret_key="password",
    )
    # A fresh store has no bucket yet, and the run creates it
    if store.get_file_info("odctl-dev").type != fs.FileType.NotFound:
        store.delete_dir_contents("odctl-dev/history", missing_dir_ok=True)
    env = {
        **os.environ,
        "PARQUET_FILESYSTEM": "s3",
        "DEST_PATH": "odctl-dev/history",
        "S3_ENDPOINT": "http://127.0.0.1:8333",
        "S3_ACCESS_KEY": "user",
        "S3_SECRET_KEY": "password",
    }
    run = subprocess.run(
        _command(example, "yaml", Plan()),
        cwd=_lay_out(example, "yaml", tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert run.returncode == 0, "\n".join(run.stderr.splitlines()[-15:])
    command = _python_read_back("parquet.md", "S3FileSystem")
    assert _run_python_read_back(command) == "3621530"


@pytest.mark.parametrize("name", READ_BACK_PAGES)
def test_the_page_read_back_commands_print_the_stated_answers(
    name, tmp_path, profile_running
):
    """After the YAML version has run, each `docker exec` command that the page gives
    prints what the page says about it."""
    example, plan = SERVICE_EXAMPLES[name], PLANS[name]
    profile_running(example.profile, PROFILE_PORTS[example.profile])
    before_run, _ = READERS[name]
    before_run(example)
    # The page reads from the beginning of a topic that it expects only this run to fill
    for topic in KAFKA_TOPICS.get(name, ()):
        subprocess.run(
            "docker exec kafka /opt/kafka/bin/kafka-topics.sh "
            f"--bootstrap-server localhost:9092 --delete --if-exists --topic {topic}".split(),
            check=True,
            capture_output=True,
        )
    env = {**os.environ, **plan.env}
    run = subprocess.run(
        _command(example, "yaml", plan),
        cwd=_lay_out(example, "yaml", tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert run.returncode == 0, "\n".join(run.stderr.splitlines()[-15:])

    page = READ_BACK_PAGES[name]
    commands = _read_back_commands(page)
    assert commands, f"{page} gives no docker exec command to read the result back"
    text = (EXAMPLES_FOLDER.parent / "docs" / "how-to" / "examples" / page).read_text(
        encoding="utf-8"
    )
    outputs = [
        subprocess.run(command, capture_output=True, text=True, timeout=120)
        for command in commands
    ]
    for command, output in zip(commands, outputs):
        assert output.returncode == 0, (command, output.stderr[-400:])
    printed = [o.stdout + o.stderr for o in outputs]

    if name == "kafka":
        assert "Processed a total of 3 messages" in text
        assert "Processed a total of 3 messages" in printed[0]
        records = [json.loads(x) for x in printed[0].splitlines() if x.startswith("{")]
        assert [(r["key"], r["value"]["status"]) for r in records] == [
            ("orders-1", "queued"),
            ("orders-1", "started"),
            ("orders-2", "queued"),
        ]
        assert records[0]["sim_ts"] == records[1]["sim_ts"] == 1.276
    elif name == "backfill-live":
        assert "Processed a total of 1 messages" in printed[0]
        first = json.loads(
            next(x for x in printed[0].splitlines() if x.startswith("{"))
        )
        assert (first["key"], first["sim_ts"]) == ("orders-320", 600.111)
    elif name == "postgres":
        assert "(3 rows)" in text and "(3 rows)" in printed[0]
        rows = [x.split("|") for x in printed[0].splitlines() if "pending" in x]
        assert [r[0].strip() for r in rows] == ["0", "1", "2"]
        assert {(r[1].strip(), r[2].strip()) for r in rows} == {("42", "59.97")}
    elif name == "related-records":
        assert "(3 rows)" in text and "(3 rows)" in printed[0]
        rows = [
            [c.strip() for c in x.split("|")]
            for x in printed[0].splitlines()
            if x.strip()[:1].isdigit()
        ]
        assert rows[0] == ["1", "82.68", "1", "82.68"]
        assert all(1 <= int(r[2]) <= 5 and r[1] == r[3] for r in rows[:2])
        assert rows[2][1:4:2] == ["332.3", "332.30"]
    elif name == "redis":
        assert all("--no-auth-warning" in " ".join(c) for c in commands)
        assert "Warning" not in "".join(o.stderr for o in outputs)
        assert int(printed[0].strip()) > 0
        assert "payload" in printed[1] and "orders-1" in printed[1]

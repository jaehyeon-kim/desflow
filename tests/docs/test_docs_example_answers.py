"""The figures that the example pages state are what the examples give.

A page of the service examples states the answer of its read-back: rows in a file,
rows in a table, the first row. Reading the answer back from the service needs
Docker, which tests/integration/test_service_examples.py does. These tests run the
same blueprints with a sink on the local disk in place of the service, so the figures
that do not depend on the service are checked without it.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"
PAGES = ROOT / "docs" / "how-to" / "examples"
DESFLOW = str(Path(sys.executable).with_name("desflow"))


def _page(name: str) -> str:
    return (PAGES / name).read_text(encoding="utf-8")


def _desflow(folder: Path, *arguments: str) -> str:
    run = subprocess.run(
        [DESFLOW, *arguments], cwd=folder, capture_output=True, text=True, timeout=300
    )
    assert run.returncode == 0, run.stderr[-800:]
    return run.stdout


def _blueprint(tmp_path: Path, name: str, folder: str = "yaml") -> Path:
    shutil.copy(EXAMPLES / folder / name, tmp_path)
    return tmp_path / name


def test_parquet_rows_and_files_are_the_pages_figures(tmp_path):
    """A week of the shop is 3621530 rows in 725 files, as the page says."""
    import pyarrow.dataset as ds

    _blueprint(tmp_path, "parquet.yaml")
    _desflow(tmp_path, "run", "parquet.yaml")
    files = list((tmp_path / "data").glob("*.parquet"))
    rows = ds.dataset(tmp_path / "data", format="parquet").count_rows()
    page = _page("parquet.md")
    assert (rows, len(files)) == (3621530, 725)
    assert f"It prints `{rows}`" in page
    assert f"`ls data | wc -l` prints `{len(files)}`" in page


def _backfill_without_kafka(tmp_path: Path) -> Path:
    """The blueprint with its Kafka sink replaced by the console, which needs no broker."""
    text = _blueprint(tmp_path, "backfill_live.yaml").read_text()
    kafka = text[text.index("  - type: Kafka") : text.index("# --8<-- [end:split]")]
    path = tmp_path / "history_only.yaml"
    path.write_text(text.replace(kafka, "  - type: Console\n    when: live\n"))
    return path


def test_backfill_live_history_rows_and_first_live_event(tmp_path):
    """957 history rows, and the first event at or after go-live is orders-320 queued
    at 600.111, as the page says. The rows stay below batch_size, so the page's 10
    seconds rests on the flush_interval setting, which this checks. The timing itself
    was measured once and is not checked here."""
    import pyarrow.parquet as pq
    import yaml

    path = _backfill_without_kafka(tmp_path)
    _desflow(tmp_path, "run", path.name, "--until", "600")
    rows = pq.read_table(tmp_path / "data" / "backfill").num_rows
    page = _page("backfill-live.md")
    assert rows == 957
    assert f"It prints `{rows}`" in page
    batching = yaml.safe_load(path.read_text())["batching"]
    assert rows < batching["batch_size"] and batching["flush_interval"] == 10.0
    assert "about 10 seconds after go-live" in page
    sample = _desflow(
        tmp_path, "run", "backfill_live.yaml", "--sample", "2500", "--stdout"
    )
    events = [json.loads(line) for line in sample.splitlines() if line.startswith("{")]
    first = next(
        e for e in events if e["stream_type"] == "event" and e["sim_ts"] >= 600
    )
    assert (first["key"], first["value"]["status"], first["sim_ts"]) == (
        "orders-320",
        "queued",
        600.111,
    )
    assert "(`orders-320` queued, at `sim_ts` 600.111)" in page


def test_iceberg_rows_and_snapshots_are_the_pages_figures(tmp_path):
    """A day of the shop is 515786 rows in 3 snapshots, as the page says, here in a
    SQLite catalog with a local warehouse in place of the REST catalog."""
    from pyiceberg.catalog import load_catalog

    text = _blueprint(tmp_path, "iceberg.yaml").read_text()
    rest = text[text.index("      catalog:\n") : text.index("      default_table")]
    properties = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{tmp_path}/warehouse",
    }
    local = "      catalog:\n        name: local\n" + "".join(
        f"        {key}: {value}\n" for key, value in properties.items()
    )
    (tmp_path / "warehouse").mkdir()
    (tmp_path / "iceberg.yaml").write_text(text.replace(rest, local))
    _desflow(tmp_path, "run", "iceberg.yaml")
    table = load_catalog("local", **properties).load_table("simulation.events")
    rows, snapshots = table.scan().to_arrow().num_rows, len(table.snapshots())
    assert (rows, snapshots) == (515786, 3)
    assert f"`{rows} {snapshots}`" in _page("iceberg.md")


def test_related_records_first_rows_are_the_pages_figures(tmp_path):
    """Order 1 has 1 item and a total of 82.68, and order 3 a total of 332.3, which
    the page's query prints as 332.30 for the sum of its items."""
    folder = EXAMPLES / "yaml" / "advanced"
    for name in ("postgres_orders.yaml", "postgres_orders_logic.py"):
        shutil.copy(folder / name, tmp_path)
    sample = _desflow(
        tmp_path, "run", "postgres_orders.yaml", "--sample", "12", "--stdout"
    )
    events = [json.loads(line) for line in sample.splitlines() if line.startswith("{")]
    items = {}
    totals = {}
    for event in events:
        value = event["value"]
        if "order_item_id" in value:
            items.setdefault(value["order_id"], []).append(value)
        elif "total_amount" in value:
            totals[value["order_id"]] = value["total_amount"]
    assert len(items[1]) == 1 and totals[1] == 82.68
    assert totals[3] == 332.3
    assert round(sum(i["quantity"] * i["unit_price"] for i in items[3]), 2) == 332.3
    page = _page("advanced-postgres-orders.md")
    assert "order 1 with 1 item and a total of 82.68" in page
    assert "the third row shows `332.3` and `332.30`" in page


def test_valkey_read_back_commands_hide_the_password_warning():
    """Both read-back commands pass --no-auth-warning, so the page need not mention it."""
    page = _page("redis.md")
    commands = [
        line
        for line in page.splitlines()
        if line.startswith("docker exec valkey valkey-cli")
    ]
    assert len(commands) == 2
    assert all("--no-auth-warning" in line for line in commands)
    assert "warning about passing a password" not in page

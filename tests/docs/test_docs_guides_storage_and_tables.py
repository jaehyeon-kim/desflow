"""The Storage and Tables pages run and show what they say.

The daily-file step of Write to Cloud Storage writes in each form 23 orders to the file
of 1 January and 24 to the file of 2 January. Data at Scale writes a file for each day in
each form and splits the customers over runs that share a folder. The first batch of Write
to Iceberg Tables sets the columns of a table."""

import json
import os
import shutil
import sys

import pytest
from how_to_support import GUIDES, ROOT, YAML, copy_into
from how_to_support import load_from as _load_from

from desflow import SimulationContext


def _daily_rows(folder):
    """The rows of each file in `folder`, keyed by the day in the file name."""
    return {
        file.name[:10]: [json.loads(line) for line in file.read_text().splitlines()]
        for file in sorted(folder.glob("*.jsonl"))
    }


def _run_form(form, folder, monkeypatch):
    """Runs one form of the daily-file shop in `folder` and returns its daily rows."""
    folder.mkdir()
    copy_into(
        folder,
        GUIDES / "cloud_storage_stores.py",
        GUIDES / "cloud_storage_daily.py",
        GUIDES / "cloud_storage_daily_low_level.py",
        YAML / "cloud_storage_daily.yaml",
    )
    shutil.copytree(YAML / "cloud_storage_stores", folder / "cloud_storage_stores")
    monkeypatch.chdir(folder)
    for name in ("cloud_storage_stores", "cloud_storage_daily"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    if form == "yaml":
        SimulationContext.from_yaml(folder / "cloud_storage_daily.yaml").run(
            until=172800
        )
    elif form == "builder":
        _load_from(folder, "cloud_storage_daily").app.run(until=172800)
    else:
        low_level = _load_from(folder, "cloud_storage_daily_low_level")
        env = low_level.shop()
        env.run(until=172800)
        env.teardown()
    assert not list((folder / "output" / "daily").glob("other*"))
    return _daily_rows(folder / "output" / "daily")


def test_daily_files_hold_the_orders_of_their_day(tmp_path, monkeypatch):
    """Each form writes 23 orders to the file of 1 January and 24 to the file of
    2 January, and the three forms write the same rows: the same keys, tasks and
    values, which differ only by the clock time of the run."""
    monkeypatch.setenv("STORAGE", "local")
    monkeypatch.setenv("STORAGE_BUCKET", "output")
    forms = {
        form: _run_form(form, tmp_path / form, monkeypatch)
        for form in ("yaml", "builder", "low_level")
    }
    for rows in forms.values():
        assert {day: len(found) for day, found in rows.items()} == {
            "2026-01-01": 23,
            "2026-01-02": 24,
        }
        assert all("__path__" not in row for found in rows.values() for row in found)
        assert rows["2026-01-02"][0]["timestamp"].startswith("2026-01-02T00:00:00")
        assert rows["2026-01-01"][0]["key"] == "orders-1"

    def without_clock(rows):
        return {
            day: [{k: v for k, v in row.items() if k != "timestamp"} for row in found]
            for day, found in rows.items()
        }

    assert without_clock(forms["yaml"]) == without_clock(forms["builder"])
    assert without_clock(forms["yaml"]) == without_clock(forms["low_level"])


def _scale_folder(tmp_path, monkeypatch, **variables):
    """A folder holding the Data at Scale files, with the variables set as a reader sets
    them, and the folder the run writes to."""
    tmp_path.mkdir(exist_ok=True)
    copy_into(
        tmp_path,
        GUIDES / "data_at_scale.py",
        GUIDES / "data_at_scale_low_level.py",
        GUIDES / "data_at_scale_check.py",
        YAML / "data_at_scale.yaml",
    )
    monkeypatch.chdir(tmp_path)
    for name, value in variables.items():
        monkeypatch.setenv(name, str(value))
    for name in ("data_at_scale", "data_at_scale_low_level"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    return tmp_path


def _run_scale_form(form, folder):
    if form == "yaml":
        SimulationContext.from_yaml(
            folder / "data_at_scale.yaml",
            variables={k: v for k, v in os.environ.items() if k.isupper()},
        ).run(until=int(os.environ["DAYS"]) * 86400)
    elif form == "builder":
        _load_from(folder, "data_at_scale").build_shop().run(
            until=int(os.environ["DAYS"]) * 86400
        )
    else:
        env = _load_from(folder, "data_at_scale_low_level").shop()
        env.run(until=int(os.environ["DAYS"]) * 86400)
        env.teardown()


def test_data_at_scale_writes_a_file_for_each_day_in_every_form(tmp_path, monkeypatch):
    """Each form of the Data at Scale shop writes one Parquet file for each of the 3
    days, no row with a `__path__` column, and the volume the check expects."""
    import pyarrow.parquet as pq

    totals = {}
    for form in ("yaml", "builder", "low_level"):
        folder = _scale_folder(
            tmp_path / form, monkeypatch, CUSTOMERS=20, DAYS=3, OUT="scale_output"
        )
        _run_scale_form(form, folder)
        found = sorted((folder / "scale_output").glob("*.parquet"))
        assert [f.name[:10] for f in found] == [
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
        ]
        assert "__path__" not in pq.read_table(found[0]).column_names
        assert not list((folder / "scale_output").glob("other*"))
        monkeypatch.syspath_prepend(str(folder))
        monkeypatch.delitem(sys.modules, "data_at_scale_check", raising=False)
        from data_at_scale_check import check

        totals[form] = check("scale_output", 20, 3)
        # a check that expects one more day than the run covered must fail
        with pytest.raises(AssertionError, match="days have a file"):
            check("scale_output", 20, 4)
    assert all(40 < total < 60 for total in totals.values())


def test_data_at_scale_shards_write_to_one_folder_without_clashes(
    tmp_path, monkeypatch
):
    """The page splits the customers over runs with their own FIRST and SEED: two
    shards of 10 customers write two files for each day to the same folder, with no
    customer and no order id in both."""
    import pyarrow.parquet as pq

    folder = _scale_folder(tmp_path, monkeypatch, DAYS=2, OUT="shared")
    seen = []
    for first, seed in ((1, 1), (11, 2)):
        monkeypatch.setenv("FIRST", str(first))
        monkeypatch.setenv("SEED", str(seed))
        monkeypatch.setenv("CUSTOMERS", "10")
        _run_scale_form("yaml", folder)
        seen.append(first)
    found = sorted((folder / "shared").glob("*.parquet"))
    assert len(found) == 4
    table = pq.read_table(folder / "shared").to_pydict()
    numbers = sorted({int(c.split("-")[1]) for c in table["customer"]})
    assert numbers[0] == 1 and numbers[-1] == 20
    assert len(set(table["order_id"])) == len(table["order_id"])


def _iceberg_rows(tmp_path, table, schemas, caplog):
    """Writes two batches of `{key, amount}` events to `table` and returns the `amount`
    column read back. The second batch holds a fraction."""
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog

    from desflow import IcebergStorageEgress

    properties = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{tmp_path}/warehouse",
    }
    sink = IcebergStorageEgress(
        catalog={"name": "local", **properties},
        default_table=table,
        schemas=schemas,
    )

    def event(amount):
        return {
            "stream_type": "event",
            "key": "k",
            "value": {"key": "a", "amount": amount},
        }

    with caplog.at_level("ERROR"):
        sink._write_batch([event(1), event(2)], pa)
        sink._write_batch([event(2.5)], pa)
    found = load_catalog("local", **properties).load_table(table).scan().to_arrow()
    return found.column("amount").to_pylist()


def test_the_first_batch_sets_the_columns_of_an_iceberg_table(tmp_path, caplog):
    """The Write to Iceberg Tables page: a fraction after whole numbers is logged and
    lost, and `schemas` with a double column keeps every row."""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    assert _iceberg_rows(tmp_path / "a", "shop.plain", None, caplog) == [1, 2]
    message = (
        "could not write 1 rows to shop.events: row 1 holds 2.5 for the int64 column "
        "'amount', which would lose its fraction. Iceberg cannot change the type of a "
        "column that exists. Name the column double in schemas before the table is "
        "created (for a list or struct column, in a PyArrow schema), or create the "
        "table again with table_policy: drop_and_create"
    )
    assert message.replace("shop.events", "shop.plain") in caplog.text
    fixed = {"shop.fixed": {"key": "string", "amount": "double"}}
    page = (ROOT / "docs/how-to/storage-and-tables/iceberg-tables.md").read_text(
        "utf-8"
    )
    assert message in page
    assert "`schemas: {shop.events: {key: string, amount: double}}`" in page
    assert sorted(_iceberg_rows(tmp_path / "b", "shop.fixed", fixed, caplog)) == [
        1.0,
        2.0,
        2.5,
    ]

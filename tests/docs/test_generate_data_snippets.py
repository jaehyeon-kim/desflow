"""The Generate Data snippets in `docs/snippets/generate-data/`.

Each snippet is written three times with the same records: `<slug>.yaml`,
`<slug>_builder.py` and `<slug>_low_level.py`. A test runs each form unpaced into a
capture, compares the three, and asserts what the snippet's page says it shows. The
sinks of a snippet that is about a sink are replaced by the capture, as the gallery
tests do, so nothing here contacts a broker or a database.
"""

import asyncio
import csv
import datetime
import importlib.util
import json
import queue
import statistics
from collections import Counter, defaultdict
from pathlib import Path

import pytest

from desflow import DynamicRealtimeEnvironment, SimulationContext
from desflow.connectors.egress.base import BaseEgress

ROOT = Path(__file__).resolve().parents[2]
SNIPPETS = ROOT / "docs" / "snippets" / "generate-data"

# The snippets written in three forms, by slug. The Low-level form of each is run to its
# own `UNTIL`, and a snippet that ends itself has none.
SLUGS = sorted(
    p.stem
    for p in SNIPPETS.glob("*.yaml")
    if (SNIPPETS / f"{p.stem}_builder.py").exists()
)
# Blueprints that are one sink of a snippet written in three forms
SINK_VARIANTS = {
    "send_data_to_kafka",
    "send_data_to_postgres",
    "send_data_to_a_file",
}
_TESTED: set = set()


def _snippet(*slugs):
    """Marks the test that asserts what the snippets `slugs` show."""

    def mark(test):
        _TESTED.update(slugs)
        return test

    return mark


class _Capture(BaseEgress):
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _module(slug, form):
    """Loads `<slug>_<form>.py` as a module."""
    path = SNIPPETS / f"{slug}_{form}.py"
    spec = importlib.util.spec_from_file_location(f"generate_data_{slug}_{form}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _context(slug, form="yaml"):
    if form == "yaml":
        return SimulationContext.from_yaml(SNIPPETS / f"{slug}.yaml")
    return _module(slug, form).build()


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


def _run(slug, form="yaml", until=None):
    """The data records and the label records of one form of a snippet, run unpaced.

    A blueprint ends where its `run` section says and a Builder or Low-level file at
    `UNTIL`, and `until` overrides both. Every sink is replaced by one capture for the
    data and one for the labels, so the records come in the order they were published.
    """
    captures = [_Capture(), _Capture()]
    if form == "low_level":
        module = _module(slug, form)
        env = DynamicRealtimeEnvironment(factor=0, **module.ENVIRONMENT)
        env.setup_egress(captures, lag_monitor_interval=None, labels=[False, True])
        module.build(env)
        try:
            env.run(until=until or module.UNTIL)
        finally:
            env.teardown()
        return captures[0].records, captures[1].records
    context = _context(slug, form)
    end = until
    if form == "builder":
        end = until or _module(slug, form).UNTIL
    context.factor = 0.0
    context.go_live_at = None
    context._before_run = []
    context._ingress_providers = []
    for name in _SINK_LISTS:
        setattr(context, name, [])
    context.add_egress(captures[0])
    context.add_egress(captures[1], when="labels")
    context.run(until=end)
    return captures[0].records, captures[1].records


def _records(slug, form="yaml", until=None):
    """The data records of one form of a snippet."""
    return _run(slug, form, until)[0]


def _stream(records):
    """Every record without its wall-clock fields."""
    return [
        (
            r["stream_type"],
            r["sim_ts"],
            r.get("key", r.get("path_id")),
            r.get("task"),
            r.get("instance"),
            r["label"] if r["stream_type"] == "label" else r["value"],
        )
        for r in records
        if not str(r.get("path_id", "")).startswith("system.")
    ]


def _rows(records, task):
    """The finished payloads of one task, in order, without its queued and started."""
    return [
        r["value"]
        for r in records
        if r["stream_type"] == "event"
        and r["task"] == task
        and "status" not in r["value"]
    ]


@pytest.mark.parametrize("slug", SLUGS)
def test_forms_publish_the_same_records(slug):
    """The Builder form publishes the YAML form's whole record stream, and the Low-level
    form the same records and labels."""
    yaml_records, yaml_labels = _run(slug)
    assert _stream(yaml_records)
    for form in ("builder", "low_level"):
        records, labels = _run(slug, form)
        assert _stream(records) == _stream(yaml_records), form
        assert _stream(labels) == _stream(yaml_labels), form


def test_every_snippet_has_its_forms_and_a_test():
    """A new snippet cannot land with a form missing or untested, and the folder holds
    nothing but the forms and the CSV file. A Low-level file is plain Python: it draws
    with `Sampler` and imports no value runtime, template or helper module."""
    assert SLUGS
    for slug in SLUGS:
        assert (SNIPPETS / f"{slug}_low_level.py").is_file(), slug
        text = (SNIPPETS / f"{slug}_low_level.py").read_text(encoding="utf-8")
        for name in ("ValueRuntime", "Template", "bind", "generate_data_support"):
            assert name not in text, (slug, name)
    for path in SNIPPETS.glob("*.yaml"):
        assert "!python" not in path.read_text(encoding="utf-8"), path.name
    yaml_files = {p.stem for p in SNIPPETS.glob("*.yaml")}
    assert yaml_files == set(SLUGS) | SINK_VARIANTS
    assert yaml_files <= _TESTED, sorted(yaml_files - _TESTED)
    expected = {f"{slug}.yaml" for slug in yaml_files} | {
        f"{slug}_{form}.py" for slug in SLUGS for form in ("builder", "low_level")
    }
    expected |= {"realistic_fields_products.csv"}
    assert {p.name for p in SNIPPETS.iterdir() if p.name != "__pycache__"} == expected


# ---------------------------------------------------------------------------
# Send Data to a Sink
# ---------------------------------------------------------------------------


@_snippet("send_data_to_a_sink", "send_data_to_kafka", "send_data_to_postgres")
def test_send_data_to_a_sink_sends_the_same_records_to_each_sink():
    """The console file generates 1-based order numbers, customers 1 to 100 and totals
    of 5 to 200, and each sink file writes the same records to its own connector."""
    records = _rows(_records("send_data_to_a_sink"), "order")
    assert len(records) >= 5
    assert [r["order_id"] for r in records] == list(range(1, len(records) + 1))
    assert all(1 <= r["customer_id"] <= 100 for r in records)
    assert all(5 <= r["total"] < 200 for r in records)
    for slug, class_name in (
        ("send_data_to_kafka", "KafkaEgress"),
        ("send_data_to_postgres", "PostgresEgress"),
    ):
        [egress] = _context(slug)._egress_providers
        assert type(egress).__name__ == class_name, slug
        assert _stream(_records(slug)) == _stream(_records("send_data_to_a_sink")), slug
    [kafka] = _context("send_data_to_kafka")._egress_providers
    assert kafka.event_topic == "orders"
    [postgres] = _context("send_data_to_postgres")._egress_providers
    assert list(postgres.tables) == ["orders"]
    columns, primary_key = postgres.tables["orders"]
    assert list(columns) == ["order_id", "customer_id", "total"]
    assert primary_key == ["order_id"]


@_snippet("send_data_to_a_file")
def test_send_data_to_a_file_writes_the_records_to_a_jsonl_file(tmp_path, monkeypatch):
    """The file egress writes the console file's records as JSON Lines rows."""
    monkeypatch.setenv("DEST_PATH", str(tmp_path))
    context = _context("send_data_to_a_file")
    context.factor = 0.0
    context.run()
    rows = _json_rows(tmp_path, "orders")
    expected = _rows(_records("send_data_to_a_sink"), "order")
    assert len(rows) == len(expected) >= 5
    assert [(r["order_id"], r["customer_id"], r["total"]) for r in rows] == [
        (r["order_id"], r["customer_id"], r["total"]) for r in expected
    ]


def _json_rows(folder, name):
    """The rows of the JSON Lines files that a Jsonl egress wrote for `name`."""
    files = sorted(folder.glob(f"{name}*.jsonl"))
    assert files, f"no {name}*.jsonl in {folder}"
    return [
        json.loads(line)
        for file in files
        for line in file.read_text(encoding="utf-8").splitlines()
    ]


def test_send_data_to_a_sink_forms_make_each_sink():
    """The Builder and Low-level files make the same four sinks, and the Builder takes
    one of them for its run."""
    for form in ("builder", "low_level"):
        module = _module("send_data_to_a_sink", form)
        assert type(module.console_sink()).__name__ == "ConsoleEgress"
        assert type(module.kafka_sink()).__name__ == "KafkaEgress"
        assert type(module.postgres_sink()).__name__ == "PostgresEgress"
        assert type(module.file_sink()).__name__ == "JsonlStorageEgress"
        assert module.kafka_sink().event_topic == "orders"
    module = _module("send_data_to_a_sink", "builder")
    [egress] = module.build(sink=module.kafka_sink)._egress_providers
    assert type(egress).__name__ == "KafkaEgress"
    [egress] = module.build()._egress_providers
    assert type(egress).__name__ == "ConsoleEgress"


# ---------------------------------------------------------------------------
# Realistic Fields
# ---------------------------------------------------------------------------


@_snippet("realistic_fields")
def test_realistic_fields_come_from_faker_and_one_csv_row():
    from faker.providers.address.de_DE import Provider as German

    products = {
        row["sku"]: row
        for row in csv.DictReader(
            (SNIPPETS / "realistic_fields_products.csv").open(encoding="utf-8")
        )
    }
    records = _rows(_records("realistic_fields", until=20000), "signup")
    assert len(records) > 9_000
    assert all(
        "@" in r["email"] and r["name"].strip() and r["city"].strip() for r in records
    )
    # The locale changes the city: every German city is one of the German provider's
    german = {r["german_city"] for r in records}
    assert german <= set(German.cities)
    assert {r["city"] for r in records} - set(German.cities)
    assert all(r["job"].strip() for r in records)
    # The product keeps its fields together, as one row of the file
    for r in records:
        row = products[r["favourite"]["sku"]]
        assert r["favourite"] == {
            "sku": row["sku"],
            "title": row["title"],
            "price": float(row["price"]),
            "popularity": int(row["popularity"]),
        }
    # The popularity column weights the draw: 12 of 30 is 0.40. Over about 10,000
    # signups four standard errors are 0.0196
    share = Counter(r["favourite"]["sku"] for r in records)["P-101"] / len(records)
    assert abs(share - 12 / 30) < 0.02


# ---------------------------------------------------------------------------
# Choices and Numbers
# ---------------------------------------------------------------------------


@_snippet("choices_and_numbers")
def test_choices_and_numbers_follow_their_weights_and_ranges():
    records = _records("choices_and_numbers", until=10_000)
    methods = [v["method"] for v in _rows(records, "payment")]
    assert len(methods) == 9_999  # the run ends before the payment at 10,000 s
    # Four standard deviations of a share of 10,000 payments are 0.0196
    assert abs(methods.count("card") / len(methods) - 0.6) < 0.02
    assert set(methods) == {"card", "cash"}
    purchases = _rows(records, "purchase")
    assert len(purchases) > 9_000
    # The price reaches both ends of 1 to 250: about 9,000 draws over a width of 249
    # leave a gap of about 0.03 at each end
    prices = [p["price"] for p in purchases]
    assert min(prices) < 1.5
    assert max(prices) > 249.5
    assert 1 <= min(prices) and max(prices) <= 250
    assert {p["items"] for p in purchases} == {1, 2, 3, 4, 5}
    minutes = [p["delivery_minutes"] for p in purchases]
    assert abs(statistics.mean(minutes) - 120) < 1
    assert abs(statistics.stdev(minutes) - 15) < 1
    channels = Counter(p["channel"] for p in purchases)
    assert set(channels) == {"web", "app", "store"}
    assert all(abs(n / len(purchases) - 1 / 3) < 0.03 for n in channels.values())
    gifts = sum(p["gift"] for p in purchases) / len(purchases)
    assert abs(gifts - 0.1) < 0.015


# ---------------------------------------------------------------------------
# Timestamps and Gaps
# ---------------------------------------------------------------------------


@_snippet("timestamps_and_gaps")
def test_timestamps_and_gaps_are_random_around_0_4_seconds():
    records = _events_of(_records("timestamps_and_gaps", until=2000), "order")
    gaps = [b["sim_ts"] - a["sim_ts"] for a, b in zip(records, records[1:])]
    assert len(gaps) > 4500
    assert abs(statistics.mean(gaps) - 0.4) < 0.004
    assert abs(statistics.stdev(gaps) - 0.03) < 0.003
    first = records[0]
    start = datetime.datetime.fromisoformat(first["value"]["placed_at"])
    offsets = []
    for record in records:
        value = record["value"]
        sent = datetime.datetime.fromisoformat(value["placed_at"])
        # placed_at is the simulation time of the record
        assert (
            abs((sent - start).total_seconds() - (record["sim_ts"] - first["sim_ts"]))
            < 0.001
        )
        # deliver_by is up to 2 days after it
        due = datetime.datetime.fromisoformat(value["deliver_by"])
        offsets.append((due - sent).total_seconds())
        # epoch_ms is the same time as a number of milliseconds
        assert abs(value["epoch_ms"] - sent.timestamp() * 1000) < 1
    # The offset reaches both ends of 0 to 2 days: about 5,000 draws over a width of
    # 172,800 s leave a gap of about 35 s at each end
    assert max(offsets) > 172000
    assert min(offsets) < 800
    assert all(0 <= offset <= 172800 for offset in offsets)
    assert len({r["value"]["deliver_by"] for r in records}) > 4500


def _events_of(records, task):
    return [
        r
        for r in records
        if r["stream_type"] == "event"
        and r["task"] == task
        and "status" not in r["value"]
    ]


# ---------------------------------------------------------------------------
# Stop After a Count
# ---------------------------------------------------------------------------


@_snippet("stop_after_a_count")
def test_stop_after_a_count_ends_the_run_after_12_records():
    context = _context("stop_after_a_count")
    assert context._default_until is None  # the blueprint has no `run`
    records = _events_of(_records("stop_after_a_count", until=1000), "order")
    # A run to 1,000 s still ends at the 12th record, which is at 24 s
    assert [r["value"]["order_id"] for r in records] == list(range(1, 13))
    assert [r["sim_ts"] for r in records] == [2.0 * n for n in range(1, 13)]
    # With no `until`, the run ends on its own
    assert len(_events_of(_records("stop_after_a_count", "builder"), "order")) == 12


# ---------------------------------------------------------------------------
# Many Entities
# ---------------------------------------------------------------------------


@_snippet("many_entities")
def test_many_entities_each_have_their_own_key():
    records = _events_of(_records("many_entities"), "report")
    keys = [f"till-{n}" for n in range(1, 11)]
    assert {r["key"] for r in records} == set(keys)
    assert len(records) == 40  # 10 tills, a report at 60, 120, 180 and 240 s
    for r in records:
        assert r["value"]["till"] == r["key"]
    per_minute = defaultdict(list)
    for r in records:
        per_minute[r["sim_ts"]].append(r["key"])
    assert sorted(per_minute) == [60.0, 120.0, 180.0, 240.0]
    assert all(sorted(k, key=keys.index) == keys for k in per_minute.values())
    # Each till draws its own takings
    assert len({r["value"]["takings"] for r in records}) > 30


@_snippet("many_entities_named")
def test_many_entities_named_start_one_entity_for_each_key():
    records = _events_of(_records("many_entities_named"), "report")
    assert [r["key"] for r in records[:3]] == ["north", "south", "east"]
    assert Counter(r["key"] for r in records) == {"north": 4, "south": 4, "east": 4}
    assert all(r["value"]["warehouse"] == r["key"] for r in records)
    assert all(0 <= r["value"]["orders_waiting"] <= 40 for r in records)


# ---------------------------------------------------------------------------
# Build on the Last Value
# ---------------------------------------------------------------------------


@_snippet("build_on_the_last_value")
def test_build_on_the_last_value_drifts_from_the_previous_price():
    records = _events_of(_records("build_on_the_last_value", until=5000), "price")
    by_product = defaultdict(list)
    for r in records:
        by_product[r["key"]].append(r["value"])
    assert sorted(by_product) == [f"product-{n}" for n in range(5)]
    for prices in by_product.values():
        assert len(prices) == 4999
        # The first price is a step from the default, 20
        assert abs(prices[0]["price"] - 20) <= 1.005
        for before, after in zip(prices, prices[1:]):
            assert abs(after["price"] - before["price"]) <= 1.005
        # A step is a draw with deviation 40, so no step is above 6 deviations,
        # unless the clamp stops it
        for before, after in zip(prices, prices[1:]):
            assert abs(after["stock"] - before["stock"]) < 240
    stock = [v["stock"] for vs in by_product.values() for v in vs]
    assert all(isinstance(h, int) and 0 <= h <= 500 for h in stock)
    # Over 5,000 steps a walk of this size reaches both limits
    assert 0 in stock and 500 in stock
    # The walk wanders: the price range is much wider than one step
    prices = [v["price"] for vs in by_product.values() for v in vs]
    assert max(prices) - min(prices) > 10


# ---------------------------------------------------------------------------
# Skewed Keys
# ---------------------------------------------------------------------------


@_snippet("skewed_keys")
def test_skewed_keys_5_of_25_customers_place_75_percent_of_orders():
    records = _events_of(_records("skewed_keys", until=10_000), "order")
    assert len(records) == 9_999
    customers = Counter(r["value"]["customer_id"] for r in records)
    assert set(customers) == set(range(1, 26))
    busy = sum(customers[c] for c in range(1, 6)) / len(records)
    # Four standard deviations of a share of 10,000 orders are 0.017
    assert abs(busy - 0.75) < 0.02
    # The 5 busy customers are customers 1 to 5, each with about 15 percent
    assert {c for c, _ in customers.most_common(5)} == {1, 2, 3, 4, 5}
    assert all(abs(customers[c] / len(records) - 0.15) < 0.02 for c in range(1, 6))
    # The customer is the record key
    assert all(r["key"] == str(r["value"]["customer_id"]) for r in records)


# ---------------------------------------------------------------------------
# Change State Over Time
# ---------------------------------------------------------------------------

ALLOWED = {
    "running": {"running", "slow"},
    "slow": {"running", "slow", "stopped"},
    "stopped": {"slow"},
}
DWELL = {"running": 300, "slow": 120, "stopped": 240}


@_snippet("change_state_over_time")
def test_change_state_over_time_follows_the_allowed_moves():
    records = _records("change_state_over_time")
    states = defaultdict(list)
    for r in _events(records, "running", "slow", "stopped"):
        states[r["key"]].append((r["sim_ts"], r["value"]["state"]))
    assert sorted(states) == ["station-1", "station-2", "station-3"]
    seen = set()
    for key, moves in states.items():
        assert moves[0] == (DWELL["running"], "running"), key
        for (before, old), (after, new) in zip(moves, moves[1:]):
            assert new in ALLOWED[old], (key, old, new)
            # A record is written when a state ends, so the time between two records
            # is the dwell time of the later state
            assert after - before == DWELL[new], (key, old, new)
        seen.update(state for _, state in moves)
    assert seen == {"running", "slow", "stopped"}
    # A station is never stopped for two states in a row, and never goes from running
    # to stopped
    for moves in states.values():
        names = [state for _, state in moves]
        assert ("running", "stopped") not in zip(names, names[1:])
        assert ("stopped", "stopped") not in zip(names, names[1:])


def _events(records, *tasks):
    return [
        r
        for r in records
        if r["stream_type"] == "event"
        and r["task"] in tasks
        and "status" not in r["value"]
    ]


# ---------------------------------------------------------------------------
# Join Across Sinks
# ---------------------------------------------------------------------------


def _join_files(form, folder, monkeypatch):
    """The customers and the orders that one form writes into its real file sinks under
    `folder`, run as the file runs: the blueprint's own `run`, the Builder's `build()`
    to its `UNTIL`, and the Low-level file's `run()`."""
    monkeypatch.setenv("DEST_PATH", str(folder))
    if form == "yaml":
        context = _context("join_across_sinks")
        context.factor = 0.0
        context.run()
    elif form == "builder":
        module = _module("join_across_sinks", "builder")
        module.build().run(until=module.UNTIL)
    else:
        _module("join_across_sinks", "low_level").run()
    customers = [
        {k: r[k] for k in ("customer_id", "country")}
        for r in _json_rows(folder, "customers")
    ]
    orders = [
        {k: r[k] for k in ("order_id", "customer_id", "total")}
        for r in _json_rows(folder, "orders")
    ]
    return customers, orders


@_snippet("join_across_sinks")
def test_join_across_sinks_orders_name_customers_in_the_other_file(
    tmp_path, monkeypatch
):
    """Run into the real file sinks in every form, each order in orders.jsonl names a
    customer in customers.jsonl, each file holds the records of one task, and the three
    forms write the same files."""
    files = {}
    for form in ("yaml", "builder", "low_level"):
        folder = tmp_path / form
        folder.mkdir()
        files[form] = customers, orders = _join_files(form, folder, monkeypatch)
        assert len(customers) >= 5, form
        assert len(orders) > 30, form
        for name in ("customers", "orders"):
            for row in _json_rows(folder, name):
                assert ("country" in row) == (name == "customers"), (form, name)
                assert ("order_id" in row) == (name == "orders"), (form, name)
        ids = {r["customer_id"] for r in customers}
        assert {r["customer_id"] for r in orders} <= ids, form
        assert len({r["customer_id"] for r in orders}) > 3, form
    assert files["builder"] == files["yaml"]
    assert files["low_level"] == files["yaml"]
    # The orders and customers are the ones of the capture run
    records = _records("join_across_sinks")
    assert [r["order_id"] for r in files["yaml"][1]] == [
        r["order_id"] for r in _rows(records, "order")
    ]


def test_join_across_sinks_an_order_names_a_customer_that_exists_by_then():
    records = _records("join_across_sinks")
    written = {}
    for r in _events(records, "customer"):
        written[r["value"]["customer_id"]] = r["sim_ts"]
    for r in _events(records, "order"):
        assert written[r["value"]["customer_id"]] <= r["sim_ts"]


def test_join_across_sinks_yaml_and_builder_sinks_each_take_one_task(
    tmp_path, monkeypatch
):
    """The two sinks keep their `when`, in the blueprint and in the Builder file. The
    Low-level file's predicates are checked by the files its `run()` writes."""
    monkeypatch.setenv("DEST_PATH", str(tmp_path))
    for form in ("yaml", "builder"):
        context = _context("join_across_sinks", form)
        assert len(context._egress_providers) == 2
        for predicate, task in zip(context._egress_predicates, ("customer", "order")):
            assert predicate({"task": task, "stream_type": "event"})
            other = "order" if task == "customer" else "customer"
            assert not predicate({"task": other, "stream_type": "event"})


# ---------------------------------------------------------------------------
# Change the Schema During a Run
# ---------------------------------------------------------------------------


@_snippet("change_the_schema_during_a_run")
def test_change_the_schema_during_a_run_at_60_120_and_180_seconds():
    data, labels = _run("change_the_schema_during_a_run")
    assert {r["stream_type"] for r in data} == {"event"}  # the labels go elsewhere
    orders = [(r["sim_ts"], r["value"]) for r in data]
    assert [t for t, _ in orders] == [10.0 * n for n in range(1, 24)]
    for t, value in orders:
        if t < 60:
            assert set(value) == {"order_id", "total"}
        elif t < 120:
            assert set(value) == {"order_id", "currency", "total"}
        elif t < 180:
            assert set(value) == {"order_id", "currency", "amount"}
            assert isinstance(value["amount"], float)
        else:
            assert set(value) == {"order_id", "currency", "amount"}
            assert isinstance(value["amount"], str)
            assert 5 <= float(value["amount"]) < 200
        if t >= 60:
            assert value["currency"] == "EUR"
    # The value of an order is the same draw under every name
    assert 5 <= orders[0][1]["total"] < 200
    assert [r["label"] for r in labels] == [
        {"change": "field_added", "field": "currency"},
        {"change": "field_renamed", "field": "total", "to": "amount"},
        {"change": "field_retyped", "field": "amount", "to": "text"},
    ]
    assert [r["sim_ts"] for r in labels] == [60.0, 120.0, 180.0]
    assert {r["stream_type"] for r in labels} == {"label"}


def test_change_the_schema_during_a_run_labels_go_only_to_the_labels_sink(monkeypatch):
    """The labels sink of each form is the second, and only the labels reach it: the
    blueprint's and the Builder's own sinks say `labels`, and the Low-level file's
    `run()` is run with its two console sinks replaced by captures."""
    slug = "change_the_schema_during_a_run"
    for form in ("yaml", "builder"):
        assert _context(slug, form)._egress_labels == [False, True], form
    module = _module(slug, "low_level")
    made = []

    def capture():
        made.append(_Capture())
        return made[-1]

    monkeypatch.setattr(module, "ConsoleEgress", capture)
    module.run()
    data, labels = made
    assert {r["stream_type"] for r in data.records} == {"event"}
    assert [r["stream_type"] for r in labels.records] == ["label"] * 3


# ---------------------------------------------------------------------------
# Look Up Other Records
# ---------------------------------------------------------------------------


@_snippet("look_up_other_records")
def test_look_up_other_records_lookup_upstream_and_aggregate():
    records = _records("look_up_other_records")
    customers = {
        r["value"]["customer_id"]: r["sim_ts"] for r in _events(records, "customer")
    }
    orders = _events(records, "order")
    payments = _events(records, "payment")
    reports = _events(records, "report")
    assert len(customers) >= 5
    assert len(orders) > 30
    # `lookup`: an order names a customer written by then, and not always the same one
    for r in orders:
        assert customers[r["value"]["customer_id"]] <= r["sim_ts"]
    assert len({r["value"]["customer_id"] for r in orders}) > 3
    # `upstream`: each order is followed by a payment of its id and total, for the same
    # entity at the same time
    assert len(payments) == len(orders)
    for order, payment in zip(orders, payments):
        assert (payment["key"], payment["sim_ts"]) == (order["key"], order["sim_ts"])
        assert payment["value"] == {
            "order_id": order["value"]["order_id"],
            "amount": order["value"]["total"],
        }
    # `aggregate`: a report every 10 s gives the mean and the largest total of the last
    # 10 orders written before it
    assert [r["sim_ts"] for r in reports] == [10.0, 20.0, 30.0, 40.0, 50.0]
    for report in reports:
        recent = [o["value"]["total"] for o in orders if o["sim_ts"] < report["sim_ts"]]
        recent = recent[-10:]
        assert len(recent) == 10
        assert report["value"]["mean_total"] == pytest.approx(sum(recent) / 10)
        assert report["value"]["largest_total"] == max(recent)

"""The guides' scripts run, unpaced, and behave as their pages say.

The pages show these files, and test_docs_match_examples.py keeps the copies equal to
them. A script that needs a broker runs here with a capturing sink in place of its
Kafka egress, so no service is needed.
"""

import ast
import asyncio
import importlib.util
import json
import logging
import queue
import shutil
import sys
from pathlib import Path

import pytest

from desflow.connectors.egress.base import BaseEgress

ROOT = Path(__file__).resolve().parents[2]
GUIDES = ROOT / "docs" / "snippets" / "guides"


class _Capture(BaseEgress):
    """Keeps every record it receives, in place of a Kafka egress."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _captured(app, until):
    """Runs a builder app unpaced with its one egress replaced by a capture."""
    capture = _Capture()
    app.factor = 0.0
    app._egress_providers = [capture]
    app.run(until=until)
    return capture.records


def _load(name):
    return _load_from(GUIDES, name)


def _load_from(folder, name):
    sys.path.insert(0, str(folder))
    try:
        spec = importlib.util.spec_from_file_location(name, folder / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(folder))


def _events(caplog):
    """(key, value) of every event the console egress logged, in order."""
    events = []
    for record in caplog.records:
        # One log call per batch, one line per record
        for line in record.getMessage().split("\n"):
            if line.startswith("[EVT] "):
                data = ast.literal_eval(line[len("[EVT] ") :])
                events.append((data["key"], data["value"]))
    return events


def test_breakdowns_downtime_matches_the_events():
    """The station of the breakdowns example fails five times and is down 52.5 s of
    300; eight orders are interrupted and all eight resume, as the page says."""
    module = _load("breakdowns")
    examples = ROOT / "examples"
    outcomes = [
        module.yaml_form(str(examples / "yaml" / "breakdowns.yaml")),
        module.builder_form(str(examples / "builder" / "breakdowns_example.py")),
        module.low_level_form(str(examples / "low-level" / "breakdowns_example.py")),
    ]
    assert outcomes[0] == outcomes[1] == outcomes[2]
    assert outcomes[0] == {
        "periods": [
            (84.6, 98.5),
            (150.1, 157.3),
            (238.9, 253.5),
            (272.7, 282.6),
            (285.0, 291.8),
        ],
        "downtime": 52.5,
        "share_down": 0.175,
        "interrupted": 8,
        "resumed": 8,
        "order_packed": 184,
    }


def test_time_varying_arrivals_boundaries():
    """The gap drawn at the step's time uses the new value; a step at until never
    applies."""
    from desflow import SimulationContext

    for module in (
        _load("time_varying_arrivals"),
        _load("time_varying_arrivals_low_level"),
    ):
        assert module.arrivals(50) == ([10.0, 20.0, 30.0, 35.0, 40.0, 45.0], 5.0)
        assert module.arrivals(30) == ([10.0, 20.0], 10.0)
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "time_varying_arrivals.yaml"
    for until, times in (
        (50, [10.0, 20.0, 30.0, 35.0, 40.0, 45.0]),
        (30, [10.0, 20.0]),
    ):
        events = _event_rows(_captured(SimulationContext.from_yaml(blueprint), until))
        assert [row[0] for row in events] == times


def test_handoff_holds_the_picker_until_the_bench_is_free(caplog):
    module = _load("multi_resource_handoffs")
    module.app.factor = 0.0
    with caplog.at_level(logging.INFO, logger="desflow"):
        module.app.run(until=60)
    events = _events(caplog)
    assert any(value.get("event_type") == "order_packed" for _, value in events)

    # Each order goes through its steps in this order
    def step(value):
        if "status" in value:
            return f"{value['path_id'].rsplit('.', 1)[1]} {value['status']}"
        return value["event_type"]

    order = [
        "walking queued",
        "walking started",
        "order_picked",
        "packing queued",
        "packing started",
        "order_packed",
    ]
    seen: dict = {}
    for key, value in events:
        seen.setdefault(key, []).append(step(value))
    for key, steps in seen.items():
        assert steps == order[: len(steps)], key

    # One order at a time holds the picker, from its pick's start until its pack
    # starts, and one at a time is on the bench, from its pack's start until it is
    # packed
    with_picker, on_bench = None, None
    for key, value in events:
        current = step(value)
        if current == "walking started":
            assert with_picker is None, f"{key} picked while {with_picker} held it"
            with_picker = key
        elif current == "packing started":
            assert with_picker == key and on_bench is None
            with_picker, on_bench = None, key
        elif current == "order_packed":
            assert on_bench == key
            on_bench = None


def _event_rows(records):
    """(sim_ts, key, task, instance, value) of every event, in order."""
    return [
        (r["sim_ts"], r["key"], r["task"], r["instance"], r["value"])
        for r in records
        if r.get("stream_type") == "event"
    ]


def test_handoff_forms_publish_the_same_events():
    """The YAML, builder and low-level handoffs publish the same events at the same
    times, with seed 42."""
    from desflow import SimulationContext

    blueprint = ROOT / "docs" / "snippets" / "yaml" / "multi_resource_handoffs.yaml"
    yaml_events = _event_rows(_captured(SimulationContext.from_yaml(blueprint), 30))
    builder_events = _event_rows(_captured(_load("multi_resource_handoffs").app, 30))
    capture = _Capture()
    _load("multi_resource_handoffs_low_level").run(30, 0.0, [capture])
    low_level_events = _event_rows(capture.records)
    assert builder_events and any(
        row[4].get("event_type") == "order_packed" for row in builder_events
    )
    assert yaml_events == builder_events == low_level_events


def test_supervisor_adds_the_wrappers_mid_run(caplog):
    module = _load("dynamic_topology")
    with caplog.at_level(logging.INFO, logger="desflow"):
        resources = module.run(until=20, factor=0.0)
    assert set(resources) == {"packers", "wrappers"}
    assert resources["wrappers"].capacity == 2

    events = _events(caplog)
    added = [value for key, value in events if key == "supervisor"]
    assert added == [{"status": "wrappers_added", "capacity": 2}]
    started = [value["resource"] for _, value in events if value["status"] == "started"]
    assert "packers" in started and "wrappers" in started


def test_supervisor_adds_the_wrappers_in_every_form(tmp_path, monkeypatch, caplog):
    """The blueprint and the builder run the same supervisor as a process, and publish
    the events the low-level script publishes."""
    from desflow import SimulationContext

    # A reader copies the page's files into one folder and runs them there
    for source in (
        GUIDES / "dynamic_topology_builder.py",
        ROOT / "docs" / "snippets" / "yaml" / "dynamic_topology.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "dynamic_topology_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    with caplog.at_level(logging.INFO, logger="desflow"):
        _load("dynamic_topology").run(until=20, factor=0.0)
    low_level = _events(caplog)
    yaml_app = SimulationContext.from_yaml(tmp_path / "dynamic_topology.yaml")
    builder_app = _load_from(tmp_path, "dynamic_topology_builder").app
    for app in (yaml_app, builder_app):
        events = [(r[1], r[4]) for r in _event_rows(_captured(app, 20))]
        assert events == low_level
    assert ("supervisor", {"status": "wrappers_added", "capacity": 2}) in low_level


def test_router_sends_each_record_to_its_topic(tmp_path, monkeypatch):
    """The router picks each record's topic; the three forms send the same events, to
    all three topics, and only failed orders to the alarms."""
    from desflow import SimulationContext

    # A reader copies the page's files into one folder and runs them there
    for source in (
        GUIDES / "complex_routing.py",
        GUIDES / "complex_routing_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "complex_routing.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "complex_routing_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    logic = _load_from(tmp_path, "complex_routing_logic")
    route = logic.custom_kafka_router
    telemetry = {"stream_type": "telemetry", "path_id": "Shop.packers.in_use"}
    assert route({**telemetry, "value": 1}) == "shop-telemetry"
    event = {"stream_type": "event", "key": "orders-1"}
    assert route({**event, "value": {"status": "packing_failed"}}) == "system-alarms"
    assert route({**event, "value": {"level": "ERROR"}}) == "system-alarms"
    assert route({**event, "value": {"status": "finished"}}) == "shop-events"
    assert route({**event, "value": "a plain string"}) == "shop-events"

    # The blueprint creates its topics in run.before; here there is no broker
    import complex_routing_logic

    monkeypatch.setattr(complex_routing_logic, "create_topics", lambda: None)
    yaml_records = _captured(
        SimulationContext.from_yaml(tmp_path / "complex_routing.yaml"), 120
    )
    builder_records = _captured(_load_from(tmp_path, "complex_routing").app, 120)
    capture = _Capture()
    _load_from(tmp_path, "complex_routing_low_level").run(120, 0.0, [capture])
    low_level_records = capture.records
    for records in (yaml_records, builder_records, low_level_records):
        assert {route(r) for r in records} == set(logic.TOPICS)
        alarms = [r for r in records if route(r) == "system-alarms"]
        assert alarms and all(r["instance"] % 10 == 9 for r in alarms)
    assert (
        _event_rows(yaml_records)
        == _event_rows(builder_records)
        == _event_rows(low_level_records)
    )


def test_secure_egress_passes_its_settings_to_the_client(monkeypatch):
    """The SASL settings reach the client in the builder and the blueprint, and the
    three forms publish the same events."""
    from desflow import SimulationContext

    for name in ("KAFKA_BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET"):
        monkeypatch.delenv(name, raising=False)
    module = _load("kafka_security")
    blueprint = SimulationContext.from_yaml(
        ROOT / "docs" / "snippets" / "yaml" / "kafka_security.yaml"
    )
    for egress in (module.egress, blueprint._egress_providers[0]):
        config = egress.producer_config
        assert config["security_protocol"] == "SASL_SSL"
        assert config["sasl_plain_username"] == "<YOUR_API_KEY>"
        assert egress.topic_router is None  # so it creates its own topics

    records = _captured(module.app, until=30)
    assert any(r.get("value") == {"event_type": "order_packed"} for r in records)
    sys.path.insert(0, str(GUIDES))  # the low-level script imports the egress
    try:
        low_level = _load("kafka_security_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    capture = _Capture()
    env = low_level.shop([capture], 0.0)
    env.run(until=30)
    env.teardown()
    assert (
        _event_rows(_captured(blueprint, until=30))
        == _event_rows(records)
        == _event_rows(capture.records)
    )


def test_predictions_fit_the_avro_schema(tmp_path, monkeypatch):
    """The generated schema describes the record KafkaEgress sends, and the
    predictions of the builder, YAML and low-level forms fit it."""
    fastavro = pytest.importorskip("fastavro")
    from desflow import SimulationContext

    module = _load("avro_and_pydantic")
    schema = json.loads(module.schema_string)
    assert [f["name"] for f in schema["fields"]] == [
        "key",
        "task",
        "instance",
        "sim_ts",
        "timestamp",
        "value",
    ]

    # A reader puts the files in one folder; the blueprint's !python tags import
    # from it, and here there is no broker to create the topics on
    for source in (
        GUIDES / "avro_and_pydantic.py",
        GUIDES / "avro_and_pydantic_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "avro_and_pydantic.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "avro_and_pydantic_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    import avro_and_pydantic_logic

    monkeypatch.setattr(avro_and_pydantic_logic, "create_topics", lambda: None)
    yaml_records = _captured(
        SimulationContext.from_yaml(tmp_path / "avro_and_pydantic.yaml"), 30
    )
    capture = _Capture()
    env = _load_from(tmp_path, "avro_and_pydantic_low_level").shop([capture], 0.0)
    env.run(until=30)
    env.teardown()

    route = module.ml_topic_router
    parsed = fastavro.parse_schema(schema)
    for records in (_captured(module.app, until=30), yaml_records, capture.records):
        predictions = [r for r in records if route(r) == "ml-predictions"]
        assert predictions
        assert {"ml-predictions", "sim-events"} <= {route(r) for r in records}
        for record in predictions:
            sent = {k: v for k, v in record.items() if k != "stream_type"}
            assert fastavro.validate(sent, parsed)


def test_secured_registry_serializer_sends_its_credentials(registry_transport):
    """The Registry Connection section builds a serializer whose requests carry `auth`."""
    httpx = pytest.importorskip("httpx")
    sent = []

    def registry(request):
        sent.append(request)
        return httpx.Response(200, json={"id": 1})

    registry_transport(registry)
    module = _load("avro_and_pydantic")

    payload = module.secured_serializer.serialize(
        "ml-predictions",
        {
            "key": "o1",
            "task": None,
            "instance": None,
            "sim_ts": 1.0,
            "timestamp": "2026-01-01T00:00:00Z",
            "value": {"event_type": "prediction", "task_id": "o1", "confidence": 0.9},
        },
    )

    assert sent[0].url.host == "registry.example.com"
    assert sent[0].headers["Authorization"].startswith("Basic ")
    assert payload[:5] == b"\x00\x00\x00\x00\x01"


def test_reneging_and_balking_ties_and_balance():
    """At a tie, the order scheduled later is served and the one scheduled first
    reneges, as the page says, in every form; and every order of the patience
    example is packed, reneged, balked or still in the shop, in every form."""
    module = _load("reneging_and_balking")
    blueprint = str(ROOT / "docs" / "snippets" / "yaml" / "reneging_tie.yaml")
    for tie in (
        module.tie,
        lambda at, patience: module.tie_yaml(at, patience, blueprint),
        module.tie_low_level,
    ):
        assert tie(3, 3) == "served"
        assert tie(0, 6) == "reneged"
    examples = ROOT / "examples"
    for outcome in (
        module.yaml_form(str(examples / "yaml" / "patience.yaml")),
        module.builder_form(str(examples / "builder" / "patience_example.py")),
        module.low_level_form(str(examples / "low-level" / "patience_example.py")),
    ):
        assert outcome == {
            "order_packed": 49,
            "balked": 17,
            "reneged": 7,
            "started": 1,
        }


def test_wait_for_one_or_all_first_ties_and_failure():
    """The first branch wins; two signals at one instant are both done; a failed
    branch of an all fails the order at once while the child flow runs on. The
    builder, YAML and low-level forms agree."""
    module = _load("wait_for_one_or_all")
    low_level = _load("wait_for_one_or_all_low_level")
    yaml = ROOT / "docs" / "snippets" / "yaml"
    forms = [
        (module.first, module.two_at_once, module.all_with_a_failure),
        (
            lambda gap: module.first_yaml(gap, yaml / "wait_first.yaml"),
            lambda: module.two_at_once_yaml(yaml / "wait_tie.yaml"),
            lambda: module.all_with_a_failure_yaml(yaml / "wait_all.yaml"),
        ),
        (low_level.first, low_level.two_at_once, low_level.all_with_a_failure),
    ]
    for first, two_at_once, all_with_a_failure in forms:
        assert first(10) == {"stock": True, "limit": False}
        assert first(50) == {"stock": False, "limit": True}
        assert two_at_once() == {"boxes": True, "labels": True}
        assert all_with_a_failure() == [
            (0.0, "order", "queued"),
            (2.0, "order", "failed"),
            (4.0, "inspect", "finished"),
        ]


def test_wait_for_one_or_all_example_packs_and_refunds():
    """The page's example: each restock waits for all of its delivery (10 s) and its
    stock count (20 s), so it comes 20 s after each delivery starts, at 80 s to 560 s;
    27 orders are packed after a restock and 35 are refunded at the limit, in every
    form."""
    expected = {
        "pack": 27,
        "refund": 35,
        "restocked_at": [80.0 + 60 * n for n in range(9)],
    }
    assert _load("wait_for_one_or_all_example").outcome() == expected
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "wait_for_one_or_all_example.yaml"
    assert _load("wait_for_one_or_all").example_yaml(str(blueprint)) == expected
    assert _load("wait_for_one_or_all_example_low_level").outcome() == expected


def test_performance_tuning_counts_the_records():
    """An hour of the shop publishes 108,688 records at factor 0: no lag readings,
    which would add 3,600, and a rate above 0."""
    module = _load("performance_tuning")
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "performance_tuning.yaml"
    sys.path.insert(0, str(GUIDES))  # the low-level script imports Count from it
    try:
        low_level = _load("performance_tuning_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    for outcome in (
        module.blueprint(str(blueprint)),
        module.throughput(module.shop(), until=3600),
        low_level.throughput(until=3600),
    ):
        assert outcome["records"] == 108_688
        assert outcome["per_second"] > 0


def test_fit_a_distribution_gaps(tmp_path, capsys):
    """The exponential fits the 299 gaps best, at 0.364 a second; 5 zero gaps skip
    gamma, weibull and lognormal; the file it writes is the one the blueprint
    includes; and one packer packs 1,302 orders in the hour, in every form."""
    pytest.importorskip("scipy")
    from desflow import SimulationContext

    module = _load("fit_a_distribution")
    result = module.fit()
    capsys.readouterr()
    assert result.best == "exponential" and result.settings()["rate"] == 0.36388
    statuses = {row["family"]: (row["status"], row["reason"]) for row in result.table}
    for family in ("gamma", "weibull", "lognormal"):
        assert statuses[family] == (
            "skipped",
            f"{family} needs every value above 0; 5 are not",
        )
    assert statuses["truncated_normal"][0] == "did not fit"
    assert statuses["uniform"][0] == statuses["triangular"][0] == "rejected"
    histogram = module.histogram().settings()
    assert len(histogram["counts"]) == 10 and sum(histogram["counts"]) == 299
    written = module.write(result, tmp_path)
    blueprint = ROOT / "docs" / "snippets" / "yaml"
    assert written.read_text() == (blueprint / "fitted" / "orders.yaml").read_text()
    app = module.shop(result)
    app.run(until="1 h")
    yaml_app = SimulationContext.from_yaml(blueprint / "fit_a_distribution.yaml")
    yaml_app.run()
    for run in (app, yaml_app):
        assert run.env.stats.readings()["Shop.stats.flows.orders.finished"] == 1302
    sys.path.insert(0, str(GUIDES))  # the low-level script imports fit from it
    try:
        low_level = _load("fit_a_distribution_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    assert low_level.run(result) == 1302


def test_test_a_model_tests_pass_and_the_saved_results_are_current(
    tmp_path, monkeypatch
):
    """The three kinds of test pass; the waits fall from 2.055 s with one packer to
    0.111 s with two and 0.016 s with three; saving the results writes the file the
    back test reads; and the YAML and low-level forms give the same results."""
    module = _load("test_a_model")
    module.test_more_packers_shorten_the_wait()
    module.test_results_match_the_saved_run()
    for packers, wait in ((1, 2.055375), (2, 0.110574), (3, 0.016206)):
        assert module.results(packers=packers)["wait_mean"] == wait
    assert module.results() == {"finished": 319, "wait_mean": 0.110574}
    for settings in (
        {"dist": "exponential", "rate": 0},
        {"dist": "constant", "value": -1},
    ):
        module.test_a_distribution_that_cannot_happen_is_refused(settings)
    monkeypatch.setattr(module, "SAVED", tmp_path / "saved.json")
    module.save_results()
    assert (tmp_path / "saved.json").read_text() == (
        GUIDES / "test_a_model_saved.json"
    ).read_text()
    # The YAML and low-level files run the same tests against the same saved results
    for source in (
        GUIDES / "test_a_model_yaml.py",
        GUIDES / "test_a_model_low_level.py",
        GUIDES / "test_a_model_saved.json",
        ROOT / "docs" / "snippets" / "yaml" / "test_a_model.yaml",
    ):
        shutil.copy(source, tmp_path)
    for form in (
        _load_from(tmp_path, "test_a_model_yaml"),
        _load_from(tmp_path, "test_a_model_low_level"),
    ):
        form.test_more_packers_shorten_the_wait()
        form.test_results_match_the_saved_run()
        for packers, wait in ((1, 2.055375), (2, 0.110574), (3, 0.016206)):
            assert form.results(packers=packers)["wait_mean"] == wait


def test_analyse_the_event_log_waits_and_throughput(tmp_path):
    """Two minutes of one packer write 191 event rows, which become 318 log rows for
    64 orders; 63 leave the shop, the mean wait is 1.250 s and 31.5 orders leave each
    minute; every start is the later of the arrival and the last start plus 1.5 s;
    and the YAML and low-level forms write the same events."""
    module = _load("analyse_the_event_log")
    module.run(tmp_path)
    events = module.read_events(tmp_path)
    assert events.num_rows == 191
    statuses = [row for row in events["status"].to_pylist()]
    assert [statuses.count(s) for s in ("queued", "started", "finished")] == [
        64,
        64,
        63,
    ]
    log = module.event_log(events)
    assert log.column_names == ["entity_id", "event_type", "event", "time", "run"]
    assert log.num_rows == 318 and set(log["run"].to_pylist()) == {1}
    per_order = module.waits_and_times(log)
    assert len(per_order) == 64 and len(module.times(log, "depart")) == 63
    assert per_order["orders-1"] == {"wait": 0.0, "in_shop": 1.5}
    assert max(row["wait"] for row in per_order.values()) == 3.66
    assert round(module.mean_wait(per_order), 3) == 1.25
    assert module.throughput_per_minute(log, module.RUN_SECONDS) == 31.5
    arrived = module.times(log, "arrival")
    started = module.times(log, "packing_start")
    last = None
    for order in sorted(started, key=started.get):
        expected = arrived[order] if last is None else max(arrived[order], last + 1.5)
        assert started[order] == pytest.approx(expected)
        last = started[order]
    # The log is sorted by time
    times = log["time"].to_pylist()
    assert times == sorted(times)
    # The YAML and low-level forms write the same events
    sys.path.insert(0, str(GUIDES))  # the low-level script imports from the module
    try:
        low_level = _load("analyse_the_event_log_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "analyse_the_event_log.yaml"
    module.run(tmp_path / "builder")
    module.run_yaml(tmp_path / "yaml", str(blueprint))
    low_level.run(tmp_path / "low_level")

    def rows(out):
        return sorted(
            (row["sim_ts"], row["key"], row["task"], row["status"])
            for row in module.read_events(out).to_pylist()
        )

    builder = rows(tmp_path / "builder")
    assert len(builder) == 191
    assert rows(tmp_path / "yaml") == rows(tmp_path / "low_level") == builder


def test_animate_a_run_frames_and_entities(tmp_path):
    """The animation of the two-minute run has 61 frames, one for every 2 seconds
    from 0 to 120, and draws the 64 orders of Analyse the Event Log, in every form."""
    import vidigi  # noqa: F401  (a missing dev dependency fails the test)

    module = _load("animate_a_run")
    figure = module.make(tmp_path / "builder")
    assert len(figure.frames) == 61
    assert [frame.name for frame in figure.frames][:3] == ["0", "2", "4"]
    assert figure.frames[-1].name == "120"
    assert module.vidigi_log(tmp_path / "builder")["entity_id"].nunique() == 64
    # The YAML and low-level runs give the same animation
    analyse = _load("analyse_the_event_log")
    sys.path.insert(0, str(GUIDES))  # the low-level script imports from analyse
    try:
        low_level = _load("analyse_the_event_log_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    blueprint = str(ROOT / "docs" / "snippets" / "yaml" / "analyse_the_event_log.yaml")
    for out, run_shop in (
        (tmp_path / "yaml", lambda out: analyse.run_yaml(out, blueprint)),
        (tmp_path / "low_level", low_level.run),
    ):
        other = module.make(out, run_shop)
        assert [frame.name for frame in other.frames] == [
            frame.name for frame in figure.frames
        ]
        assert module.vidigi_log(out).equals(module.vidigi_log(tmp_path / "builder"))


def test_share_by_weight_grants_by_finish_tag_in_every_form(tmp_path, monkeypatch):
    """Nine orders arrive at time 0: tier 2, six of tier 1, two of tier 2. Tier 1
    weighs 3 and tier 2 weighs 1, and the first order takes the packer, so the
    finish tags are worked out by hand here.

    The first order of tier 2 is granted at once with tag 1, and the queue's clock
    becomes 1. Tier 1 then gets 4/3, 5/3, 2, 7/3, 8/3 and 3 (1/3 more each time);
    tier 2 gets 2 and 3 (1 more each time). The smallest tag goes first, and at the
    tags 2 and 3 the order that arrived first goes first, which is tier 1. So the
    tags are granted 4/3, 5/3, 2 (tier 1), 2 (tier 2), 7/3, 8/3, 3 (tier 1), 3 (tier 2),
    and the tiers in grant order, including the first order, are 2, 1, 1, 1, 2, 1, 1, 1, 2.
    First in, first out would give
    2, 1, 1, 1, 1, 1, 1, 2, 2.
    """
    # A reader copies the page's files into one folder and runs the script there
    for source in (
        GUIDES / "share_by_weight.py",
        GUIDES / "share_by_weight_low_level.py",
        *(ROOT / "docs" / "snippets" / "yaml").glob("share_by_weight*"),
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    module = _load_from(tmp_path, "share_by_weight")
    low_level = _load_from(tmp_path, "share_by_weight_low_level")
    by_hand = [2, 1, 1, 1, 2, 1, 1, 1, 2]
    fifo = [2, 1, 1, 1, 1, 1, 1, 2, 2]
    assert module.TIERS == fifo  # the orders arrive in the first in, first out order
    assert by_hand != fifo
    for form in (module.yaml_form, module.builder_form, low_level.low_level_form):
        assert form() == by_hand


def test_editor_settings_hold_the_schema_and_the_three_tags():
    """Set Up Your Editor shows one settings object: the schema for a folder of
    blueprints and the three tags a blueprint uses."""
    settings = json.loads((ROOT / "docs/snippets/editor/settings.json").read_text())
    assert settings["yaml.customTags"] == [
        "!python scalar:string",
        "!include scalar:object",
        "!include mapping:object",
    ]
    assert list(settings["yaml.schemas"].values()) == ["blueprints/*.yaml"]


def test_check_against_theory_agrees_with_the_formulas(capsys, monkeypatch):
    """The wait by hand is 3 s for one packer and 1.286 s for two; explain gives the
    same values; 10 runs of the blueprint agree on every measure; and two packers
    with constant packing have no wait estimate, with D3's reason."""
    from desflow import SimulationContext

    module = _load("check_against_theory")
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "check_against_theory.yaml"
    assert module.BLUEPRINT == blueprint
    assert round(module.by_hand(1, 0.75), 3) == 3.0
    assert round(module.by_hand(2, 1.5), 3) == 1.286
    packers = SimulationContext.from_yaml(blueprint).analyze().resources[0]
    assert packers.wait == pytest.approx(module.by_hand(1, 0.75), abs=1e-9)
    with monkeypatch.context() as patch:
        patch.setenv("PACKERS", "2")
        patch.setenv("RATE", "1.5")
        two = SimulationContext.from_yaml(blueprint).analyze().resources[0]
    assert two.wait == pytest.approx(module.by_hand(2, 1.5), abs=1e-9)
    explanation = module.verify()
    assert [c.differs for c in explanation.verification.comparisons] == [False] * 4
    shop = _load("check_against_theory_shop").app.analyze().resources[0]
    assert shop.wait == pytest.approx(module.by_hand(1, 0.75), abs=1e-9)
    # The builder file is the same model, so its runs give the same measures
    builder = module.verify_builder()
    assert [
        (c.measure, c.measured, c.half_width) for c in builder.verification.comparisons
    ] == [
        (c.measure, c.measured, c.half_width)
        for c in explanation.verification.comparisons
    ]
    reason = "no closed formula for c > 1 with non-exponential times; run --verify to measure it"
    unsupported = module.unsupported()
    assert (unsupported.utilization, unsupported.wait) == (0.75, None)
    assert unsupported.wait_reason == reason
    constant = (
        ROOT / "docs" / "snippets" / "yaml" / "check_against_theory_constant.yaml"
    )
    from_yaml = SimulationContext.from_yaml(constant).analyze().resources[0]
    assert (from_yaml.utilization, from_yaml.wait, from_yaml.wait_reason) == (
        0.75,
        None,
        reason,
    )
    capsys.readouterr()


def test_write_a_connector_applies_the_change_and_writes_the_events(
    tmp_path, monkeypatch
):
    """Nineteen orders arrive, one a second from 1 s, and each takes 1.5 s, so the
    18 that arrive by 18 s are packed by 20 s. The file's change gives the packers 2
    units; the ingress applies it within the first moments of the run, so from then
    on each order starts when it arrives. The three forms write the same events."""
    # A reader copies the page's files into one folder and runs the script there
    for source in (
        GUIDES / "write_a_connector.py",
        GUIDES / "write_a_connector_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "write_a_connector.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "write_a_connector_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    module = _load_from(tmp_path, "write_a_connector")
    low_level = _load_from(tmp_path, "write_a_connector_low_level")
    written = []
    for form in (module.yaml_form, module.builder_form, low_level.low_level_form):
        capacity, rows = form()
        written.append(rows)
        assert capacity == 2
        statuses = [row["status"] for row in rows if row["status"]]
        assert statuses.count("queued") == statuses.count("started") == 19
        packed = [row for row in rows if row["event_type"] == "order_packed"]
        assert len(packed) == 18
        last = [row for row in rows if row["key"] == "orders-19"]
        assert [(row["sim_ts"], row["status"]) for row in last] == [
            ("19.0", "queued"),
            ("19.0", "started"),
        ]
    assert written[0] == written[1] == written[2]

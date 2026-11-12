"""read_trace: the sources, units and checks a replayed arrival relies on."""

from datetime import datetime

import pytest

from desflow import SimulationContext, read_trace


def test_parquet_and_timestamps_map_from_the_logical_start(tmp_path):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    start = datetime(2026, 10, 18, 9, 0, 0)
    path = tmp_path / "orders.parquet"
    pq.write_table(
        pa.table(
            {
                "at": [
                    datetime(2026, 10, 18, 9, 0, 30),
                    datetime(2026, 10, 18, 9, 2, 0),
                ],
                "order": ["o1", "o2"],
            }
        ),
        path,
    )
    rows = read_trace(path, time="at", unit="timestamp", fields=["order"], start=start)
    assert [(row.time, row.fields) for row in rows] == [
        (30.0, {"order": "o1"}),
        (120.0, {"order": "o2"}),
    ]


def test_units_and_the_initial_time():
    rows = read_trace([{"time": 1.5}, {"time": 2}], unit="min", initial_time=100)
    assert [row.time for row in rows] == [190.0, 220.0]


def test_rows_before_the_start_and_unknown_settings_are_refused():
    start = datetime(2026, 10, 18, 9, 0, 0)
    with pytest.raises(ValueError, match="remove the row, or start the run earlier"):
        read_trace([{"time": "2026-10-18T08:59:00"}], unit="timestamp", start=start)
    with pytest.raises(ValueError, match="unit must be one of"):
        read_trace([], unit="days")
    with pytest.raises(ValueError, match="no field sku; add it to the row, or leave"):
        read_trace([{"time": 1}], fields=["sku"])
    with pytest.raises(ValueError, match="needs the run's logical_start_time"):
        read_trace([], unit="timestamp")


def test_fields_as_one_string_other_than_all_is_refused_in_every_api(tmp_path):
    """`fields="id"` would be read one letter at a time, so the YAML, the Builder and
    `read_trace` each refuse it with the words of the blueprint loader."""
    from desflow import SimulationContext
    from desflow.blueprint.loader import BlueprintError

    (tmp_path / "rows.csv").write_text("time,id\n1,7\n")
    path = tmp_path / "model.yaml"
    path.write_text(
        "simulation: {sim_id: R, factor: 0}\n"
        "arrivals:\n"
        "  a: {replay: {path: rows.csv, fields: id}, spawn: work}\n"
        "tasks:\n"
        "  work: {service_time: 1, payload: {event_type: done}}\n"
    )
    refusals = []
    with pytest.raises(BlueprintError) as refused:
        SimulationContext.from_yaml(path)
    refusals.append(str(refused.value))
    with pytest.raises(ValueError) as refused:
        SimulationContext("R", factor=0).add_arrival_replay(
            "a", [{"time": 1, "id": 7}], fields="id"
        )
    refusals.append(str(refused.value))
    with pytest.raises(ValueError) as refused:
        read_trace([{"time": 1, "id": 7}], fields="id")
    refusals.append(str(refused.value))
    for text in refusals:
        assert "Input should be 'all'" in text, text
        assert "Input should be a valid list" in text, text
    rows = read_trace([{"time": 1, "id": 7, "i": 1}], fields=["id"])
    assert rows[0].fields == {"id": 7}


def test_a_replayed_timestamp_arrives_when_records_carry_it():
    """With an initial time of 100, a row stamped 110 s after logical_start_time
    arrives at simulation time 110, and its record carries the row's time."""
    from datetime import datetime, timezone

    from desflow import SimulationContext

    start = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
    records = []

    class Keep:
        async def run(self, egress_queue):
            import asyncio
            import queue

            while True:
                try:
                    records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)

    app = SimulationContext(
        "Shop", factor=0, logical_start_time=start, initial_time=100
    ).add_egress(Keep())
    app.add_arrival_replay(
        "orders", [{"at": "2026-10-01T09:01:50+00:00"}], time="at", unit="timestamp"
    )

    @app.task()
    def take(task_id):
        return {"event": "arrived"}

    @app.arrival_loop("orders")
    def loop(context):
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(take(context.next_instance("take"), entity=entity))

    app.run(until=200)
    [record] = [r for r in records if r["stream_type"] == "event"]
    assert record["sim_ts"] == 110.0
    assert record["timestamp"] == "2026-10-01T09:01:50.000+00:00"


def test_replay_refuses_unsorted_and_invalid_rows():
    """A row out of time order is refused, naming it; so is a row with no valid
    time when on_invalid is error."""

    from desflow import read_trace
    from tests.conformance.harness import BLUEPRINTS, run_yaml
    from tests.conformance.test_s3_replay import ORDERS, _builder

    with pytest.raises(ValueError, match="row 3 is at 5 s, before the row above it"):
        read_trace(BLUEPRINTS / "s3_unsorted.csv")
    with pytest.raises(ValueError, match="row 3 is at 5 s"):
        _builder(BLUEPRINTS / "s3_unsorted.csv")
    with pytest.raises(ValueError, match="row 3 is at 5 s"):
        run_yaml("s3_unsorted.yaml")
    with pytest.raises(ValueError, match="row 4 has no valid time: 'bad'"):
        read_trace(ORDERS)


def test_replay_stops_at_the_end_of_the_input(caplog):
    """After its last row the arrival stops, with one INFO line naming the time."""
    import logging

    from tests.conformance.test_s3_replay import _arrived, _builder

    rows = [
        {"time": 1, "order": "o1", "sku": "tea"},
        {"time": 3, "order": "o2", "sku": "mug"},
    ]
    with caplog.at_level(logging.INFO, logger="desflow.builder.context"):
        records = _builder(rows)
    assert _arrived(records) == [(1.0, "o1", "tea"), (3.0, "o2", "mug")]
    lines = [
        r.getMessage()
        for r in caplog.records
        if "replayed its last row" in r.getMessage()
    ]
    assert lines == ["Arrival 'orders' replayed its last row at 3 s; it stops."]


@pytest.mark.parametrize("times", [[0, 0, 3], [1, 1, 3]])
def test_replayed_rows_dropped_while_the_source_is_off_draw_no_attributes(times):
    """Rows that start nobody draw nothing, whether they are due at the step in
    progress or later: the entity that starts at 3 gets the first number."""
    from desflow import SimulationContext
    from desflow.values import generator

    app = SimulationContext("S", factor=0, random_seed=1)
    got = []
    app.add_arrival_replay(
        "a",
        [{"time": time} for time in times],
        spawn="t",
        active=False,
        attributes={"n": generator({"gen": "sequence", "start": 1})},
    )

    def turn_on(context):
        yield context.env.timeout(2)
        context._run.arrivals.set_arrivals(["a"], True)

    app.add_process(turn_on)
    app.task(service_time=0, name="t")(
        lambda task_id, entity: got.append(entity.attributes["n"]) or {}
    )
    app.run(until=10)
    assert got == [1]


_REPLAY_MODEL = (
    "simulation: {sim_id: R, factor: 0}\n"
    "arrivals:\n"
    "  a: {replay: {path: rows.csv}, spawn: work}\n"
    "tasks:\n"
    "  work: {service_time: 1, payload: {event_type: done}}\n"
)


def _replay_builder(path):
    """A builder model whose one arrival replays `path`."""
    app = SimulationContext("R", factor=0)
    app.add_arrival_replay("a", path)

    @app.task()
    def work(task_id, entity):
        return {"event": "arrived"}

    @app.arrival_loop("a")
    def arrivals(context):
        while True:
            entity = yield context.wait_for_arrival("a")
            context.spawn(work(context.next_instance("work"), entity=entity))

    return app


def test_a_csv_with_a_byte_order_mark_keeps_its_time_column(tmp_path):
    """Excel's "CSV UTF-8" starts the file with a byte order mark, which is not part
    of the first header, in `read_trace`, the Builder and YAML alike."""
    csv_path = tmp_path / "rows.csv"
    csv_path.write_bytes("time,sku\r\n1.5,a\r\n2,b\r\n".encode("utf-8-sig"))
    (tmp_path / "model.yaml").write_text(_REPLAY_MODEL)

    assert [(row.time, row.fields) for row in read_trace(csv_path)] == [
        (1.5, {"time": 1.5, "sku": "a"}),
        (2.0, {"time": 2, "sku": "b"}),
    ]
    assert len(read_trace(csv_path, on_invalid="skip")) == 2
    _replay_builder(csv_path).run(until=3)
    SimulationContext.from_yaml(tmp_path / "model.yaml").run(until=3)


def test_a_csv_row_of_another_length_or_a_repeated_header_is_refused(tmp_path):
    """A row with more or fewer values than the header, and a header that names a
    column twice, are refused naming the row or the column, in every API."""
    csv_path = tmp_path / "rows.csv"
    (tmp_path / "model.yaml").write_text(_REPLAY_MODEL)

    def refusals():
        found = []
        for run in (
            lambda: read_trace(csv_path, fields=["time", "qty"]),
            lambda: _replay_builder(csv_path).run(until=3),
            lambda: SimulationContext.from_yaml(tmp_path / "model.yaml").run(until=3),
        ):
            with pytest.raises(ValueError) as refused:
                run()
            found.append(str(refused.value))
        return found

    csv_path.write_text("time,sku,qty\n1,a,2\n2,b,3,EXTRA\n")
    assert all("row 2 has 4 values, but the header has 3" in m for m in refusals())
    csv_path.write_text("time,sku,qty\n1,a,2\n2,b\n")
    assert all("row 2 has 2 values, but the header has 3" in m for m in refusals())
    csv_path.write_text("time,sku,sku\n1,a,b\n")
    assert all("names column sku more than once" in m for m in refusals())
    from_trace, from_builder, from_yaml = refusals()
    assert "arrival" not in from_trace
    assert "arrival 'a' replays" in from_builder and "rows.csv" in from_builder
    assert from_yaml.startswith(
        f"{tmp_path / 'model.yaml'}:3: arrivals.a.replay.path: rows.csv: "
    )


def test_a_refusal_of_rows_given_in_memory_does_not_print_them():
    rows = [{"time": i} for i in range(200)] + [{"time": "late"}]
    with pytest.raises(ValueError) as refused:
        _replay_builder(rows).run(until=3)
    assert str(refused.value) == (
        "arrival 'a' replays its rows: row 201 has no valid time: 'late'"
    )

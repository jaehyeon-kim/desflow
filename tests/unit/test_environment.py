import asyncio
import enum
import logging
import queue
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from datetime import time as dt_time
from decimal import Decimal

import numpy as np
import orjson
import pytest
import simpy
from pydantic import BaseModel, Field, ValidationError
from pydantic_core import PydanticSerializationError

from desflow.connectors.egress.base import BaseEgress
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.models.schemas import EventPayload, LabelPayload, TelemetryPayload


class TrackingEgress(BaseEgress):
    """A mock egress provider that consumes the queue and tracks batches for testing."""

    def __init__(self):
        self.received_batches = []

    async def run(self, egress_queue: queue.Queue):
        try:
            while True:
                try:
                    # Actually consume the queue to prevent teardown deadlocks!
                    batch = egress_queue.get_nowait()
                    self.received_batches.append(batch)
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass  # Clean shutdown


class SlowEgress(BaseEgress):
    """An egress provider that consumes one batch at a time, slowly.

    The old teardown gave the queue five fixed seconds and then discarded the rest,
    so a sink like this lost its tail. These tests pin the behaviour that replaced
    that: the wait lasts as long as the queue keeps shrinking.
    """

    def __init__(self, delay: float = 0.2):
        self.delay = delay
        self.received_batches: list = []

    async def run(self, egress_queue: queue.Queue):
        try:
            while True:
                try:
                    batch = egress_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.01)
                    continue
                await asyncio.sleep(self.delay)
                self.received_batches.append(batch)
        except asyncio.CancelledError:
            pass


class StalledEgress(BaseEgress):
    """An egress provider that never consumes anything."""

    async def run(self, egress_queue: queue.Queue):
        try:
            while True:
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            pass


def test_teardown_waits_for_a_slow_sink():
    """Teardown drains the queue rather than abandoning it on a deadline."""
    env = DynamicRealtimeEnvironment(strict=False)
    slow = SlowEgress(delay=0.2)
    env.setup_egress(providers=[slow], batch_size=1)

    for index in range(10):
        env.publish_telemetry(f"metric_{index}", float(index))

    env.teardown()

    # Ten batches at 0.2s each take longer than the five seconds the old code
    # allowed, so this is exactly the case that used to lose data.
    assert env.egress_queue.empty()
    assert len(slow.received_batches) == 10


def test_teardown_sleeps_half_a_second_after_the_queue_drains():
    """Once the queue empties, teardown sleeps a fixed 0.5 s to let a file sink close
    its last file, so even a fast drain costs that half second."""
    env = DynamicRealtimeEnvironment(strict=False)
    slow = SlowEgress(delay=0.3)
    env.setup_egress(providers=[slow], batch_size=1)

    env.publish_telemetry("metric", 1.0)

    started = time.monotonic()
    env.teardown()
    elapsed = time.monotonic() - started

    assert len(slow.received_batches) == 1
    assert elapsed >= 0.5
    assert elapsed < 5.0


def test_teardown_gives_up_when_the_queue_stops_shrinking():
    """A sink that consumes nothing must not hang teardown forever."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[StalledEgress()], batch_size=1, drain_stall_seconds=0.3)

    env.egress_queue.put([{"stream_type": "telemetry"}])

    env.teardown()

    assert not env.egress_queue.empty()


def test_flush_fails_loudly_when_the_sink_stops_consuming():
    """A full queue raises rather than blocking the simulation forever."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[StalledEgress()],
        batch_size=1,
        max_queued_batches=1,
        drain_stall_seconds=0.2,
    )

    env.egress_queue.put(["already full"])

    with pytest.raises(RuntimeError, match="stopped consuming"):
        env.publish_telemetry("metric", 1.0)

    env.teardown()


class BusyProvider(BaseEgress):
    """A provider that reports work in flight without ever finishing it."""

    def __init__(self):
        self.active_tasks = 3

    async def run(self, egress_queue: queue.Queue):
        try:
            while True:
                try:
                    egress_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


def test_egress_defaults_are_bounded_and_patient():
    """The defaults are the contract, so they are worth pinning."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[TrackingEgress()])

    assert env.egress_queue.maxsize == 2000
    assert env.egress_drain_stall_seconds == 30.0

    env.teardown()


def test_backpressure_loses_nothing_with_a_tiny_queue():
    """A queue far smaller than the workload must still deliver every batch."""
    env = DynamicRealtimeEnvironment(strict=False)
    slow = SlowEgress(delay=0.02)
    env.setup_egress(providers=[slow], batch_size=1, max_queued_batches=2)

    for index in range(25):
        env.publish_telemetry(f"metric_{index}", float(index))

    env.teardown()

    assert len(slow.received_batches) == 25
    assert env.egress_queue.empty()


def test_teardown_returns_when_providers_never_finish():
    """A provider stuck with active tasks must not hold teardown open forever."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[BusyProvider()], drain_stall_seconds=0.3)

    started = time.monotonic()
    env.teardown()
    elapsed = time.monotonic() - started

    # The guard gives up shortly after the stall window rather than hanging.
    assert elapsed < 5.0


def test_stall_window_is_honoured_before_giving_up():
    """The wait lasts about as long as it was told to, not five fixed seconds."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[StalledEgress()], batch_size=1, drain_stall_seconds=1.0)

    env.egress_queue.put([{"stream_type": "telemetry"}])

    started = time.monotonic()
    env.teardown()
    elapsed = time.monotonic() - started

    assert elapsed >= 1.0
    assert elapsed < 6.0


class RecordingEgress:
    """Captures every record it is handed, so a test can compare sinks."""

    def __init__(self):
        self.records: list = []
        self.batch_sizes: list = []
        self.active_tasks = 0

    async def run(self, egress_queue: queue.Queue):
        while True:
            try:
                batch = egress_queue.get_nowait()
                self.records.extend(batch)
                self.batch_sizes.append(len(batch))
            except queue.Empty:
                await asyncio.sleep(0.01)


def test_predicate_count_must_match_provider_count():
    """A misaligned list would silently route the wrong records, so it is rejected."""
    env = DynamicRealtimeEnvironment(strict=False)
    with pytest.raises(ValueError, match="matched by position"):
        env.setup_egress(
            providers=[RecordingEgress(), RecordingEgress()], predicates=[None]
        )


LOGICAL_START = datetime(2024, 1, 1, 12, 0, 0)


def watch_factor(env, until, interval=0.25):
    """Runs `env` and returns the pacing factor seen at each simulated instant."""
    seen: list = []

    def watcher():
        while True:
            seen.append((round(env.now, 3), env.factor))
            yield env.timeout(interval)

    env.process(watcher())
    env.run(until=until)
    return seen


def test_factor_switches_when_the_logical_clock_reaches_go_live():
    """One run, two speeds: unpaced history, then real time from the go-live instant."""
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START + timedelta(seconds=0.5),
    )

    started = time.monotonic()
    seen = watch_factor(env, until=1.0)
    elapsed = time.monotonic() - started

    before = [factor for sim_time, factor in seen if sim_time < 0.5]
    after = [factor for sim_time, factor in seen if sim_time >= 0.5]

    assert before and set(before) == {0.0}
    assert len(after) >= 2 and set(after) == {1.0}

    # The half second after the switch is paced, so it costs half a second of real
    # time. Without the switch the whole run would finish immediately.
    assert elapsed >= 0.4
    assert elapsed < 5.0


def test_live_pacing_is_measured_from_the_go_live_instant():
    """Backfilled simulation time must not become real waiting at the switch.

    simpy maps simulation time onto the wall clock from a pair of anchors taken when
    the environment is built. Switching the factor without moving those anchors makes
    the first paced event sleep off the entire backfill, which here would be ten
    seconds instead of a quarter of a second, and a week in the case this exists for.
    """
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START + timedelta(seconds=10),
    )

    seen: list = []

    def backfill_then_live():
        yield env.timeout(9.5)  # the whole backfill in one step
        seen.append((env.now, env.factor))
        yield env.timeout(0.5)  # lands exactly on the go-live instant
        seen.append((env.now, env.factor))
        yield env.timeout(0.25)  # the first paced interval
        seen.append((env.now, env.factor))

    env.process(backfill_then_live())

    started = time.monotonic()
    env.run()
    elapsed = time.monotonic() - started

    assert [factor for _, factor in seen] == [0.0, 1.0, 1.0]
    assert elapsed >= 0.2
    assert elapsed < 5.0


def test_a_run_without_go_live_keeps_its_factor():
    """No go-live instant means the previous behaviour, unpaced from start to end."""
    env = DynamicRealtimeEnvironment(
        factor=0.0, strict=False, logical_start_time=LOGICAL_START
    )

    started = time.monotonic()
    seen = watch_factor(env, until=3600, interval=60)
    elapsed = time.monotonic() - started

    assert {factor for _, factor in seen} == {0.0}
    assert env.factor == 0.0
    # An hour of simulated time still costs no real time, and the clock anchor that
    # the switch would have moved is untouched.
    assert elapsed < 2.0
    assert env.env_start == 0


def test_go_live_before_the_start_paces_the_whole_run(caplog):
    """A go-live instant already in the past is the degenerate case: live throughout."""
    with caplog.at_level(logging.WARNING, logger="desflow.core.environment"):
        env = DynamicRealtimeEnvironment(
            factor=0.0,
            strict=False,
            logical_start_time=LOGICAL_START,
            go_live_at=LOGICAL_START - timedelta(hours=1),
        )

    seen = watch_factor(env, until=0.5)

    assert set(factor for _, factor in seen) == {1.0}
    assert "before the start of the run" in caplog.text


def test_go_live_at_the_start_is_a_live_run():
    """Going live at the first instant is allowed and needs no warning."""
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START,
    )

    seen = watch_factor(env, until=0.5)

    assert set(factor for _, factor in seen) == {1.0}


def test_go_live_does_not_hold_the_schedule_open():
    """A pending switch must not keep an otherwise finished run alive."""
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START + timedelta(days=7),
    )

    def short_process():
        yield env.timeout(1.0)

    env.process(short_process())

    started = time.monotonic()
    env.run()
    elapsed = time.monotonic() - started

    assert env.now == 1.0
    assert env.factor == 0.0
    assert elapsed < 2.0


def test_go_live_switch_reads_the_head_it_processes(monkeypatch):
    """The switch starts the lag monitor and the flush timers, which are scheduled at
    `now`, before the go-live instant. The step then waits for the event it processes,
    which is one of those, not for the model event behind them. Otherwise every timer
    from the go-live instant to the next model event runs at once when that event is
    due."""
    clock = {"now": 1000.0}
    monkeypatch.setattr(simpy.rt, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(
        simpy.rt, "sleep", lambda delay: clock.__setitem__("now", clock["now"] + delay)
    )
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START + timedelta(seconds=10),
    )
    env.setup_egress([TrackingEgress()], lag_monitor_interval="auto")
    ran = []
    original = simpy.Environment.step

    def recording_step(self):
        ran.append((self._queue[0][0], clock["now"]))
        original(self)

    monkeypatch.setattr(simpy.Environment, "step", recording_step)

    def model_event():
        yield env.timeout(13)

    env.process(model_event())
    env.run(until=20)
    env.teardown()
    # The lag monitor ticks at 10, 11 and 12, each at its own wall time, before the
    # model event at 13 is due
    live = {(sim, wall) for sim, wall in ran if 10 <= sim <= 13}
    assert live == {(10, 1000.0), (11, 1001.0), (12, 1002.0), (13, 1003.0)}


def test_step_across_go_live_returns_after_a_model_event():
    """The go-live switch puts desflow's own events at the head of the queue. A `step()`
    that reaches it keeps going until it has processed a model event, as SimPy's
    `step()` processes the model event it is given."""
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START + timedelta(seconds=10),
    )
    env.setup_egress([TrackingEgress()], lag_monitor_interval="auto")
    reached = []

    def model():
        yield env.timeout(13)
        reached.append(env.now)
        yield env.timeout(1)
        reached.append(env.now)

    env.process(model())
    env.step()
    env.step()
    assert env.now == 13
    assert reached == [13]
    env.step()
    assert env.now == 14
    assert reached == [13, 14]
    env.teardown()


def test_go_live_without_an_offset_is_read_in_the_zone():
    """A naive start and an aware go-live are compared in the run's zone, UTC by
    default, so they are ten seconds apart."""
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START.replace(tzinfo=timezone.utc) + timedelta(seconds=10),
    )
    assert env._go_live_sim_time == 10.0
    assert env.start_datetime.tzinfo is timezone.utc


def test_the_clock_runs_in_utc_across_a_daylight_saving_change():
    """12:00 on 3 Oct 2026 in Melbourne plus 24 hours of simulation is 13:00 the
    next day, as the clocks went forward an hour on 4 Oct; timestamps carry the
    offset of their moment."""
    env = DynamicRealtimeEnvironment(
        factor=0,
        logical_start_time=datetime(2026, 10, 3, 12, 0),
        timezone="Australia/Melbourne",
    )
    start = env.start_datetime
    assert env._get_iso_timestamp(start, 0) == "2026-10-03T12:00:00.000+10:00"
    assert env._get_iso_timestamp(start, 86_400) == "2026-10-04T13:00:00.000+11:00"
    with pytest.raises(ValueError, match="not an IANA time zone name"):
        DynamicRealtimeEnvironment(timezone="Mars/Base")


def test_batch_size_governs_when_unpaced():
    """At factor=0 the simulation clock outruns the wall clock, so a flush timer in
    simulation seconds fires constantly and batch_size never governs. Issue #25."""
    tracker = RecordingEgress()
    env = DynamicRealtimeEnvironment(factor=0, strict=False)
    # lag_monitor_interval=0 turns off the built-in lag metric, which would otherwise
    # publish one record per simulated second and change the batch arithmetic.
    env.setup_egress(
        providers=[tracker], batch_size=10, flush_interval=1.0, lag_monitor_interval=0
    )

    def publisher():
        for i in range(30):
            env.publish_telemetry("metric", i)
            yield env.timeout(1.0)

    env.process(publisher())
    env.run(until=31)
    env.teardown()

    assert len(tracker.records) == 30
    # 30 records at batch_size 10 is three full batches. A per-simulation-second timer
    # would have cut them into batches of one.
    assert tracker.batch_sizes[:3] == [10, 10, 10]


def test_flush_timer_still_runs_when_paced():
    """With factor above 0 the interval is a real latency bound, so it stays."""
    tracker = RecordingEgress()
    env = DynamicRealtimeEnvironment(factor=0.001, strict=False)
    env.setup_egress(
        providers=[tracker], batch_size=1000, flush_interval=1.0, lag_monitor_interval=0
    )

    def publisher():
        for i in range(5):
            env.publish_telemetry("metric", i)
            yield env.timeout(1.0)

    env.process(publisher())
    env.run(until=6)
    env.teardown()

    # batch_size is never reached, so every record arrived via the timer.
    assert len(tracker.records) == 5
    assert max(tracker.batch_sizes) < 1000


class _Level(enum.IntEnum):
    HIGH = 9


class _Aliased(BaseModel):
    """A model whose field renames itself on the way out."""

    real_name: str = Field(serialization_alias="aliasName")


class _Nested(BaseModel):
    when: datetime
    amount: Decimal


# Values chosen because each one is serialized by a different branch of the
# pydantic serializer. Every one of them is published by both the model and the
# hand-built dict, so the two must agree on all of them.
PAYLOAD_VALUES = [
    ("json_native", {"s": "a", "i": 1, "f": 1.5, "b": True, "n": None}),
    ("nested", {"a": {"b": [1, 2, {"c": "d"}]}}),
    ("empty", {}),
    ("datetime", {"when": datetime(2024, 3, 4, 5, 6, 7, 891234)}),
    (
        "date_time_delta",
        {"d": date(2024, 3, 4), "t": dt_time(1, 2, 3), "td": timedelta(days=1)},
    ),
    ("decimal", {"amount": Decimal("1.10")}),
    ("uuid", {"id": uuid.UUID("12345678-1234-5678-1234-567812345678")}),
    ("enum", {"level": _Level.HIGH}),
    ("set_frozenset", {"s": {1, 2}, "f": frozenset(["a"])}),
    ("bytes", {"b": b"abc"}),
    ("model", _Nested(when=datetime(2024, 1, 1), amount=Decimal("2.50"))),
    ("model_in_dict", {"n": _Nested(when=datetime(2024, 1, 1), amount=Decimal("0.1"))}),
    ("aliased_model", _Aliased(real_name="n")),
    ("nan_inf", {"a": float("nan"), "b": float("inf"), "c": float("-inf")}),
    ("bare_nan", float("nan")),
    ("big_int", {"n": 2**80}),
    ("scalar_str", "a bare string"),
    ("scalar_int", 42),
    ("scalar_list", [1, "two", 3.0, None]),
    (
        "envelope_key_collision",
        {"stream_type": "x", "timestamp": "y", "key": "z", "sim_ts": 1},
    ),
    ("numpy_float", {"v": np.float64(2.5)}),
]


def _buffered_env():
    """An environment whose records stay in the buffer, so each one can be read."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[TrackingEgress()], batch_size=10**6, lag_monitor_interval=0
    )
    return env


def _assert_same_payload(record, expected, case_id):
    assert record == expected, case_id
    assert list(record) == list(expected), f"{case_id}: key order"
    assert [type(v) for v in record.values()] == [type(v) for v in expected.values()], (
        f"{case_id}: value types"
    )


@pytest.mark.parametrize(
    "case_id,value", PAYLOAD_VALUES, ids=[c[0] for c in PAYLOAD_VALUES]
)
def test_publish_event_matches_event_payload_model(case_id, value):
    """publish_event builds its dict directly, so pin it to the schema it replaced.

    Up to 0.12.0 the record was `EventPayload(...).model_dump(mode="json")`. Consumers
    read that shape, so the hand-built dict has to reproduce it byte for byte, not
    merely decode to the same JSON.
    """
    env = _buffered_env()
    try:
        expected = EventPayload(
            key="task-001",
            value=value,
            sim_ts=round(env.now, 3),
            timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
        ).model_dump(mode="json")

        env.publish_event("task-001", value)

        _assert_same_payload(env._event_buffer[-1], expected, case_id)
    finally:
        env.teardown()


@pytest.mark.parametrize(
    "case_id,value", PAYLOAD_VALUES, ids=[c[0] for c in PAYLOAD_VALUES]
)
def test_publish_telemetry_matches_telemetry_payload_model(case_id, value):
    """Same contract as publish_event, for the scalar metric stream."""
    env = _buffered_env()
    try:
        expected = TelemetryPayload(
            path_id="Shop.packers.utilization",
            value=value,
            sim_ts=round(env.now, 3),
            timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
        ).model_dump(mode="json")

        env.publish_telemetry("Shop.packers.utilization", value)

        _assert_same_payload(env._event_buffer[-1], expected, case_id)
    finally:
        env.teardown()


def test_sim_ts_is_a_float_when_the_clock_is_an_integer():
    """`env.now` is the integer 0 until the first fractional timeout.

    `sim_ts` was declared `float` on the model, so 0 was published as 0.0 and every
    JSON record carried "sim_ts":0.0. Publishing the raw integer writes "sim_ts":0,
    which changes the bytes and the inferred column type of every early record.
    """
    env = _buffered_env()
    try:
        assert isinstance(env.now, int)

        env.publish_event("task-001", {"status": "started"})
        env.publish_telemetry("metric", 1.0)

        for record in env._event_buffer:
            assert type(record["sim_ts"]) is float
        assert b'"sim_ts":0.0' in orjson.dumps(env._event_buffer[0])
    finally:
        env.teardown()


def test_sim_ts_is_a_plain_float_when_the_clock_is_a_numpy_float():
    """A numpy delay leaves `env.now` a numpy float, which orjson cannot serialize.

    The model coerced it to a plain float, so the sinks never saw a numpy type.
    """
    env = _buffered_env()
    try:
        env._now = np.float64(3.14159)

        env.publish_event("task-001", {"status": "started"})

        record = env._event_buffer[-1]
        assert type(record["sim_ts"]) is float
        assert record["sim_ts"] == 3.142
        assert orjson.dumps(record)  # a numpy float raises here
    finally:
        env.teardown()


def test_non_string_keys_are_coerced_or_rejected_as_the_model_did():
    """`key` and `path_id` were declared `str`, so the model validated them.

    Bytes were decoded and a number was rejected outright. Passing them straight
    through instead wrote a non-string key to the sinks, and orjson cannot serialize
    bytes at all.
    """
    env = _buffered_env()
    try:
        env.publish_event(b"task-001", {"status": "started"})
        assert env._event_buffer[-1]["key"] == "task-001"
        assert type(env._event_buffer[-1]["key"]) is str

        env.publish_telemetry(b"metric", 1.0)
        assert env._event_buffer[-1]["path_id"] == "metric"

        with pytest.raises(ValidationError):
            env.publish_event(5, {"status": "started"})
        with pytest.raises(ValidationError):
            env.publish_telemetry(None, 1.0)
    finally:
        env.teardown()


def test_a_value_the_serializer_cannot_handle_still_raises():
    """The model refused a type it could not serialize, and so must this path.

    Letting the object through would move the failure to the egress thread, where the
    record is lost rather than reported.
    """
    env = _buffered_env()
    try:
        with pytest.raises(PydanticSerializationError):
            env.publish_event("task-001", {"i": np.int64(7)})
        with pytest.raises(PydanticSerializationError):
            env.publish_telemetry("metric", object())
    finally:
        env.teardown()


def test_envelope_key_order_matches_the_published_schema():
    """Key order is part of the record, because the sinks serialize the dict as given."""
    env = _buffered_env()
    try:
        env.publish_event("orders-1", {"status": "started"}, "pack", 0)
        env.publish_telemetry("metric", 1.0)

        assert list(env._event_buffer[0]) == [
            "stream_type",
            "sim_ts",
            "timestamp",
            "key",
            "task",
            "instance",
            "value",
        ]
        assert list(env._event_buffer[1]) == [
            "stream_type",
            "sim_ts",
            "timestamp",
            "path_id",
            "value",
        ]
        assert list(EventPayload.model_fields) == list(env._event_buffer[0])
        assert list(TelemetryPayload.model_fields) == list(env._event_buffer[1])
    finally:
        env.teardown()

    # A ground-truth record, in a labels sink's buffer
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[TrackingEgress()],
        batch_size=10**6,
        lag_monitor_interval=0,
        labels=[True],
    )
    try:
        env.publish_label({"fault": "slow"}, path="Shop.x", old=1, new=2)
        assert (
            list(env._event_buffer[0])
            == list(LabelPayload.model_fields)
            == [
                "stream_type",
                "sim_ts",
                "timestamp",
                "path",
                "old",
                "new",
                "label",
            ]
        )
    finally:
        env.teardown()


def test_two_providers_flush_at_their_own_sizes():
    """One cadence for every sink meant a stream sink and a lake sink could not
    coexist: the stream one wants small frequent batches and the lake one wants
    large rare ones, because the batch is the file."""
    small, large = TrackingEgress(), TrackingEgress()
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[small, large],
        batch_sizes=[2, 10**6],
        lag_monitor_interval=0,
    )

    for index in range(6):
        env.publish_event(f"task-{index}", {"n": index})

    # The small sink flushed three batches of two; the large one is still buffering.
    assert [len(batch) for batch in env._event_buffers] == [0, 6]
    env.teardown_egress()

    assert [len(batch) for batch in small.received_batches] == [2, 2, 2]
    assert [len(batch) for batch in large.received_batches] == [6]


def test_omitting_the_per_provider_values_reproduces_the_shared_cadence():
    """Every existing call passes neither, so both have to fall back to the context
    default rather than to a hardcoded number."""
    first, second = TrackingEgress(), TrackingEgress()
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(providers=[first, second], batch_size=3, lag_monitor_interval=0)

    assert env.egress_batch_sizes == [3, 3]
    assert env.egress_flush_intervals == [1.0, 1.0]

    for index in range(3):
        env.publish_event(f"task-{index}", {"n": index})
    env.teardown_egress()

    assert [len(batch) for batch in first.received_batches] == [3]
    assert [len(batch) for batch in second.received_batches] == [3]


def test_a_predicate_keeps_records_out_of_the_other_buffer():
    """The predicate moved from flush time to append time, so a record is tested once
    on the way in rather than against every predicate at fan-out. A buffer must then
    hold only what its own sink takes."""
    evens, odds = TrackingEgress(), TrackingEgress()
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[evens, odds],
        batch_size=10**6,
        predicates=[
            lambda record: record["value"]["n"] % 2 == 0,
            lambda record: record["value"]["n"] % 2 == 1,
        ],
        lag_monitor_interval=0,
    )

    for index in range(6):
        env.publish_event(f"task-{index}", {"n": index})

    assert [len(buffer) for buffer in env._event_buffers] == [3, 3]
    env.teardown_egress()
    assert [record["value"]["n"] for record in evens.received_batches[0]] == [0, 2, 4]
    assert [record["value"]["n"] for record in odds.received_batches[0]] == [1, 3, 5]


def test_teardown_drains_every_buffer():
    """A partial batch in any provider's buffer is data the run produced, so teardown
    has to flush all of them rather than only the first."""
    first, second, third = TrackingEgress(), TrackingEgress(), TrackingEgress()
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[first, second, third],
        batch_sizes=[10**6, 10**6, 10**6],
        lag_monitor_interval=0,
    )

    env.publish_event("task-001", {"n": 1})
    env.teardown_egress()

    for provider in (first, second, third):
        assert [len(batch) for batch in provider.received_batches] == [1]


def test_mismatched_per_provider_lists_are_rejected():
    """Aligned by position, so a short list would silently give the wrong sink the
    wrong cadence."""
    env = DynamicRealtimeEnvironment(strict=False)
    with pytest.raises(ValueError, match="matched by position"):
        env.setup_egress(
            providers=[TrackingEgress(), TrackingEgress()],
            batch_sizes=[10],
            lag_monitor_interval=0,
        )


def test_a_batch_size_that_never_governs_is_reported_once(caplog):
    """The two limits are an OR, so a high batch_size with a short flush_interval
    means the size does nothing. No configuration-time check can catch it, because
    the arrival rate is unknown until the run starts."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[TrackingEgress()], batch_sizes=[1000], lag_monitor_interval=0
    )

    with caplog.at_level(logging.WARNING):
        for _ in range(25):
            env.publish_event("task", {"n": 1})
            env._note_timer_flush(0)
            env._flush_buffer(0)

    warnings = [r for r in caplog.records if "never governs" in r.message]
    assert len(warnings) == 1
    # Named, because an index alone makes the reader count their add_egress calls.
    assert "TrackingEgress" in warnings[0].getMessage()
    env.teardown_egress()


def test_no_warning_when_batch_size_governs(caplog):
    """A sink whose size does govern is configured correctly and must stay quiet."""
    env = DynamicRealtimeEnvironment(strict=False)
    env.setup_egress(
        providers=[TrackingEgress()], batch_sizes=[2], lag_monitor_interval=0
    )

    with caplog.at_level(logging.WARNING):
        for _ in range(50):
            env.publish_event("task", {"n": 1})
            env._note_timer_flush(0)

    assert not [r for r in caplog.records if "never governs" in r.message]
    env.teardown_egress()


def test_going_live_starts_a_flush_timer_for_every_provider():
    """setup_egress skips the timers at factor=0, so going live starts them. It started
    one before the buffers were split per provider, which left every sink after the
    first reaching its destination only at teardown."""
    first, second = TrackingEgress(), TrackingEgress()
    env = DynamicRealtimeEnvironment(
        factor=0.0,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=LOGICAL_START + timedelta(seconds=0.2),
    )
    env.setup_egress(
        providers=[first, second],
        batch_sizes=[10**6, 10**6],
        flush_intervals=[0.05, 0.05],
        lag_monitor_interval=0,
    )

    def emit():
        while True:
            env.publish_event("task", {"n": 1})
            yield env.timeout(0.05)

    env.process(emit())
    env.run(until=0.6)

    # Both sinks received data while the run was still going, not just at teardown.
    assert first.received_batches
    assert second.received_batches
    env.teardown_egress()


@pytest.mark.parametrize("provider_count", [1, 2])
def test_go_live_does_not_duplicate_periodic_flush(provider_count):
    """A run that starts paced keeps one flush timer per provider after go-live."""
    start = datetime(2026, 1, 1)
    env = DynamicRealtimeEnvironment(
        logical_start_time=start,
        # Non-zero, so setup_egress starts the timers, but small enough that the
        # backfill costs almost no real time. factor is real seconds per sim second.
        factor=0.01,
        strict=False,
        go_live_at=start + timedelta(seconds=0.2),
    )
    started = []
    original = env._periodic_flush

    def record(index):
        started.append(index)
        return original(index)

    env._periodic_flush = record
    env.setup_egress(
        providers=[TrackingEgress() for _ in range(provider_count)],
        flush_interval=0.05,
        lag_monitor_interval=0,
    )
    assert started == list(range(provider_count))

    def tick():
        while True:
            yield env.timeout(0.05)

    env.process(tick())
    env.run(until=0.4)

    # Without the guard the go-live switch starts a second timer per provider, which
    # flushes each buffer twice an interval and hides a starved batch_size. One sink
    # is affected exactly as many are, so both counts are checked.
    assert started == list(range(provider_count))
    env.teardown_egress()


def test_nan_becomes_none_inside_a_pydantic_model():
    """A model keeps NaN in its own dump, so the record value is cleaned as well."""
    from pydantic import BaseModel

    from desflow.core.environment import _jsonable

    class Reading(BaseModel):
        celsius: float
        peak: float

    nan, inf = float("nan"), float("inf")
    assert _jsonable(Reading(celsius=nan, peak=inf)) == {"celsius": None, "peak": None}
    assert _jsonable({"r": Reading(celsius=nan, peak=1.0)}) == {
        "r": {"celsius": None, "peak": 1.0}
    }
    assert _jsonable([Reading(celsius=2.0, peak=nan)]) == [
        {"celsius": 2.0, "peak": None}
    ]
    assert _jsonable({"x": nan, "y": 1}) == {"x": None, "y": 1}


def test_lag_monitor_runs_with_a_start_time_in_a_time_zone():
    """The lag monitor subtracted a naive now() from the start time, so a run with
    a logical_start_time that has a time zone raised TypeError once it had a sink."""
    tracker = RecordingEgress()
    env = DynamicRealtimeEnvironment(
        factor=0,
        strict=False,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    env.setup_egress(providers=[tracker], batch_size=1, lag_monitor_interval=1.0)
    env.run(until=3)
    env.teardown()

    lags = [
        r for r in tracker.records if r["path_id"] == "system.simulation.lag_seconds"
    ]
    assert len(lags) == 3
    assert all(r["timestamp"].endswith("+00:00") for r in lags)


def _lag_times(factor, until, **kwargs):
    """The simulation times of the lag records of an empty run."""
    tracker = RecordingEgress()
    go_live_at = kwargs.pop("go_live_at", None)
    env = DynamicRealtimeEnvironment(
        factor=factor,
        strict=False,
        logical_start_time=LOGICAL_START,
        go_live_at=go_live_at,
    )
    env.setup_egress(providers=[tracker], batch_size=1, **kwargs)
    env.process(_tick(env, until))
    env.run(until=until)
    env.teardown()
    return [
        r["sim_ts"]
        for r in tracker.records
        if r["path_id"] == "system.simulation.lag_seconds"
    ]


def _tick(env, until):
    """An event every 0.25 s, so a run that goes live has events to pace."""
    while env.now < until:
        yield env.timeout(0.25)


def test_lag_is_off_at_factor_0_and_on_otherwise_by_default():
    """At factor 0 the clock is detached from the wall clock, so no lag record is
    published; at factor 1 one is published every simulation second."""
    assert _lag_times(0, 3) == []
    assert _lag_times(1, 2) == [0.0, 1.0]
    # An explicit interval applies at any factor, and 0 or None turns it off
    assert _lag_times(0, 3, lag_monitor_interval=1.0) == [0.0, 1.0, 2.0]
    assert _lag_times(1, 1, lag_monitor_interval=0) == []


def test_lag_starts_when_a_run_at_factor_0_goes_live():
    """A backfill at factor 0 has no lag records until it goes live at 1 s."""
    times = _lag_times(0, 2.5, go_live_at=LOGICAL_START + timedelta(seconds=1))
    assert times == [1.0, 2.0]


def test_cached_timestamp_is_the_isoformat_text():
    """The cached text equals isoformat of the moment for every second of a day and
    for the milliseconds of an hour, with a start that has microseconds, and with a
    start in a zone that changes its offset that day."""
    from zoneinfo import ZoneInfo

    env = DynamicRealtimeEnvironment(factor=0)
    starts = [
        datetime(2026, 10, 1, 9, 0, 0, 999_500, tzinfo=timezone.utc),
        datetime(2026, 10, 3, 12, 0, tzinfo=ZoneInfo("Australia/Melbourne")),
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    ]
    times = [float(s) for s in range(86_400)] + [ms / 1000 for ms in range(3_600_000)]
    times += [s + 0.0005 for s in range(1000)] + [s + 0.9995 for s in range(1000)]
    for start in starts:
        env.zone = start.tzinfo
        for now in times[:: 1 if start.tzinfo is timezone.utc else 7]:
            # The clock runs in UTC and the text is written in the zone
            utc = start.astimezone(timezone.utc) + timedelta(seconds=now)
            moment = utc.astimezone(start.tzinfo)
            expected = moment.isoformat(timespec="milliseconds")
            assert env._get_iso_timestamp(start, now) == expected, (start, now)


def test_factor_0_steps_without_the_wall_clock_and_gives_the_same_records():
    """At factor 0 SimPy's own step runs; the records are those of the real-time
    step, which waits for nothing at factor 0."""
    from simpy import RealtimeEnvironment

    from desflow import DistributionConfig, Sampler

    def run(step):
        env = DynamicRealtimeEnvironment(
            factor=0, strict=False, logical_start_time=LOGICAL_START
        )
        env.step = step.__get__(env)  # type: ignore[method-assign]
        tracker = RecordingEgress()
        env.setup_egress(providers=[tracker], batch_size=10**6)
        sampler = Sampler(seed=7)
        gaps = DistributionConfig(dist="exponential", rate=2.0)

        def jobs():
            for n in range(200):
                yield env.timeout(sampler.sample(gaps))
                env.publish_event(f"jobs-{n}", {"n": n})

        env.process(jobs())
        env.run(until=60)
        env.teardown()
        return tracker.records

    fast = run(DynamicRealtimeEnvironment.step)
    assert fast
    assert fast == run(RealtimeEnvironment.step)


def test_ingress_monitor_applies_items_in_arrival_order():
    """Items put on the one ingress queue are applied in the order they arrived."""
    from desflow.models.params import SimParameter

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(SimParameter(sim_id="Shop", variables={"x": 0}))
    applied: list = []
    update = env.registry.update

    def recording_update(path, value, *args, **kwargs):
        applied.append((path, value))
        return update(path, value, *args, **kwargs)

    env.registry.update = recording_update  # type: ignore[method-assign]
    env.setup_ingress([])
    try:
        env.ingress_queue.put(("Shop.variables.x", 1))
        env.ingress_queue.put(("Shop.variables.x", 2))
        env.run(until=0.2)
    finally:
        env.teardown_ingress()

    assert env.registry.get("Shop.variables.x").value == 2
    assert applied == [("Shop.variables.x", 1), ("Shop.variables.x", 2)]


def _wait_for_ingress_items(env, count):
    """Waits, for at most 2 wall-clock seconds, until `count` items are queued."""
    deadline = time.monotonic() + 2
    while env.ingress_queue.qsize() < count:
        assert time.monotonic() < deadline, "the ingress items never arrived"
        time.sleep(0.005)


def _env_with_variable():
    """A factor 0 environment that has `Shop.variables.x` and a list of its changes."""
    from desflow.models.params import SimParameter

    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(SimParameter(sim_id="Shop", variables={"x": 0}))
    changes: list = []
    env.registry.on_change.append(changes.append)
    return env, changes


def test_ingress_changes_carry_their_source_and_connector():
    """A change from an ingress connector has source "ingress" and the class as actor."""
    from desflow import LocalIngress

    env, changes = _env_with_variable()
    env.setup_ingress([LocalIngress([(0.0, "Shop.variables.x", 5)])])
    try:
        _wait_for_ingress_items(env, 1)
        env.run(until=0.2)
    finally:
        env.teardown_ingress()

    assert [(c.path, c.new, c.source, c.actor) for c in changes] == [
        ("Shop.variables.x", 5, "ingress", "LocalIngress")
    ]


def test_file_ingress_changes_carry_their_source_and_connector(tmp_path):
    """A change read from a file has source "ingress" and FileIngress as actor."""
    from desflow import FileIngress

    path = tmp_path / "params.yaml"
    path.write_text("Shop.variables.x: 7\n")
    env, changes = _env_with_variable()
    env.setup_ingress([FileIngress(path, poll=0.05)])
    try:
        _wait_for_ingress_items(env, 1)
        env.run(until=0.2)
    finally:
        env.teardown_ingress()

    assert [(c.path, c.new, c.source, c.actor) for c in changes] == [
        ("Shop.variables.x", 7, "ingress", "FileIngress")
    ]


def test_ingress_keeps_one_order_across_providers():
    """Two providers share one arrival order, and `put_nowait` carries the actor too."""
    from desflow import LocalIngress
    from desflow.connectors.ingress.base import BaseIngress

    class Late(BaseIngress):
        async def run(self, ingress_queue):
            while ingress_queue.qsize() < 1:
                await asyncio.sleep(0.005)
            ingress_queue.put_nowait(("Shop.variables.x", 2))

    env, changes = _env_with_variable()
    env.setup_ingress([Late(), LocalIngress([(0.0, "Shop.variables.x", 1)])])
    try:
        _wait_for_ingress_items(env, 2)
        env.run(until=0.2)
    finally:
        env.teardown_ingress()

    assert [(c.new, c.actor) for c in changes] == [(1, "LocalIngress"), (2, "Late")]
    assert env.registry.get("Shop.variables.x").value == 2


def test_a_source_queue_can_be_copied():
    """A copy has no shared queue until `__init__` runs, and asking for it must
    raise AttributeError instead of calling `__getattr__` without end."""
    import copy

    from desflow.core.environment import _SourceQueue

    shared: "queue.Queue[tuple]" = queue.Queue()
    copied = copy.copy(_SourceQueue(shared, actor="LocalIngress"))
    copied.put(("Shop.variables.x", 1))

    assert shared.get_nowait() == ("Shop.variables.x", 1, "LocalIngress")

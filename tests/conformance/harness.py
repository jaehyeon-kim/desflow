"""Shared tools for the native conformance tests.

Each conformance test compares a plain SimPy reference model against the
equivalent desflow model in the low-level, builder and YAML interfaces. The
comparison is on the published event trace: the simulation time, entity key,
task and status of each event, captured through the real egress path so the
test exercises what a run actually writes.
"""

import asyncio
import queue
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from desflow import DynamicRealtimeEnvironment, SimParameter
from desflow.core.context import SimulationContext

# One captured event: simulation time rounded to 3 decimals, entity key, task
# name, and the status the task published (started, finished and so on).
Event = Tuple[float, str, Any, Any]


class RecordingEgress:
    """An egress that keeps every record it is handed, for trace comparison.

    It uses the same `run(egress_queue)` interface the real connectors use, so a
    recorded run goes through `publish_event`, the buffer and the queue exactly
    as a Kafka or Parquet run does.
    """

    def __init__(self) -> None:
        self.records: List[Dict[str, Any]] = []

    async def run(self, egress_queue: "queue.Queue[Any]") -> None:
        try:
            while True:
                try:
                    self.records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.005)
        except asyncio.CancelledError:
            pass


def event_trace(records: List[Dict[str, Any]]) -> List[Event]:
    """The event records as comparable tuples, in publication order.

    Telemetry and label records are left out, so the trace is the discrete
    events a reader compares across interfaces and against SimPy.
    """
    trace: List[Event] = []
    for record in records:
        if record.get("stream_type") != "event":
            continue
        value = record.get("value")
        status = value.get("status") if isinstance(value, dict) else value
        trace.append(
            (round(float(record["sim_ts"]), 3), record["key"], record["task"], status)
        )
    return trace


BLUEPRINTS = Path(__file__).parent / "blueprints"


def at(records: List[Dict[str, Any]], status: Any) -> List[Tuple[float, str]]:
    """The time and entity key of every event with this status, in order."""
    return [(t, key) for t, key, _task, s in event_trace(records) if s == status]


def times(
    records: List[Dict[str, Any]], task: str, status: Optional[str] = None
) -> List[float]:
    """The simulation times of a task's events, by status.

    With `status` None it returns the payload events (a mapping with no
    `status` key); with a status it returns the events of that status.
    """
    found: List[float] = []
    for record in records:
        if record["stream_type"] != "event" or record["task"] != task:
            continue
        value = record["value"]
        is_status = isinstance(value, dict) and "status" in value
        if (status is None and not is_status) or (
            status is not None and is_status and value["status"] == status
        ):
            found.append(record["sim_ts"])
    return found


def periodic(env: Any, gap: float, start: Callable[[], Any]) -> Any:
    """Starts `start()` as a process every `gap` seconds, from `gap`."""
    while True:
        yield env.timeout(gap)
        env.process(start())


@dataclass
class Outcome:
    """A run's result when a runner is called with `raises=True`.

    `records` are the records written before the run ended, kept even when it
    raised; `value` is what the run returned (None when it raised); `error` is
    what it raised (None when it returned); `app_or_env` is the context or the
    environment the run used.
    """

    records: List[Dict[str, Any]]
    value: Any
    error: Optional[BaseException]
    app_or_env: Union[SimulationContext, DynamicRealtimeEnvironment]


def run_yaml(
    name: str,
    prepare: Optional[Callable[[SimulationContext], None]] = None,
    until: Optional[float] = None,
    raises: bool = False,
) -> Union[Tuple[List[Dict[str, Any]], SimulationContext], Outcome]:
    """Runs a blueprint from `blueprints/` into a recording egress.

    `prepare(app)` runs after `from_yaml` and `add_egress`, before the run, for
    example to add a probe process. `until` None keeps the blueprint's
    `run.until`, as `app.run()` does. With `raises=True` the run's exception is
    caught and returned in an `Outcome`; otherwise it propagates, as today.
    """
    rec = RecordingEgress()
    app = SimulationContext.from_yaml(BLUEPRINTS / name)
    app.add_egress(rec)
    if prepare is not None:
        prepare(app)
    if not raises:
        app.run(until=until)
        return rec.records, app
    value: Any = None
    error: Optional[BaseException] = None
    try:
        # run() returns the stop event's value from Batch 5 (E14); None until then.
        value = app.run(until=until)  # type: ignore[func-returns-value]
    except Exception as exc:  # noqa: BLE001: kept for the caller to assert on
        error = exc
    return Outcome(rec.records, value, error, app)


def run_builder(
    app: SimulationContext, until: float, raises: bool = False
) -> Union[List[Dict[str, Any]], Outcome]:
    """Runs a builder model into a recording egress.

    With `raises=True` the run's exception is caught and returned in an
    `Outcome`; otherwise it propagates, as today.
    """
    rec = RecordingEgress()
    app.add_egress(rec)
    if not raises:
        app.run(until=until)
        return rec.records
    value: Any = None
    error: Optional[BaseException] = None
    try:
        # run() returns the stop event's value from Batch 5 (E14); None until then.
        value = app.run(until=until)  # type: ignore[func-returns-value]
    except Exception as exc:  # noqa: BLE001: kept for the caller to assert on
        error = exc
    return Outcome(rec.records, value, error, app)


def run_low_level(
    params: Optional[SimParameter],
    start: Callable[[DynamicRealtimeEnvironment], Any],
    until: float,
    stats: Optional[str] = None,
    initial_time: float = 0.0,
    factor: float = 0.0,
    strict: bool = False,
    on_unhandled_failure: str = "stop",
    raises: bool = False,
) -> Union[Tuple[List[Dict[str, Any]], DynamicRealtimeEnvironment], Outcome]:
    """Runs a low-level model into a recording egress.

    `start(env)` builds the resources and starts the processes, after the
    parameters are registered and the egress and statistics are set up. The run
    is unpaced by default (`factor=0.0`); pass `factor` and `strict` for a paced
    run. `on_unhandled_failure` is `stop` by default, as a plain SimPy
    environment is, so a one-shot signal's failure nobody waits for stops the
    run; pass `continue` for the defusing behaviour. With `raises=True` the
    run's exception is caught and returned in an `Outcome`, with the records
    written before it; otherwise it propagates. The environment is always torn
    down.
    """
    env = DynamicRealtimeEnvironment(
        initial_time=initial_time,
        factor=factor,
        strict=strict,
        on_unhandled_failure=on_unhandled_failure,
    )
    if params is not None:
        env.registry.register_sim_parameter(params)
    rec = RecordingEgress()
    env.setup_egress([rec], lag_monitor_interval=None)
    if stats is not None:
        env.setup_stats(params.sim_id if params else "Sim", stats)
    start(env)
    value: Any = None
    error: Optional[BaseException] = None
    try:
        value = env.run(until=until)
    except Exception as exc:  # noqa: BLE001: returned when raises, else re-raised
        if not raises:
            raise
        error = exc
    finally:
        env.teardown()
    if raises:
        return Outcome(rec.records, value, error, env)
    return rec.records, env


def probe(
    read_at: List[float], read: Callable[[SimulationContext], Any]
) -> Tuple[Callable[[SimulationContext], Any], List[Tuple[float, Any]]]:
    """A builder process that reads the model at each time in `read_at`.

    Returns the generator function and the list of `(now, reading)` pairs it
    fills, in time order. Add it with `app.add_process(func)`, or pass it in a
    `run_yaml` `prepare`.
    """
    readings: List[Tuple[float, Any]] = []

    def func(context: SimulationContext) -> Any:
        for moment in read_at:
            yield context.env.timeout(moment - context.env.now)
            readings.append((context.env.now, read(context)))

    return func, readings


def probe_env(
    read_at: List[float], read: Callable[[DynamicRealtimeEnvironment], Any]
) -> Tuple[Callable[[DynamicRealtimeEnvironment], Any], List[Tuple[float, Any]]]:
    """The low-level form of `probe`: its generator function takes `env`.

    Start it with `env.process(func(env))` in `run_low_level`'s `start`.
    """
    readings: List[Tuple[float, Any]] = []

    def func(env: DynamicRealtimeEnvironment) -> Any:
        for moment in read_at:
            yield env.timeout(moment - env.now)
            readings.append((env.now, read(env)))

    return func, readings


class FakeClock:
    """A controllable stand-in for simpy.rt's wall clock.

    A paced run reads `simpy.rt.monotonic` and `simpy.rt.sleep`; patching them
    with this clock advances the wall time in sleep itself, so a factor test
    runs with no real waiting and a deterministic wall trace. Only those two
    names are patched, never `time.monotonic`, which the egress asyncio thread
    uses. Install it before the environment is built, because the environment
    reads the clock in its constructor.
    """

    def __init__(self) -> None:
        self._now = 0.0
        self.wall: List[Tuple[float, float]] = []

    def monotonic(self) -> float:
        return self._now

    def sleep(self, delta: float) -> None:
        if delta > 0:
            self._now += delta

    def advance(self, delta: float) -> None:
        self._now += delta

    def install(self, monkeypatch: Any) -> "FakeClock":
        import simpy.rt

        monkeypatch.setattr(simpy.rt, "monotonic", self.monotonic)
        monkeypatch.setattr(simpy.rt, "sleep", self.sleep)
        return self

    def trace(self, env: DynamicRealtimeEnvironment) -> List[Tuple[float, float]]:
        """Wraps `env.publish_event` so each event records its simulation time
        and the fake wall offset from the run's start. Returns the list it fills.
        """
        start = env.real_start
        original = env.publish_event

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            self.wall.append((round(env.now, 3), round(self._now - start, 6)))
            return original(*args, **kwargs)

        env.publish_event = wrapped  # type: ignore[method-assign]
        return self.wall

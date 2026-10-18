"""Shared tools for the native conformance tests.

Each conformance test compares a plain SimPy reference model against the
equivalent desflow model in the low-level, builder and YAML interfaces. The
comparison is on the published event trace: the simulation time, entity key,
task and status of each event, captured through the real egress path so the
test exercises what a run actually writes.
"""

import asyncio
import queue
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

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


def run_yaml(name: str) -> Tuple[List[Dict[str, Any]], SimulationContext]:
    """Runs a blueprint from `blueprints/` into a recording egress."""
    rec = RecordingEgress()
    app = SimulationContext.from_yaml(BLUEPRINTS / name)
    app.add_egress(rec)
    app.run()
    return rec.records, app


def run_builder(app: SimulationContext, until: float) -> List[Dict[str, Any]]:
    """Runs a builder model into a recording egress."""
    rec = RecordingEgress()
    app.add_egress(rec)
    app.run(until=until)
    return rec.records


def run_low_level(
    params: Optional[SimParameter],
    start: Callable[[DynamicRealtimeEnvironment], Any],
    until: float,
    stats: Optional[str] = None,
    initial_time: float = 0.0,
) -> Tuple[List[Dict[str, Any]], DynamicRealtimeEnvironment]:
    """Runs a low-level model, unpaced, into a recording egress.

    `start(env)` builds the resources and starts the processes, after the
    parameters are registered and the egress and statistics are set up.
    """
    env = DynamicRealtimeEnvironment(initial_time=initial_time, factor=0.0)
    if params is not None:
        env.registry.register_sim_parameter(params)
    rec = RecordingEgress()
    env.setup_egress([rec], lag_monitor_interval=None)
    if stats is not None:
        env.setup_stats(params.sim_id if params else "Sim", stats)
    start(env)
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return rec.records, env

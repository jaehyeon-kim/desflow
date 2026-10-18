"""Shared tools for the native conformance tests.

Each conformance test compares a plain SimPy reference model against the
equivalent desflow model in the low-level, builder and YAML interfaces. The
comparison is on the published event trace: the simulation time, entity key,
task and status of each event, captured through the real egress path so the
test exercises what a run actually writes.
"""

import asyncio
import queue
from typing import Any, Dict, List, Tuple

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

"""The Egress page: the JSON lines a run prints, checked against the record schema.

ConsoleEgress with format="json" prints each record as one JSON line. A consumer reads
the lines and checks each with the Pydantic model of its stream type.
"""

import contextlib
import io
import json

from desflow import (
    ConsoleEgress,
    EventPayload,
    LabelPayload,
    SimulationContext,
    TelemetryPayload,
)

app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=2)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5, spawn="pack")
    .with_stats("minimal", interval=10)
    # A second packer at 10 seconds, with a label that records why
    .add_step(10, path="Shop.resources.packers.current_cap", value=2,
              record={"cause": "rush"})
    .add_egress(ConsoleEgress(format="json"))
    .add_egress(ConsoleEgress(format="json"), when="labels")
)  # fmt: skip


@app.task(service_id="packing", resource_id="packers")
def pack(task_id):
    return {"event_type": "order_packed"}


printed = io.StringIO()
with contextlib.redirect_stdout(printed):
    app.run(until=20)

# --8<-- [start:validate]
# The model of each stream type; a record with a missing or mistyped field raises
SCHEMAS = {"event": EventPayload, "telemetry": TelemetryPayload, "label": LabelPayload}
for line in printed.getvalue().splitlines():
    record = json.loads(line)
    SCHEMAS[record["stream_type"]].model_validate(record)
# --8<-- [end:validate]

kinds = {json.loads(line)["stream_type"] for line in printed.getvalue().splitlines()}
assert kinds == {"event", "telemetry", "label"}, kinds

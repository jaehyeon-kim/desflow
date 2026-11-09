"""The arguments the builder takes for the keys of a blueprint.

tests/docs runs this file and checks the records it prints. Every run in it is
unpaced and short.
"""

from datetime import datetime, timezone

from desflow import NO_RECORD, ConsoleEgress, SimulationContext
from desflow.values import Template, generator

# --8<-- [start:arrival]
app = SimulationContext(
    sim_id="Shop",
    factor=0.0,
    random_seed=42,
    logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
)
app.add_egress(ConsoleEgress())
app.add_resource("packers", current_cap=1, max_cap=2)
app.add_service("packing", dist="constant", value=20)
app.add_arrival(
    "orders",
    gap="30 s",  # a number, a duration, an expression or a generator
    first_at=5,  # the first gap ends at 5 s; a time in the past arrives at once
    max_events=4,  # stops itself after the fourth start; needs a spawn
    vars={"size": generator({"gen": "integer", "bounds": [1, 5]})},  # read by key
    key=generator({"gen": "sequence", "format": "order-{}"}),
    attributes={"size": generator({"gen": "var", "name": "size"})},
    spawn="pack_order",
)
# --8<-- [end:arrival]

# --8<-- [start:task]
payload = Template(
    {"size": {"gen": "attr", "name": "size"}, "shift": {"gen": "var", "name": "shift"}},
    vars={"label": {"gen": "uuid"}},  # drawn for each instance of the task
    vars_once={
        "shift": {"gen": "one_of", "choices": ["early", "late"]}
    },  # for each entity
)


@app.task(
    service_id="packing",
    resource_id="packers",
    history=100,  # rows kept for a Template or an aggregate that looks this task up
    next="record",
)
def pack_order(task_id: int):
    return payload


@app.task()
def record(task_id: int, upstream):
    return NO_RECORD  # the task runs and publishes no finished record


# --8<-- [end:task]

app.run(until="3 min")

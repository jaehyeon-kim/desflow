"""The builder arguments for holding, leaving a line, signals and families.

tests/docs runs this file and checks the records it prints. Every run in it is
unpaced and short.
"""

from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext

app = SimulationContext(
    sim_id="Shop",
    factor=0.0,
    random_seed=42,
    logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
)
app.add_egress(ConsoleEgress())

# --8<-- [start:holders]
app.add_resource("bench", current_cap=1, max_cap=1, queue="priority")
# One desk for each key: desk[0] and desk[1], made when the run starts
app.add_resource("desk", current_cap=1, family=True, members=2)
app.add_signal("closing", once=True, defused=True)
app.add_variable("closings_seen", 0)
# --8<-- [end:holders]
app.add_arrival("orders", dist="constant", value=10, spawn="pack", max_events=3)


# --8<-- [start:tasks]
@app.task(
    # A name, or a mapping with priority, preempt and patience
    seize={"resource": "bench", "priority": 1, "preempt": "f", "patience": 15},
    service_time=18,
    release="bench",  # frees the oldest unit of bench the flow holds
    # A request for desk[1], opened as a handle, and cancelled again
    open={"spare": {"request": {"of": "desk", "key": 1}}},
    cancel="spare",
    on_renege="gave_up",  # the task a flow goes to after it leaves the line
    listen={
        "signal": "closing",
        "on": [{"path": "Shop.variables.closings_seen", "value": 1}],
    },
)
def pack(task_id: int):
    return {"event_type": "packed"}


@app.task()
def gave_up(task_id: int, upstream):
    return {"event_type": "left", "key": upstream.key}


# --8<-- [end:tasks]

app.run(until="2 min")

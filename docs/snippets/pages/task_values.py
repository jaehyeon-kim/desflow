"""The Task Flow page: the key of a task's records, the values a task draws, values
read from earlier records, and values carried to the next task, in the Builder.

tests/docs runs this file: the model runs unpaced for 150 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=2, max_cap=4)
    .add_service("packing", dist="constant", value=1.5)
    # A sign-up every 10 s on average, an order every 2 s on average, and the end of the day
    .add_arrival("signups", dist="exponential", rate=0.1, spawn="sign_up")
    .add_arrival("orders", dist="exponential", rate=0.5, spawn="place_order")
    .add_arrival("closing", dist="constant", value=100, spawn="close_shop")
    # Sign-ups after closing, which start when close_shop turns the source on
    .add_arrival("night", dist="exponential", rate=0.05, active=False, spawn="sign_up")
)


# --8<-- [start:key]
customer = Template(
    {"customer_id": {"gen": "sequence", "format": "c-{}"}}, name="tasks.sign_up"
)


# The finished record carries the customer id as its key. A builder task keeps the
# rows that a lookup reads only when it asks, with history
@app.task(key_field="customer_id", history=1000)
def sign_up(task_id):
    return customer


# --8<-- [end:key]


# --8<-- [start:earlier]
order = Template(
    {
        # A customer who signed up earlier
        "customer_id": {"gen": "lookup", "task": "sign_up", "path": "customer_id"},
        "total": {"gen": "var", "name": "total"},
        # This stream's last running_total plus this order's total
        "running_total": {
            "gen": "add",
            "args": [
                {"gen": "previous", "path": "running_total", "default": 0},
                {"gen": "var", "name": "total"},
            ],
        },
    },
    name="tasks.place_order",
)


# A value drawn here is read by name below
@app.task(
    history=10,
    vars={"total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2}},
    next="pack",
)
def place_order(task_id):
    return order


# --8<-- [end:earlier]


# --8<-- [start:draws]
packed = Template(
    {
        "box": {"gen": "var", "name": "box"},
        "weight": {"gen": "var", "name": "weight"},
        "fee": {"gen": "var", "name": "fee"},
        # The total of the order that came here
        "total": {"gen": "upstream", "path": "total"},
    },
    name="tasks.pack",
)


@app.task(
    service_id="packing",
    resource_id="packers",
    # Drawn once for each order, so a second packing keeps the box
    vars_once={"box": {"gen": "one_of", "choices": ["small", "large"]}},
    # Drawn for each packing, when it starts because on_start is written
    vars={"weight": {"gen": "uniform", "low": 0.5, "high": 5, "decimals": 1}},
    # Published as one record when the packing starts
    on_start={"weight": {"gen": "var", "name": "weight"}},
    # Drawn after the service, and able to read the values above
    finish_vars={"fee": {"gen": "expr", "expr": "round(weight * 0.4, 2)"}},
    # One packing in ten is done again
    next={"weights": {"pack": 1, "finish": 9}},
)
def pack(task_id):
    return packed


# --8<-- [end:draws]


@app.task()
def finish(task_id):
    return {"event_type": "order_done"}


# --8<-- [start:carry]
report = Template(
    {
        "closed_at": {"gen": "expr", "expr": "arg.closed_at"},
        "mean_total": {
            "gen": "aggregate",
            "task": "place_order",
            "path": "total",
            "last": 10,
            "function": "mean",
        },
    },
    name="tasks.day_report",
)


@app.task(
    # Written now, and read as arg.closed_at by this task and the tasks after it
    set_args={"closed_at": {"gen": "expr", "expr": "t"}},
    # Turn on the source of the night sign-ups, as the task finishes
    activate="night",
    next="day_report",
)
def close_shop(task_id):
    return {"event_type": "shop_closed"}


@app.task()
def day_report(task_id):
    return report


# --8<-- [end:carry]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(until=150)

"""The Builder API page: a whole simulation in one Python file, step by step.

Run it with `python builder.py`: it runs unpaced for 2 simulation minutes and prints
every record to the terminal.
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, LocalIngress, SimulationContext

# --8<-- [start:clock]
app = SimulationContext(
    sim_id="Shop",
    factor=0.0,  # 0.0 unpaced, 1.0 real time
    random_seed=42,  # the same draws on every run
    logical_start_time=datetime(2026, 10, 1, 9, 0),  # what simulation time 0 is
)
# --8<-- [end:clock]

# --8<-- [start:resources]
app.add_resource("packers", current_cap=2, max_cap=5)  # 2 packers now, room for 5
app.add_container("boxes", current_cap=200, max_cap=200, level=100)  # an amount
app.add_store("parcels", current_cap=20, max_cap=50)  # items, waiting for a courier
app.add_variable("conveyor_speed", 1.5)  # any other live value
# --8<-- [end:resources]

# --8<-- [start:distributions]
app.add_arrival("orders", dist="exponential", rate=0.5)  # an order every 2 s on average
app.add_arrival("couriers", dist="constant", value=10)  # a courier every 10 s
app.add_service("packing", dist="triangular", low=1, mode=2, high=4)
app.add_service("labelling", dist="constant", value=0.5)
# --8<-- [end:distributions]


# --8<-- [start:tasks]
@app.task(
    service_id="packing",
    resource_id="packers",
    get={"container": "boxes", "amount": 1},  # one box, once it has a packer
    next="print_label",  # the task that follows
)
def pack_order(task_id: int):
    # The value of the finished event: exactly this dictionary
    return {"event_type": "order_packed"}


@app.task(service_id="labelling", put={"store": "parcels"})  # a timed step
def print_label(task_id: int, upstream):
    # upstream is what pack_order handed on; the payload becomes the parcel
    return {"event_type": "label_printed", "packed_by": upstream.task_id}


@app.task(get={"store": "parcels"})
def collect(task_id: int, item):
    return {"event_type": "parcel_collected", "label": item["event_type"]}


# --8<-- [end:tasks]


# --8<-- [start:arrivals]
@app.arrival_loop("orders")
def orders(context: SimulationContext):
    task_id = 0
    while True:
        order = yield context.wait_for_arrival("orders")  # keyed orders-1, ...
        context.spawn(pack_order(task_id, entity=order))
        task_id += 1


@app.arrival_loop("couriers")
def couriers(context: SimulationContext):
    task_id = 0
    while True:
        courier = yield context.wait_for_arrival("couriers")
        context.spawn(collect(task_id, entity=courier))
        task_id += 1


# --8<-- [end:arrivals]

# --8<-- [start:stats]
app.with_stats("typical", interval=30)  # readings, counts, waits, every 30 s
# --8<-- [end:stats]


# --8<-- [start:process]
def speed_drift(context: SimulationContext, step: float):
    """Raises the conveyor speed a little every 10 simulation seconds."""
    path = "Shop.variables.conveyor_speed"
    while True:
        yield context.env.timeout(10.0)
        speed = context.env.registry.get(path).value
        context.env.registry.update(path, speed + step)


app.add_process(speed_drift, step=0.1)
# --8<-- [end:process]

# --8<-- [start:connectors]
app.add_ingress(LocalIngress(schedule=[(5.0, "Shop.resources.packers.current_cap", 3)]))
app.add_egress(ConsoleEgress(), batch_size=100, flush_interval=1.0)
app.with_batching(batch_size=1_000, flush_interval=5.0)  # defaults for other sinks
# --8<-- [end:connectors]

# --8<-- [start:compile]
paths = app.compile_parameters()  # the SimParameter that run() registers
# --8<-- [end:compile]

# --8<-- [start:run]
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="2 min")
# --8<-- [end:run]

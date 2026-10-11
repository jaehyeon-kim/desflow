import logging

import simpy

from dynamic_des import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)

MACHINING = 5.0  # seconds of work per part
REPAIR = 10.0  # seconds the lathe is down after a breakdown
BREAKDOWN_EVERY = 12.0  # seconds between breakdowns

app = (
    SimulationContext(sim_id="Line_A", factor=1.0)
    .add_resource("lathe", current_cap=1, max_cap=1)
    .add_arrival("standard", dist="exponential", rate=0.2)
    .add_egress(ConsoleEgress())
)

# The process of the part being machined, if any, so a breakdown can interrupt it.
on_lathe = []


# --8<-- [start:worker]
def worker_task(context: SimulationContext, task_id: int):
    lathe = context.get_resource("lathe")
    key = f"task-{task_id}"
    context.env.publish_event(key, {"status": "queued"})

    with lathe.request() as req:
        yield req
        context.env.publish_event(key, {"status": "started"})
        remaining = MACHINING
        while remaining > 0:
            on_lathe.append(context.env.active_process)
            began = context.env.now
            try:
                yield context.env.timeout(remaining)
                remaining = 0
            except simpy.Interrupt as interrupt:
                remaining -= context.env.now - began
                context.env.publish_event(
                    key, {"status": "interrupted", "cause": interrupt.cause}
                )
            on_lathe.remove(context.env.active_process)
            if remaining > 0:
                # The part stays on the lathe, so no other part can start, and a
                # lathe under repair cannot break down again.
                yield context.env.timeout(REPAIR)
                context.env.publish_event(key, {"status": "machine_repaired"})
        context.env.publish_event(key, {"status": "finished"})
# --8<-- [end:worker]


# --8<-- [start:breakdowns]
def chaos_monkey(context: SimulationContext):
    while True:
        yield context.env.timeout(BREAKDOWN_EVERY)
        if on_lathe:  # an idle lathe has nothing to interrupt
            on_lathe[0].interrupt("overheating")
# --8<-- [end:breakdowns]


@app.arrival_loop("standard")
def arrival_loop(context: SimulationContext):
    context.spawn(chaos_monkey(context))
    task_id = 0
    while True:
        yield context.wait_for_arrival("standard")
        context.spawn(worker_task(context, task_id))
        task_id += 1


if __name__ == "__main__":
    app.run(until=40.0)

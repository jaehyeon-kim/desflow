import logging

import simpy

from dynamic_des import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)

LABELLING = 5.0  # seconds to print and attach the shipping label of one order
REPAIR = 10.0  # seconds the printer is down after a jam
JAM_EVERY = 12.0  # seconds between jams

app = (
    SimulationContext(sim_id="Shop", factor=1.0)
    .add_resource("printer", current_cap=1, max_cap=1)
    .add_arrival("orders", dist="exponential", rate=0.2)
    .add_egress(ConsoleEgress())
)

# The process of the order being labelled, if any, so a jam can interrupt it.
on_printer = []


# --8<-- [start:worker]
def worker_task(context: SimulationContext, task_id: int):
    printer = context.get_resource("printer")
    key = f"task-{task_id}"
    context.env.publish_event(key, {"status": "queued"})

    with printer.request() as req:
        yield req
        context.env.publish_event(key, {"status": "started"})
        remaining = LABELLING
        while remaining > 0:
            on_printer.append(context.env.active_process)
            began = context.env.now
            try:
                yield context.env.timeout(remaining)
                remaining = 0
            except simpy.Interrupt as interrupt:
                remaining -= context.env.now - began
                context.env.publish_event(
                    key, {"status": "interrupted", "cause": interrupt.cause}
                )
            on_printer.remove(context.env.active_process)
            if remaining > 0:
                # The order stays on the printer, so no other order can start, and a
                # printer under repair cannot jam again.
                yield context.env.timeout(REPAIR)
                context.env.publish_event(key, {"status": "printer_repaired"})
        context.env.publish_event(key, {"status": "finished"})


# --8<-- [end:worker]


# --8<-- [start:breakdowns]
def chaos_monkey(context: SimulationContext):
    while True:
        yield context.env.timeout(JAM_EVERY)
        if on_printer:  # an idle printer has nothing to interrupt
            on_printer[0].interrupt("paper_jam")


# --8<-- [end:breakdowns]


@app.arrival_loop("orders")
def arrival_loop(context: SimulationContext):
    context.spawn(chaos_monkey(context))
    task_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(worker_task(context, task_id))
        task_id += 1


if __name__ == "__main__":
    app.run(until=40.0)

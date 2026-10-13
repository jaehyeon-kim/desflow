# Preemptive Breakdowns

A preemptive breakdown stops a machine in the middle of a job. Here a label printer jams while it prints an order's shipping label. The order stays on the printer while it is repaired, then the rest of its work continues. No other order can start while the printer is down.

![worker_task publishes queued and started; every 12 s chaos_monkey interrupts the order on the printer, which publishes interrupted with the cause, keeps the printer for 10 s of repair, publishes printer_repaired and labels the rest, then publishes finished; with no jam it finishes after 5 s of labelling](../assets/diagrams/breakdown-interrupt.png)

`@app.task` waits out the service time inside its own generator, so your code cannot catch a `simpy.Interrupt` there. A breakdown needs a task written as a generator that you start with `context.spawn`, and a process that interrupts it.

---

## Example

```python title="docs/snippets/guides/preemptive_breakdowns.py"
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


def chaos_monkey(context: SimulationContext):
    while True:
        yield context.env.timeout(JAM_EVERY)
        if on_printer:  # an idle printer has nothing to interrupt
            on_printer[0].interrupt("paper_jam")


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
```

Save it as `preemptive_breakdowns.py` and run `python preemptive_breakdowns.py`. It runs for 40 seconds in real time (`factor=1.0`) and prints each event.

---

## How It Works

- **`on_printer`** holds the process of the order being labelled, so the breakdown process knows what to interrupt.
- **`worker_task`** takes the printer and labels for `LABELLING` seconds. When it is interrupted, it subtracts the time already worked, publishes `interrupted` with the cause, keeps the printer for `REPAIR` seconds, publishes `printer_repaired` and labels the rest. It publishes `finished` when the work is done.
- **`chaos_monkey`** interrupts the order on the printer every `JAM_EVERY` seconds. An idle printer, or one under repair, has nothing to interrupt, so it does not jam. For jams at random times, replace the fixed wait with a random draw such as `random.expovariate(1 / JAM_EVERY)`.
- **`arrival_loop`** starts `chaos_monkey` once, then starts a `worker_task` for each arriving order.

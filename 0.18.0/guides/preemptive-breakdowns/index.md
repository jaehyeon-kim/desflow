# Preemptive Breakdowns

A preemptive breakdown stops a machine in the middle of a job. Here the part being machined stays on the lathe while it is repaired, then the rest of its work continues. No other part can start while the lathe is down.

`@app.task` waits out the service time inside its own generator, so your code cannot catch a `simpy.Interrupt` there. A breakdown needs a task written as a generator that you start with `context.spawn`, and a process that interrupts it.

______________________________________________________________________

## Example

docs/snippets/guides/preemptive_breakdowns.py

```python
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


def chaos_monkey(context: SimulationContext):
    while True:
        yield context.env.timeout(BREAKDOWN_EVERY)
        if on_lathe:  # an idle lathe has nothing to interrupt
            on_lathe[0].interrupt("overheating")


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
```

Save it as `preemptive_breakdowns.py` and run `python preemptive_breakdowns.py`. It runs for 40 seconds in real time (`factor=1.0`) and prints each event.

______________________________________________________________________

## How It Works

- **`on_lathe`** holds the process of the part being machined, so the breakdown process knows what to interrupt.
- **`worker_task`** takes the lathe and machines for `MACHINING` seconds. When it is interrupted, it subtracts the time already worked, publishes `interrupted` with the cause, keeps the lathe for `REPAIR` seconds, publishes `machine_repaired` and machines the rest. It publishes `finished` when the work is done.
- **`chaos_monkey`** interrupts the part on the lathe every `BREAKDOWN_EVERY` seconds. An idle lathe, or one under repair, has nothing to interrupt, so it does not break down. For breakdowns at random times, replace the fixed wait with a random draw such as `random.expovariate(1 / BREAKDOWN_EVERY)`.
- **`arrival_loop`** starts `chaos_monkey` once, then starts a `worker_task` for each arriving part.

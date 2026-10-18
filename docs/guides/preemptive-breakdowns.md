# Preemptive Breakdowns

**Level:** Advanced

A preemptive breakdown stops a machine in the middle of a job. Here a label printer jams every 12 seconds while it prints orders' shipping labels. The jam takes the printer from the order being labelled for 10 seconds of repair; the order queues again and, once the printer is repaired, labels what it had left. It builds on [Interruptions](../tutorials/interruptions.md), which explains preemption and `on_interrupt`.

---

## Steps

1. Make the printer preemptive: `queue: priority` and `preempt: true`, so a request with a lower priority value takes it from the order being labelled.
2. Model a jam as a task of priority 0 that holds the printer for the repair time. While it holds the printer, no order is labelled.
3. Give the labelling task `on_interrupt: resume`, the default, so a preempted order keeps the time it had left; `restart` would print the label again from the start.

The order publishes `preempted` when the jam takes the printer and `resumed` when it has it back. A preempted order queues again behind the orders that were already waiting, as a new SimPy request does.

---

## Example

```python title="docs/snippets/guides/preemptive_breakdowns.py"
import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)

app = (
    SimulationContext(sim_id="Shop", factor=1.0)
    # A jam takes the printer from the order being labelled: priority 0 preempts 1
    .add_resource("printer", current_cap=1, max_cap=1, queue="priority", preempt=True)
    # 5 s to print and attach the shipping label of one order
    .add_service("labelling", dist="constant", value=5.0)
    # 10 s of repair after a jam
    .add_service("repair", dist="constant", value=10.0)
    .add_arrival("orders", dist="exponential", rate=0.2)
    # A jam every 12 s
    .add_arrival("jams", dist="constant", value=12.0)
    .add_egress(ConsoleEgress())
)


# An interrupted order gives the printer back, queues again, and labels what was
# left once it has the printer back
@app.task(
    service_id="labelling", resource_id="printer", priority=1, on_interrupt="resume"
)
def label(task_id: int):
    return {"event_type": "label_printed"}


# A jam holds the printer for the repair, so no order is labelled meanwhile
@app.task(service_id="repair", resource_id="printer", priority=0)
def jam(task_id: int):
    return {"event_type": "printer_repaired"}


@app.arrival_loop("orders")
def order_loop(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(label(context.next_instance("label"), entity=order))


@app.arrival_loop("jams")
def jam_loop(context: SimulationContext):
    while True:
        breakdown = yield context.wait_for_arrival("jams")
        context.spawn(jam(context.next_instance("jam"), entity=breakdown))


if __name__ == "__main__":
    app.run(until=40.0)
```

Save it as `preemptive_breakdowns.py` and run `python preemptive_breakdowns.py`. It runs for 40 seconds in real time (`factor=1.0`) and prints each event. Each jam stops the printer for 10 seconds, the first from 12 to 22: an order being labelled at that moment publishes `preempted`, and `resumed` once it has the printer again.

A jam here happens on a fixed clock, whether or not the printer is busy. For jams at random times, give the `jams` arrival a distribution such as `exponential`.

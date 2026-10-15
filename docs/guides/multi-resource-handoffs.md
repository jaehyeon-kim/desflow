# Multi-Resource Handoffs

A handoff passes an order from one resource to the next without letting go of the first too early. Here a picker carries each order to a packing bench. The order keeps the picker until the bench is free, so for a moment it holds both, and then it releases the picker.

![The order holds the picker for 1 s of walking and while it waits for the bench; when it reaches the bench it releases the picker, then holds the bench for 4 s of packing](../assets/diagrams/picker-handoff.png)

`@app.task` requests one resource, holds it for the service time and releases it. An order that holds two resources at once, and releases them at different times, needs a task written as a generator that you start with `context.spawn`.

---

## Example

```python title="docs/snippets/guides/multi_resource_handoffs.py"
import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)

WALK = 1.0  # seconds the picker takes to carry an order to the packing bench
PACKING = 4.0  # seconds of packing per order

app = (
    SimulationContext(sim_id="Shop", factor=1.0)
    .add_resource("picker", current_cap=1, max_cap=1)
    .add_resource("bench", current_cap=1, max_cap=1)
    .add_arrival("orders", dist="exponential", rate=1.0)
    .add_egress(ConsoleEgress())
)


def handoff_worker(context: SimulationContext, order_id: int):
    picker = context.get_resource("picker")
    bench = context.get_resource("bench")
    key = f"task-{order_id}"
    context.env.publish_event(key, {"status": "queued"})

    # The picker is released by hand below, not by a `with` block, because the
    # handoff happens in the middle of the bench's block.
    yield picker.request()
    context.env.publish_event(key, {"status": "picked"})
    yield context.env.timeout(WALK)

    # Ask for the bench while still holding the picker.
    with bench.request() as bench_req:
        yield bench_req
        context.env.publish_event(key, {"status": "at_bench"})
        picker.release()  # the order is on the bench, so the picker is free
        yield context.env.timeout(PACKING)
        context.env.publish_event(key, {"status": "finished"})


@app.arrival_loop("orders")
def arrival_loop(context: SimulationContext):
    order_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(handoff_worker(context, order_id))
        order_id += 1


if __name__ == "__main__":
    app.run(until=30.0)
```

Save it as `multi_resource_handoffs.py` and run `python multi_resource_handoffs.py`. It runs for 30 seconds in real time (`factor=1.0`) and prints each event.

---

## How It Works

- **`handoff_worker`** is one order. It publishes `queued`, waits for the picker, publishes `picked` and spends `WALK` seconds on the way to the bench.
- **Asking for the bench** happens inside the picker's hold: the order waits for the bench while it still has the picker. When it gets the bench it publishes `at_bench` and calls `picker.release()`, so the next order can use the picker. It is packed for `PACKING` seconds, publishes `finished` and releases the bench when its `with` block ends.
- **`picker.request()`** is not in a `with` block, because a `with` block would release the picker at its end, after the packing. The picker is released by hand instead.
- **`arrival_loop`** starts a `handoff_worker` for each arriving order. Orders arrive about once a second and packing takes 4 seconds, so the bench is the bottleneck. An order that waits for the bench keeps the picker, and the orders behind it wait for the picker.

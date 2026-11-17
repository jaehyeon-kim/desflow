# Multi-Resource Handoffs

A handoff passes an order from one resource to the next without letting go of the first too early. Here a picker carries each order to a packing bench. The order keeps the picker until the bench is free, so for a moment it holds both, and then it releases the picker. It builds on [Competing Resources](https://jaehyeon.me/desflow/latest/tutorials/competing-resources/index.md), which measures what such a holding costs, and on [Several Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/#several-resources).

______________________________________________________________________

## Steps

1. Give the first task the resource and `keep` it: `pick` holds the picker and, instead of releasing it when it finishes, hands it to the next task.
1. Give the next task the second resource: `pack` waits for the bench while the order still holds the picker, and gives the picker back as soon as it publishes `started`.
1. Acquire the two resources in the same order everywhere. A task that takes the bench first and the picker second would be refused when the model is built, because two orders could then wait for each other for ever.

In YAML the same tasks are `keep: [picker]` on `pick` and `resource: bench` on `pack`; in the low-level API the picker's request is released by the next process instead of a `with` block.

______________________________________________________________________

## Example

```python
import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)

app = (
    SimulationContext(sim_id="Shop", factor=1.0)
    .add_resource("picker", current_cap=1, max_cap=1)
    .add_resource("bench", current_cap=1, max_cap=1)
    # The picker takes 1 s to carry an order to the bench, and packing takes 4 s
    .add_service("walking", dist="constant", value=1.0)
    .add_service("packing", dist="constant", value=4.0)
    .add_arrival("orders", dist="exponential", rate=1.0)
    .add_egress(ConsoleEgress())
)


# The picker carries the order to the bench and stays with it until it is on the
# bench: keep hands the picker to the next task instead of releasing it
@app.task(service_id="walking", resource_id="picker", keep=["picker"], next="pack")
def pick(task_id: int):
    return {"event_type": "order_picked"}


# pack gives the picker back as soon as it publishes started
@app.task(service_id="packing", resource_id="bench")
def pack(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def arrival_loop(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pick(context.next_instance("pick"), entity=order))


if __name__ == "__main__":
    app.run(until=30.0)
```

Save it as `multi_resource_handoffs.py` and run `python multi_resource_handoffs.py`. It runs for 30 seconds in real time (`factor=1.0`) and prints each event. Orders arrive about once a second and packing takes 4 seconds, so the bench is the bottleneck: an order that waits for the bench keeps the picker, and the orders behind it wait for the picker.

A task that needs both resources for the whole of its work lists them instead, `resource_id=["picker", "bench"]`; the `competing-resources` example on [Several Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/#example-competing-resources) has both kinds.

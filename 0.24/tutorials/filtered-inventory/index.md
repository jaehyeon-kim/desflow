# Filtered Inventory

The shop now sells shirts in three sizes, small, medium and large. Shirts of every size go on one shelf, a shirt every 10 seconds on average, and an order every 10 seconds asks for one shirt of its own size. A customer cancels an order not filled within 10 minutes. Half the orders are medium, but only 4 shirts in 10 are. This page asks: **which waiting order can use the shirt that arrives, and which orders go unfilled?** It runs a working day of 8 hours.

## What You Add

A `shelf` store, and a `filter` on the order's `get` that picks a shirt of the order's size. The order task has no resource: it waits only for its shirt, with the [Patience](https://jaehyeon.me/desflow/latest/tutorials/patience/index.md) of 10 minutes covering that wait.

______________________________________________________________________

## One Shelf of Items

A store holds items, each a value of its own; here each shirt is a mapping such as `{size: M}`, put on the shelf by a restock task. [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/index.md) describes stores:

```yaml
stores:
  # Shirts of every size on one shelf, each item a mapping such as {size: M}
  shelf: {current_cap: 1000, max_cap: 1000}
```

```python
# Shirts of every size on one shelf, each item a mapping such as {"size": "M"}
app.add_store("shelf", current_cap=1000, max_cap=1000)
```

```python
# Shirts of every size on one shelf, each item a mapping such as {"size": "M"}
stores={"shelf": CapacityConfig(current_cap=1000, max_cap=1000)},
```

The shirts and the orders arrive from six arrivals, one per size, each with a `size` attribute. The YAML is below; the builder and low-level versions add the same six arrivals, and their full files are at the end of the page:

```yaml
# A shirt every 10 seconds: 3 in 10 small, 4 medium, 3 large
shirts_s: {dist: exponential, rate: 0.03, spawn: restock, attributes: {size: S}}
shirts_m: {dist: exponential, rate: 0.04, spawn: restock, attributes: {size: M}}
shirts_l: {dist: exponential, rate: 0.03, spawn: restock, attributes: {size: L}}
# An order every 10 seconds: a quarter small, half medium, a quarter large
orders_s: {dist: exponential, rate: 0.025, spawn: take_shirt, attributes: {size: S}}
orders_m: {dist: exponential, rate: 0.05, spawn: take_shirt, attributes: {size: M}}
orders_l: {dist: exponential, rate: 0.025, spawn: take_shirt, attributes: {size: L}}
```

______________________________________________________________________

## Take the Right Item

A plain `get` takes the oldest item, whatever it is. A `get` with a `filter` takes the oldest item the filter accepts and waits while none does. In SimPy this is a `FilterStore`, whose `get` takes a function of the item. In desflow the filter is a condition in which `item.<field>` reads the item offered, beside the other names a condition reads, such as `attr.size` for the order's own size:

```yaml
take_shirt:
  # Takes the oldest shirt of the order's own size; waits while there is none
  get: {store: shelf, filter: item.size == attr.size}
  # A customer cancels an order not filled within 10 minutes
  patience: 10 min
  # The shirt the get took, in the finished event
  item_field: shirt
  payload: {event_type: order_filled}
```

`item_field: shirt` adds the shirt the get took to the finished event's payload under the key `shirt`, as `id_field` adds the task id under the key it names ([YAML Format](https://jaehyeon.me/desflow/latest/reference/yaml-format/#tasks) lists both).

```python
@app.task(
    # Takes the oldest shirt of the order's own size; waits while there is none
    get={"store": "shelf", "filter": "item.size == attr.size"},
    # A customer cancels an order not filled within 10 minutes
    patience="10 min",
)
def take_shirt(task_id: int, item):
    # `item` is the shirt the get took
    return {"event_type": "order_filled", "shirt": item}
```

The builder also takes a function of the item, such as `"filter": lambda shirt: shirt["size"] == "M"`.

```python
def take_shirt(env, shelf, key, size, task_id):
    def publish(value):
        env.publish_event(key, value, task="take_shirt", instance=task_id)

    publish({"status": "queued"})
    # Takes the oldest shirt of the order's own size; waits while there is none
    get = shelf.get(filter=lambda shirt: shirt["size"] == size)
    # A customer cancels an order not filled within 10 minutes
    yield get | env.timeout(PATIENCE)
    if not get.triggered:
        get.cancel()  # out of the queue, so it never takes a shirt later
        publish({"status": "reneged"})
        return
    publish({"status": "started"})
    publish({"event_type": "order_filled", "shirt": get.value})
```

`shelf.get(filter=...)` is SimPy's `FilterStore.get`. Cancelling a get that has not succeeded takes it out of the queue, so it never takes a shirt later.

When a shirt arrives, every waiting order is offered it in the order they started waiting, and the first that accepts it takes it. A medium order waiting at the front does not stop a small order behind it from taking a small shirt, as it would if all orders waited in one line for the next shirt.

`patience` covers the wait for the get, as it covers a wait for a resource. A get that gives up takes nothing later: the shirt that would have filled it stays on the shelf for the next order.

______________________________________________________________________

## Result

Which waiting order can use the shirt that arrives? The oldest order of the shirt's size, and no other. Over 8 hours, **315** medium orders are cancelled, and no small or large one, because medium orders are half the demand and medium shirts only 4 in 10:

The Builder and Low-level scripts print the counts:

```text
size S:  732 filled,    0 cancelled
size M: 1110 filled,  315 cancelled
size L:  679 filled,    0 cancelled
```

## Why It Works

A filtered store is one shelf with a queue per kind of item. Medium orders arrive at 0.05 a second and medium shirts at 0.04, so medium orders are short of shirts for the whole day and only the patience limit keeps their queue from growing without end. Small and large shirts arrive a little faster than their orders, so those orders are filled within their patience, and the shirts left over, 248 by the end of the day, wait on the shelf, which has room for 1,000. [SimPy and desflow](https://jaehyeon.me/desflow/latest/des/simpy-and-desflow/#containers-and-stores) compares stores and containers with SimPy's. [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#take-and-add) describes filters and cancelled gets.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Filtered Inventory tutorial: each order waits for a shirt of its own size.
# Run it with: desflow run filtered_inventory.yaml
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42

stores:
  # Shirts of every size on one shelf, each item a mapping such as {size: M}
  shelf: {current_cap: 1000, max_cap: 1000}

arrivals:
  # A shirt every 10 seconds: 3 in 10 small, 4 medium, 3 large
  shirts_s: {dist: exponential, rate: 0.03, spawn: restock, attributes: {size: S}}
  shirts_m: {dist: exponential, rate: 0.04, spawn: restock, attributes: {size: M}}
  shirts_l: {dist: exponential, rate: 0.03, spawn: restock, attributes: {size: L}}
  # An order every 10 seconds: a quarter small, half medium, a quarter large
  orders_s: {dist: exponential, rate: 0.025, spawn: take_shirt, attributes: {size: S}}
  orders_m: {dist: exponential, rate: 0.05, spawn: take_shirt, attributes: {size: M}}
  orders_l: {dist: exponential, rate: 0.025, spawn: take_shirt, attributes: {size: L}}

tasks:
  restock:
    # The shirt goes on the shelf, as a mapping with its size
    put: {store: shelf}
    payload: {}
    attributes: [size]
  take_shirt:
    # Takes the oldest shirt of the order's own size; waits while there is none
    get: {store: shelf, filter: item.size == attr.size}
    # A customer cancels an order not filled within 10 minutes
    patience: 10 min
    # The shirt the get took, in the finished event
    item_field: shirt
    payload: {event_type: order_filled}

egress:
  - type: Console

run:
  until: 8 h
```

```python
"""The Filtered Inventory tutorial: each order waits for a shirt of its own size.

Shirts of three sizes arrive on one shelf, and orders of three sizes take them.
Every record is printed to the terminal, and the script prints, for each size, how
many orders were filled and how many were cancelled: python filtered_inventory.py
2>/dev/null shows only those lines.
"""

import logging
from collections import Counter

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# A shirt every 10 seconds: 3 in 10 small, 4 medium, 3 large
SHIRTS = {"S": 0.03, "M": 0.04, "L": 0.03}
# An order every 10 seconds: a quarter small, half medium, a quarter large
ORDERS = {"S": 0.025, "M": 0.05, "L": 0.025}


def shop() -> SimulationContext:
    """The shop with one shelf of shirts and orders of three sizes."""
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42).add_egress(
        ConsoleEgress()
    )
    # Shirts of every size on one shelf, each item a mapping such as {"size": "M"}
    app.add_store("shelf", current_cap=1000, max_cap=1000)
    for size, rate in SHIRTS.items():
        name = f"shirts_{size.lower()}"
        app.add_arrival(name, dist="exponential", rate=rate, attributes={"size": size})
    for size, rate in ORDERS.items():
        name = f"orders_{size.lower()}"
        app.add_arrival(name, dist="exponential", rate=rate, attributes={"size": size})

    # The shirt goes on the shelf, as a mapping with its size
    @app.task(put={"store": "shelf"})
    def restock(task_id: int, entity):
        return {"size": entity.attributes["size"]}

    @app.task(
        # Takes the oldest shirt of the order's own size; waits while there is none
        get={"store": "shelf", "filter": "item.size == attr.size"},
        # A customer cancels an order not filled within 10 minutes
        patience="10 min",
    )
    def take_shirt(task_id: int, item):
        # `item` is the shirt the get took
        return {"event_type": "order_filled", "shirt": item}


    def spawn(arrival: str, task, name: str):
        def loop(context: SimulationContext):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)

    for size in SHIRTS:
        spawn(f"shirts_{size.lower()}", restock, "restock")
    for size in ORDERS:
        spawn(f"orders_{size.lower()}", take_shirt, "take_shirt")
    return app


class _Outcomes:
    """An egress that counts each order's outcome, by size, from its events."""

    def __init__(self):
        self.counts = Counter()

    async def run(self, egress_queue):
        import asyncio
        import queue

        try:
            while True:
                try:
                    for record in egress_queue.get_nowait():
                        count(self.counts, record)
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


def count(counts: Counter, record: dict) -> None:
    """Counts a filled or cancelled order, by the size in its key."""
    if record["stream_type"] != "event" or record["task"] != "take_shirt":
        return
    size = record["key"].split("-")[0][-1].upper()
    value = record["value"]
    if value.get("event_type") == "order_filled":
        counts[(size, "filled")] += 1
    elif value.get("status") == "reneged":
        counts[(size, "cancelled")] += 1


def outcomes() -> Counter:
    """Runs eight hours and counts filled and cancelled orders by size."""
    app = shop()
    outcome = _Outcomes()
    app.add_egress(outcome)
    app.run(until="8 h")
    return outcome.counts


if __name__ == "__main__":
    counts = outcomes()
    for size in ORDERS:
        print(
            f"size {size}: {counts[(size, 'filled')]:4} filled, "
            f"{counts[(size, 'cancelled')]:4} cancelled"
        )
```

```python
"""The Filtered Inventory tutorial: each order waits for a shirt of its own size.

Shirts of three sizes arrive on one shelf, and orders of three sizes take them.
Every record is printed to the terminal, and the script prints, for each size, how
many orders were filled and how many were cancelled:
python filtered_inventory_low_level.py 2>/dev/null shows only those lines.
"""

import itertools
import logging
from collections import Counter

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicStore,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# A shirt every 10 seconds: 3 in 10 small, 4 medium, 3 large
SHIRTS = {"S": 0.03, "M": 0.04, "L": 0.03}
# An order every 10 seconds: a quarter small, half medium, a quarter large
ORDERS = {"S": 0.025, "M": 0.05, "L": 0.025}
PATIENCE = 600  # a customer cancels an order not filled within 10 minutes

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        **{
            f"shirts_{s.lower()}": DistributionConfig(dist="exponential", rate=r)
            for s, r in SHIRTS.items()
        },
        **{
            f"orders_{s.lower()}": DistributionConfig(dist="exponential", rate=r)
            for s, r in ORDERS.items()
        },
    },
    # Shirts of every size on one shelf, each item a mapping such as {"size": "M"}
    stores={"shelf": CapacityConfig(current_cap=1000, max_cap=1000)},
)


def restock(env, shelf, key, size, task_id):
    """The shirt goes on the shelf, as a mapping with its size."""
    yield shelf.put({"size": size})
    env.publish_event(key, {"size": size}, task="restock", instance=task_id)


def take_shirt(env, shelf, key, size, task_id):
    def publish(value):
        env.publish_event(key, value, task="take_shirt", instance=task_id)

    publish({"status": "queued"})
    # Takes the oldest shirt of the order's own size; waits while there is none
    get = shelf.get(filter=lambda shirt: shirt["size"] == size)
    # A customer cancels an order not filled within 10 minutes
    yield get | env.timeout(PATIENCE)
    if not get.triggered:
        get.cancel()  # out of the queue, so it never takes a shirt later
        publish({"status": "reneged"})
        return
    publish({"status": "started"})
    publish({"event_type": "order_filled", "shirt": get.value})


def generator(env, sampler, shelf, arrival, size, task, ids):
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(task(env, shelf, f"{arrival}-{number}", size, next(ids)))


class _Outcomes:
    """An egress that counts each order's outcome, by size, from its events."""

    def __init__(self):
        self.counts = Counter()

    async def run(self, egress_queue):
        import asyncio
        import queue

        try:
            while True:
                try:
                    for record in egress_queue.get_nowait():
                        count(self.counts, record)
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


def count(counts: Counter, record: dict) -> None:
    """Counts a filled or cancelled order, by the size in its key."""
    if record["stream_type"] != "event" or record["task"] != "take_shirt":
        return
    size = record["key"].split("-")[0][-1].upper()
    value = record["value"]
    if value.get("event_type") == "order_filled":
        counts[(size, "filled")] += 1
    elif value.get("status") == "reneged":
        counts[(size, "cancelled")] += 1


def run(until=8 * 3600, factor=0.0, sinks=()):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress(), *sinks])
    shelf = DynamicStore(env, "Shop", "shelf")
    sampler = Sampler(seed=42)
    restocks, takes = itertools.count(), itertools.count()
    for size in SHIRTS:
        arrival = f"shirts_{size.lower()}"
        env.process(generator(env, sampler, shelf, arrival, size, restock, restocks))
    for size in ORDERS:
        arrival = f"orders_{size.lower()}"
        env.process(generator(env, sampler, shelf, arrival, size, take_shirt, takes))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def outcomes() -> Counter:
    """Runs eight hours and counts filled and cancelled orders by size."""
    outcome = _Outcomes()
    run(sinks=[outcome])
    return outcome.counts


if __name__ == "__main__":
    counts = outcomes()
    for size in ORDERS:
        print(
            f"size {size}: {counts[(size, 'filled')]:4} filled, "
            f"{counts[(size, 'cancelled')]:4} cancelled"
        )
```

Next, [Changing Demand](https://jaehyeon.me/desflow/latest/tutorials/changing-demand/index.md) asks what a rush of orders does to waits.

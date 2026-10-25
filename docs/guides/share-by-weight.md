# Share Work by Weight

Share one resource between classes of work by weight, so that a class of weight 3 is served three times for each time a class of weight 1 is served while both wait. It builds on [Custom Resource Types](../user-guide/resources/custom-resource-types.md), which shows how to subclass `ResourceBase`, and on [Priority and Preemption](../user-guide/resources/priority-and-preemption.md). A priority queue serves the lowest priority value first, so a class with a higher value waits for as long as a class with a lower value has work. A weight gives each class its share instead.

---

## Choose the Weights

Each class has a weight. Here tier 1 weighs 3 and tier 2 weighs 1:

```python title="docs/snippets/yaml/share_by_weight_logic.py:weights"
# Tier 1 is served three times for each time tier 2 is served
WEIGHTS = {1: 3, 2: 1}
```

---

## Order the Waiting Line

A resource keeps its waiting requests in its `PutQueue`. `ResourceBase` creates the queue with no arguments, and a request joins it with `append`. Write a queue that gives each request a finish tag and keeps the requests sorted by it. A request's finish tag is the later of the queue's clock and its class's last tag, plus 1 divided by the class's weight. A class of weight 3 therefore gets tags a third as far apart as a class of weight 1. The queue serves the smallest tag first, so the class of weight 3 is served three times as often. Requests with equal tags leave in the order they arrived, which the counter `arrived` keeps. When a request is granted, the queue takes its tag as the clock:

```python title="docs/snippets/yaml/share_by_weight_logic.py:queue"
class FinishTagQueue(list):
    """Waiting requests in the order of their finish tags, smallest first.

    A request's tag is where its class would finish it if every class had its share:
    the later of the queue's clock and the class's last tag, plus 1 divided by the
    class's weight. Equal tags leave in the order the requests arrived.
    """

    def __init__(self):
        super().__init__()
        self.clock = Fraction(0)  # the tag of the request granted last
        self.last = {}  # the last tag given to each class
        self.arrived = itertools.count()

    def append(self, request):
        tier = request.priority  # set by `priority: attr.tier`, not by _rank
        start = max(self.clock, self.last.get(tier, Fraction(0)))
        tag = start + Fraction(1, WEIGHTS[tier])
        self.last[tier] = tag
        request.tag = (tag, next(self.arrived))
        bisect.insort(self, request, key=lambda waiting: waiting.tag)

    def pop(self, index=-1):
        request = super().pop(index)
        self.clock = request.tag[0]  # a grant moves the clock to its tag
        return request
```

The class of a request is its `priority`, which the task sets. The queue reads it there, not from the rank `ResourceBase` computes for the request, which this queue does not use. Fractions keep the tags exact, so two tags that should be equal compare as equal. The tags assume that every request holds the packer for the same time, as here, where every order takes 1 second to pack.

---

## Make the Resource

Subclass `ResourceBase` and set `PutQueue`. The resource also keeps the tier of each grant, so a test can read the order:

```python title="docs/snippets/yaml/share_by_weight_logic.py:resource"
class WeightedPackers(ResourceBase):
    """A resource that grants its units by weight and keeps the tier of each grant."""

    PutQueue = FinishTagQueue

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.granted = []

    def _do_put(self, request):
        super()._do_put(request)
        if request.triggered:  # the request was granted just now
            self.granted.append(request.priority)
```

---

## Use It

A task sets the class with its `priority`, and the loader refuses a task priority on a resource that does not set `queue: priority`. So set `queue: priority` beside `type`. This queue orders its waiting requests itself and ignores the order that setting names.

=== "YAML"

    ```yaml title="docs/snippets/yaml/share_by_weight.yaml:resource"
    resources:
      # queue: priority lets a task set a priority; the class orders the waiting requests itself
      packers:
        current_cap: 1
        max_cap: 4
        queue: priority
        type: !python share_by_weight_logic.WeightedPackers
    ```

    The class is in `share_by_weight_logic.py`, beside the blueprint. The task takes the tier from the order's attributes:

    ```yaml title="docs/snippets/yaml/share_by_weight.yaml:task"
    tasks:
      pack_order:
        service: packing
        resource: packers
        priority: attr.tier  # the tier of the order is the class of its request
        payload: {event_type: order_packed}
    ```

    The nine orders arrive at time 0, from `share_by_weight_orders.csv`, one row each:

    ```csv title="docs/snippets/yaml/share_by_weight_orders.csv"
    time,tier
    0,2
    0,1
    0,1
    0,1
    0,1
    0,1
    0,1
    0,2
    0,2
    ```

=== "Builder"

    ```python title="docs/snippets/guides/share_by_weight.py:builder"
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_service("packing", dist="constant", value=1.0)
        .add_arrival_replay(
            "orders", [{"time": 0, "tier": tier} for tier in TIERS], fields=["tier"]
        )
    )
    # queue="priority" lets a task set a priority; the class orders the waiting requests itself
    app.add_resource(
        "packers", current_cap=1, max_cap=4, queue="priority", type=WeightedPackers
    )

    # The tier of the order is the class of its request
    @app.task(service_id="packing", resource_id="packers", priority="attr.tier")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1
    ```

    The class is the one the YAML tab shows.

=== "Low-level"

    ```python title="docs/snippets/guides/share_by_weight.py:low-level"
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            service={"packing": DistributionConfig(dist="constant", value=1.0)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
        )
    )
    packers = WeightedPackers(env, "Shop", "packers", queue="priority")

    def order(env, tier):
        # The tier is the priority of the request
        with packers.request(priority=tier) as request:
            yield request
            yield env.timeout(1.0)

    for tier in TIERS:
        env.process(order(env, tier))
    ```

    The class is the one the YAML tab shows.

---

## Check the Order

The first order is tier 2. It takes the packer at once, with tag 1, and the clock becomes 1. The six orders of tier 1 then get tags 4/3, 5/3, 2, 7/3, 8/3 and 3, and the two of tier 2 get 2 and 3. The smallest tag goes first, and where two tags are equal, tier 1 arrived first. So after the first order the packer grants **1, 1, 1, 2, 1, 1, 1, 2**: three of tier 1 for each of tier 2. First in, first out would grant 1, 1, 1, 1, 1, 1, 2, 2. All three forms grant the order worked out here.

The queue leaves out three things. A request that leaves the queue before it is granted keeps the tag it took, so its class does not get that share back. A class not in `WEIGHTS` raises `KeyError` when its first request joins. Do not set `preempt`: preemption ranks users by priority, here the tier, so a tier 1 order would take the packer from tier 2 whatever the weights.

---

## Example

The example has the Python file with the queue and the resource, the YAML blueprint, and a script. The script runs the YAML, builder and low-level forms and prints the tier of each grant in order. The orders file is the CSV in the YAML tab of Use It. Put the four files in one folder and run `python share_by_weight.py` from it.

```python title="docs/snippets/yaml/share_by_weight_logic.py"
"""Python for share_by_weight.yaml: a packer that shares its time between classes by
weight, as the Share Work by Weight guide builds it."""

import bisect
import itertools
from fractions import Fraction

from desflow import ResourceBase

# Tier 1 is served three times for each time tier 2 is served
WEIGHTS = {1: 3, 2: 1}


class FinishTagQueue(list):
    """Waiting requests in the order of their finish tags, smallest first.

    A request's tag is where its class would finish it if every class had its share:
    the later of the queue's clock and the class's last tag, plus 1 divided by the
    class's weight. Equal tags leave in the order the requests arrived.
    """

    def __init__(self):
        super().__init__()
        self.clock = Fraction(0)  # the tag of the request granted last
        self.last = {}  # the last tag given to each class
        self.arrived = itertools.count()

    def append(self, request):
        tier = request.priority  # set by `priority: attr.tier`, not by _rank
        start = max(self.clock, self.last.get(tier, Fraction(0)))
        tag = start + Fraction(1, WEIGHTS[tier])
        self.last[tier] = tag
        request.tag = (tag, next(self.arrived))
        bisect.insort(self, request, key=lambda waiting: waiting.tag)

    def pop(self, index=-1):
        request = super().pop(index)
        self.clock = request.tag[0]  # a grant moves the clock to its tag
        return request


class WeightedPackers(ResourceBase):
    """A resource that grants its units by weight and keeps the tier of each grant."""

    PutQueue = FinishTagQueue

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.granted = []

    def _do_put(self, request):
        super()._do_put(request)
        if request.triggered:  # the request was granted just now
            self.granted.append(request.priority)


```

```yaml title="docs/snippets/yaml/share_by_weight.yaml"
# The Share Work by Weight guide: one packer shared between two tiers of orders by weight.
# Run it with: desflow run share_by_weight.yaml
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42

resources:
  # queue: priority lets a task set a priority; the class orders the waiting requests itself
  packers:
    current_cap: 1
    max_cap: 4
    queue: priority
    type: !python share_by_weight_logic.WeightedPackers

services:
  packing: {dist: constant, value: 1 s}

# Nine orders at time 0, each with a tier
arrivals:
  orders:
    replay: {path: share_by_weight_orders.csv, time: time, unit: s}
    spawn: pack_order

tasks:
  pack_order:
    service: packing
    resource: packers
    priority: attr.tier  # the tier of the order is the class of its request
    payload: {event_type: order_packed}

egress:
  - type: Console

run:
  until: 20 s
```

```python title="docs/snippets/guides/share_by_weight.py"
"""Share Work by Weight: one packer shared between two tiers of orders, 3 to 1.

Nine orders arrive at time 0: the first is tier 2, then six of tier 1, then two more
of tier 2. The first takes the packer and the rest wait. The packer grants the
waiting orders by weight, three of tier 1 for each of tier 2, in YAML, in the
builder and in the low-level API. Each function returns the tier of each order in
the order the packer granted them.
"""

import logging
import sys
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    SimParameter,
    SimulationContext,
)

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # share_by_weight_logic.py is beside the blueprint
from share_by_weight_logic import WeightedPackers  # noqa: E402

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)
# The tier of each order, in the order the orders arrive
TIERS = [2, 1, 1, 1, 1, 1, 1, 2, 2]


def yaml_form():
    """The blueprint beside the class."""
    app = SimulationContext.from_yaml(HERE / "share_by_weight.yaml")
    app.run()
    return app.get_resource("packers").granted


def builder_form():
    """The same shop with the builder."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_service("packing", dist="constant", value=1.0)
        .add_arrival_replay(
            "orders", [{"time": 0, "tier": tier} for tier in TIERS], fields=["tier"]
        )
    )
    # queue="priority" lets a task set a priority; the class orders the waiting requests itself
    app.add_resource(
        "packers", current_cap=1, max_cap=4, queue="priority", type=WeightedPackers
    )

    # The tier of the order is the class of its request
    @app.task(service_id="packing", resource_id="packers", priority="attr.tier")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    app.run(until=20)
    return app.get_resource("packers").granted


def low_level_form():
    """The same shop with the environment and the resource."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            service={"packing": DistributionConfig(dist="constant", value=1.0)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
        )
    )
    packers = WeightedPackers(env, "Shop", "packers", queue="priority")

    def order(env, tier):
        # The tier is the priority of the request
        with packers.request(priority=tier) as request:
            yield request
            yield env.timeout(1.0)

    for tier in TIERS:
        env.process(order(env, tier))
    env.run(until=20)
    return packers.granted


if __name__ == "__main__":
    for form in (yaml_form, builder_form, low_level_form):
        print(f"{form.__name__:15} {form()}")
```

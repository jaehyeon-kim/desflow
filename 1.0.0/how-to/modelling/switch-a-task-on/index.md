# Switch a Task On Mid-run

Sometimes the work itself changes during a run: from 1 minute, the shop gift wraps some orders after packing. Declare the new task from the start, route to it with a weight read from the registry, and change that weight with a scenario step. The weight starts at 0, so no order reaches the task until the step.

A blueprint and the builder create their tasks before the clock starts and have no setting that adds one later. A task declared from the start publishes nothing while no order is routed to it, so this page needs no process and no Python in the blueprint. To add a resource during a run instead, see [Add Resources Mid-run](https://jaehyeon.me/desflow/latest/how-to/modelling/dynamic-topology/index.md).

______________________________________________________________________

## Before You Start

- [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/#weights) explains `next.weights` and the expressions a weight can be.
- [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/#find-the-path) explains the registry paths, such as `Shop.variables.wrap_weight`.
- [Script an Experiment](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/#script-an-experiment) explains scenario steps.

______________________________________________________________________

## Steps

### Declare the Switch and the New Task

Declare a variable that holds the new task's weight, here `wrap_weight` at 0. Declare the new task's resource and service too, as for any other task: the wrappers have a unit from the start, and it stays idle until the switch.

```yaml
# The switch: a weight of 0 sends no order to wrap_gift
variables: {wrap_weight: 0}

resources:
  packers: {current_cap: 1, max_cap: 1}
  # The new task's resource exists from the start, unused until the switch
  wrappers: {current_cap: 1, max_cap: 1}

services:
  packing: {dist: constant, value: 2 s}
  wrapping: {dist: constant, value: 3 s}
```

```python
# The switch: a weight of 0 sends no order to wrap_gift
.add_variable("wrap_weight", 0)
.add_resource("packers", current_cap=1, max_cap=1)
# The new task's resource exists from the start, unused until the switch
.add_resource("wrappers", current_cap=1, max_cap=1)
.add_service("packing", dist="constant", value="2 s")
.add_service("wrapping", dist="constant", value="3 s")
```

```python
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="constant", value=5.0)},
    service={
        "packing": DistributionConfig(dist="constant", value=2.0),
        "wrapping": DistributionConfig(dist="constant", value=3.0),
    },
    resources={
        "packers": CapacityConfig(current_cap=1, max_cap=1),
        # The new task's resource exists from the start, unused until the switch
        "wrappers": CapacityConfig(current_cap=1, max_cap=1),
    },
    # The switch: a weight of 0 sends no order to wrap_gift
    variables={"wrap_weight": 0},
)
```

### Route to the Task by the Switch

Give the task before it a weight for the new task that reads the variable: `registry["Shop.variables.wrap_weight"]`. The expression is read each time an order is packed, so a change lands on the next order. Before the switch the weights are 3 to 0 and every order ships. After it they are 3 to 1, so one order in four is wrapped on average. In the low-level API, a `Router` with the same rules chooses the next task, and `choose(..., env=env)` gives it the registry.

```yaml
pack_order:
  service: packing
  resource: packers
  payload: {event_type: order_packed}
  next:
    # Read when each order is packed: 3 to 0 before the switch, 3 to 1 after it
    weights: {ship_order: 3, wrap_gift: 'registry["Shop.variables.wrap_weight"]'}
wrap_gift:
  service: wrapping
  resource: wrappers
  payload: {event_type: gift_wrapped}
  next: ship_order
```

```python
@app.task(
    service_id="packing",
    resource_id="packers",
    # Read when each order is packed: 3 to 0 before the switch, 3 to 1 after it
    next={
        "weights": {
            "ship_order": 3,
            "wrap_gift": 'registry["Shop.variables.wrap_weight"]',
        }
    },
)
def pack_order(task_id: int):
    return {"event_type": "order_packed"}

@app.task(service_id="wrapping", resource_id="wrappers", next="ship_order")
def wrap_gift(task_id: int):
    return {"event_type": "gift_wrapped"}
```

```python
# Read when each order is packed: 3 to 0 before the switch, 3 to 1 after it
ROUTE = {
    "weights": {"ship_order": 3, "wrap_gift": 'registry["Shop.variables.wrap_weight"]'}
}
```

### Turn It On With a Scenario Step

A scenario step sets the variable at 1 minute. Its `record` publishes what it did at that time, as a ground-truth record to a sink with `when: labels`. To retire the old route as well, give `ship_order` a weight that reads a second variable, and set that variable to 0 with another step.

```yaml
# At 1 minute wrapping starts, and the step records what it did
scenario:
  - at: 1 min
    path: Shop.variables.wrap_weight
    value: 1
    record: {intervention: gift_wrapping_on}
```

```python
# At 1 minute wrapping starts, and the step records what it did
app.add_scenario(
    [
        {
            "at": "1 min",
            "path": "Shop.variables.wrap_weight",
            "value": 1,
            "record": {"intervention": "gift_wrapping_on"},
        }
    ]
)
```

```python
# At 1 minute wrapping starts, and the step records what it did
SCHEDULE = [
    {
        "at": 60,
        "path": "Shop.variables.wrap_weight",
        "value": 1,
        "record": {"intervention": "gift_wrapping_on"},
    }
]
```

______________________________________________________________________

## Check the Result

Run `desflow run switch_a_task_on.yaml`, `python switch_a_task_on_builder.py` or `python switch_a_task_on_low_level.py`. Each runs 5 simulated minutes at once, with an order every 5 seconds:

- Before 1 minute, every order goes from `pack_order` to `ship_order`, and no `gift_wrapped` event appears.
- From 1 minute, 48 orders are packed and 9 of them are wrapped, the first `orders-12` at 65 seconds. The weights give one in four on average; this one run of 48 orders wraps 9.
- The wrapped orders are the same in every form, because each draws the route from the routing task's own random stream, `route.pack_order`, with the same seed.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Switch a Task On Mid-run guide: from 1 minute, one order in four is gift wrapped.
# Run it with: desflow run switch_a_task_on.yaml
simulation: {sim_id: Shop, factor: 0, random_seed: 42}

egress:
  - {type: Console}

# The switch: a weight of 0 sends no order to wrap_gift
variables: {wrap_weight: 0}

resources:
  packers: {current_cap: 1, max_cap: 1}
  # The new task's resource exists from the start, unused until the switch
  wrappers: {current_cap: 1, max_cap: 1}

services:
  packing: {dist: constant, value: 2 s}
  wrapping: {dist: constant, value: 3 s}

arrivals:
  orders: {dist: constant, value: 5 s, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}
    next:
      # Read when each order is packed: 3 to 0 before the switch, 3 to 1 after it
      weights: {ship_order: 3, wrap_gift: 'registry["Shop.variables.wrap_weight"]'}
  wrap_gift:
    service: wrapping
    resource: wrappers
    payload: {event_type: gift_wrapped}
    next: ship_order
  ship_order:
    payload: {event_type: order_shipped}

# At 1 minute wrapping starts, and the step records what it did
scenario:
  - at: 1 min
    path: Shop.variables.wrap_weight
    value: 1
    record: {intervention: gift_wrapping_on}

run: {until: 5 min}
```

```python
"""The Switch a Task On Mid-run guide, Builder API: from 1 minute, one order in four
is gift wrapped after packing.

`build()` returns the app. Run it with: python switch_a_task_on_builder.py, which runs
the shop for 5 simulated minutes and prints its events, as switch_a_task_on.yaml does.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(level=logging.INFO)


def build() -> SimulationContext:
    """The shop, with gift wrapping declared from the start and switched on at 1 min."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_egress(ConsoleEgress())
        # The switch: a weight of 0 sends no order to wrap_gift
        .add_variable("wrap_weight", 0)
        .add_resource("packers", current_cap=1, max_cap=1)
        # The new task's resource exists from the start, unused until the switch
        .add_resource("wrappers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value="2 s")
        .add_service("wrapping", dist="constant", value="3 s")
        .add_arrival("orders", dist="constant", value="5 s", spawn="pack_order")
    )

    @app.task(
        service_id="packing",
        resource_id="packers",
        # Read when each order is packed: 3 to 0 before the switch, 3 to 1 after it
        next={
            "weights": {
                "ship_order": 3,
                "wrap_gift": 'registry["Shop.variables.wrap_weight"]',
            }
        },
    )
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.task(service_id="wrapping", resource_id="wrappers", next="ship_order")
    def wrap_gift(task_id: int):
        return {"event_type": "gift_wrapped"}

    @app.task()
    def ship_order(task_id: int):
        return {"event_type": "order_shipped"}

    # At 1 minute wrapping starts, and the step records what it did
    app.add_scenario(
        [
            {
                "at": "1 min",
                "path": "Shop.variables.wrap_weight",
                "value": 1,
                "record": {"intervention": "gift_wrapping_on"},
            }
        ]
    )
    return app


if __name__ == "__main__":
    build().run(until=300)
```

```python
"""The Switch a Task On Mid-run guide, low-level API: from 1 minute, one order in four
is gift wrapped after packing.

The low-level version of switch_a_task_on_builder.py: a Router reads the weight from
the registry each time an order is packed, and `run_scenario` changes it at 1 minute.
python switch_a_task_on_low_level.py runs 5 simulated minutes at once.
"""

import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Router,
    Sampler,
    SimParameter,
    Upstream,
    run_scenario,
)

logging.basicConfig(level=logging.INFO)

UNTIL = 300
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="constant", value=5.0)},
    service={
        "packing": DistributionConfig(dist="constant", value=2.0),
        "wrapping": DistributionConfig(dist="constant", value=3.0),
    },
    resources={
        "packers": CapacityConfig(current_cap=1, max_cap=1),
        # The new task's resource exists from the start, unused until the switch
        "wrappers": CapacityConfig(current_cap=1, max_cap=1),
    },
    # The switch: a weight of 0 sends no order to wrap_gift
    variables={"wrap_weight": 0},
)
# Read when each order is packed: 3 to 0 before the switch, 3 to 1 after it
ROUTE = {
    "weights": {"ship_order": 3, "wrap_gift": 'registry["Shop.variables.wrap_weight"]'}
}
# At 1 minute wrapping starts, and the step records what it did
SCHEDULE = [
    {
        "at": 60,
        "path": "Shop.variables.wrap_weight",
        "value": 1,
        "record": {"intervention": "gift_wrapping_on"},
    }
]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the orders and the scenario on an environment that has PARAMS."""
    sampler = Sampler(seed=42)
    route = Router(ROUTE)
    # The routing task's own stream, as the blueprint and the builder draw from
    route.rng = sampler.generator("route.pack_order")
    shop = {
        "packers": DynamicResource(env, "Shop", "packers"),
        "wrappers": DynamicResource(env, "Shop", "wrappers"),
    }
    ids = {
        name: itertools.count() for name in ("pack_order", "wrap_gift", "ship_order")
    }

    def serve(key, task, service, resource, payload):
        task_id = next(ids[task])
        env.publish_event(key, {"status": "queued"}, task, task_id)
        with shop[resource].request() as request:
            yield request
            env.publish_event(key, {"status": "started"}, task, task_id)
            path = f"Shop.service.{service}"
            yield env.timeout(sampler.sample(env.registry.get_config(path)))
            env.publish_event(key, payload, task, task_id)
        return task_id

    def order(key):
        task_id = yield from serve(
            key, "pack_order", "packing", "packers", {"event_type": "order_packed"}
        )
        packed = Upstream(task_id, key, {}, {"event_type": "order_packed"})
        if route.choose(packed, env=env) == "wrap_gift":
            yield from serve(
                key, "wrap_gift", "wrapping", "wrappers", {"event_type": "gift_wrapped"}
            )
        env.publish_event(
            key, {"event_type": "order_shipped"}, "ship_order", next(ids["ship_order"])
        )

    def orders():
        gap = env.registry.get_config("Shop.arrival.orders")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(order(f"orders-{number}"))

    env.process(orders())
    env.process(run_scenario(env, SCHEDULE, until=UNTIL))


def run(egress=None):
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(egress or [ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
```

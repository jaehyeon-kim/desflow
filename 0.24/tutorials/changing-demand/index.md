# Changing Demand

The shop's one packer packs an order in 1.5 seconds, and orders usually arrive at 0.4 a second. From 20 to 40 minutes a promotion brings 0.9 a second, more than one packer can pack. This page asks: **what does a 20-minute rush do to waits, and does a second packer for the rush keep them short?** It runs the hour with one packer through the rush and with two.

## What You Add

A **schedule**: changes to the arrival rate and the number of packers at set times. The shop is the one from [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/index.md), with packing at a constant 1.5 seconds.

______________________________________________________________________

## Write the Schedule

A schedule is a list of steps, each a registry path, a value and the simulation time it applies at. The rush is four steps: the rate and the packers change at 20 minutes and change back at 40:

```yaml
# The rush: 0.9 orders a second from 20 to 40 minutes, and RUSH_PACKERS for it
scenario:
  - {at: 20 min, path: Shop.arrival.orders.rate, value: 0.9}
  - {at: 20 min, path: Shop.resources.packers.current_cap, value: "${RUSH_PACKERS:-1}"}
  - {at: 40 min, path: Shop.arrival.orders.rate, value: 0.4}
  - {at: 40 min, path: Shop.resources.packers.current_cap, value: 1}
```

`${RUSH_PACKERS:-1}` reads the environment variable `RUSH_PACKERS`, so `RUSH_PACKERS=2 desflow run changing_demand.yaml` runs the second plan.

```python
# The rush: 0.9 orders a second from 20 to 40 minutes, and rush_packers for it
.add_scenario(
    [
        {"at": "20 min", "path": "Shop.arrival.orders.rate", "value": 0.9},
        {
            "at": "20 min",
            "path": "Shop.resources.packers.current_cap",
            "value": rush_packers,
        },
        {"at": "40 min", "path": "Shop.arrival.orders.rate", "value": 0.4},
        {
            "at": "40 min",
            "path": "Shop.resources.packers.current_cap",
            "value": 1,
        },
    ]
)
```

```python
def schedule(rush_packers):
    """The rush: 0.9 orders a second from 20 to 40 minutes, and rush_packers for it."""
    return [
        {"at": 1200, "path": "Shop.arrival.orders.rate", "value": 0.9},
        {
            "at": 1200,
            "path": "Shop.resources.packers.current_cap",
            "value": rush_packers,
        },
        {"at": 2400, "path": "Shop.arrival.orders.rate", "value": 0.4},
        {"at": 2400, "path": "Shop.resources.packers.current_cap", "value": 1},
    ]
```

`run_scenario` is the SimPy process that applies the steps:

```python
# Each step applies before every other event at its time
env.process(run_scenario(env, schedule(rush_packers), until=3600))
```

The steps follow the simulation clock, so the schedule repeats exactly on every run and at any `factor`. A step at a time applies before every other event at that time: the order that arrives at 20 minutes draws its next gap from the rush rate. [Time-varying Arrivals](https://jaehyeon.me/desflow/latest/guides/time-varying-arrivals/#boundaries) sets out these boundaries.

______________________________________________________________________

## Result

What does a 20-minute rush do to waits? With one packer, it builds a queue for the whole rush: the mean wait over the hour is **147.74 seconds** and the longest **416.75 seconds**, more than 6 minutes. A second packer for the rush keeps the mean at **0.74 seconds** and the longest at **6.06**. Both pack the same **1,978** orders:

The Builder and Low-level scripts print both plans:

```text
1 in the rush: 1978 packed, mean wait 147.74 s, longest 416.75 s
2 in the rush: 1978 packed, mean wait  0.74 s, longest   6.06 s
```

## Why It Works

During the rush one packer would need 0.9 × 1.5 = 1.35 seconds of packing every second, more than it has: 0.9 orders arrive a second and the packer finishes about 0.67, so the queue grows by about 0.23 orders a second for as long as the rush lasts. That is a **finite overload**: when the rush ends the load falls to 0.6, and the packer works the queue off, here by about 54 minutes. A load that stays at 1 or above is different: its queue grows for as long as the run lasts, and no run length gives a stable mean wait, as [Queueing Basics](https://jaehyeon.me/desflow/latest/des/queueing-basics/#parts-of-a-queue) explains. A finite overload has an answer, but it depends on the schedule: a mean over the hour mixes the quiet minutes with the rush, so report the longest wait, or the waits during the rush, beside it. With two packers the rush load is 0.68 a packer, and waits stay short.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Changing Demand tutorial: a 20-minute rush, with one packer or two.
# Run it with: desflow run changing_demand.yaml
# With a second packer for the rush: RUSH_PACKERS=2 desflow run changing_demand.yaml
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42

resources:
  packers: {current_cap: 1, max_cap: 2}

services:
  packing: {dist: constant, value: 1.5 s}

arrivals:
  orders: {dist: exponential, rate: 0.4, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

# The rush: 0.9 orders a second from 20 to 40 minutes, and RUSH_PACKERS for it
scenario:
  - {at: 20 min, path: Shop.arrival.orders.rate, value: 0.9}
  - {at: 20 min, path: Shop.resources.packers.current_cap, value: "${RUSH_PACKERS:-1}"}
  - {at: 40 min, path: Shop.arrival.orders.rate, value: 0.4}
  - {at: 40 min, path: Shop.resources.packers.current_cap, value: 1}

# Orders packed, and the mean and longest wait, at the end
stats: {level: typical, interval: 2 h}

egress:
  - type: Console

run:
  until: 1 h
```

```python
"""The Changing Demand tutorial: a 20-minute rush, with one packer or two.

One packer and 1.5 seconds of packing; orders arrive at 0.4 a second, and at 0.9
from 20 to 40 minutes. Every record is printed to the terminal, and the script
prints the waits with one packer through the rush and with a second packer for it:
python changing_demand.py 2>/dev/null shows only those lines.
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(rush_packers: int) -> SimulationContext:
    """The shop of First Simulation, with a rush from 20 to 40 minutes."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=2)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.4)
        # The rush: 0.9 orders a second from 20 to 40 minutes, and rush_packers for it
        .add_scenario(
            [
                {"at": "20 min", "path": "Shop.arrival.orders.rate", "value": 0.9},
                {
                    "at": "20 min",
                    "path": "Shop.resources.packers.current_cap",
                    "value": rush_packers,
                },
                {"at": "40 min", "path": "Shop.arrival.orders.rate", "value": 0.4},
                {
                    "at": "40 min",
                    "path": "Shop.resources.packers.current_cap",
                    "value": 1,
                },
            ]
        )
        # Orders packed, and the mean and longest wait, at the end
        .with_stats("typical", interval=7200)
        .add_egress(ConsoleEgress())
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            # The gap is drawn from the rate in force now
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def outcomes(rush_packers: int) -> dict:
    """Runs the hour and returns the orders packed and their waits."""
    app = shop(rush_packers)
    app.run(until="1 h")
    r = app.env.stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "finished": r[name("finished")],
        "wait_mean": r[name("wait_mean")],
        "wait_max": r[name("wait_max")],
    }


if __name__ == "__main__":
    for packers in (1, 2):
        r = outcomes(packers)
        print(
            f"{packers} in the rush: {r['finished']} packed, "
            f"mean wait {r['wait_mean']:5.2f} s, longest {r['wait_max']:6.2f} s"
        )
```

```python
"""The Changing Demand tutorial: a 20-minute rush, with one packer or two.

One packer and 1.5 seconds of packing; orders arrive at 0.4 a second, and at 0.9
from 20 to 40 minutes. Every record is printed to the terminal, and the script
prints the waits with one packer through the rush and with a second packer for it:
python changing_demand_low_level.py 2>/dev/null shows only those lines.
"""

import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    run_scenario,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=2)},
)


def schedule(rush_packers):
    """The rush: 0.9 orders a second from 20 to 40 minutes, and rush_packers for it."""
    return [
        {"at": 1200, "path": "Shop.arrival.orders.rate", "value": 0.9},
        {
            "at": 1200,
            "path": "Shop.resources.packers.current_cap",
            "value": rush_packers,
        },
        {"at": 2400, "path": "Shop.arrival.orders.rate", "value": 0.4},
        {"at": 2400, "path": "Shop.resources.packers.current_cap", "value": 1},
    ]


def pack_order(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack_order", task_id)


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    for task_id in itertools.count():
        # The gap is drawn from the rate in force now
        yield env.timeout(sampler.sample(arrival))
        key = f"orders-{task_id + 1}"
        env.process(pack_order(env, sampler, packers, key, task_id))


def run(rush_packers=1, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Orders packed, and the mean and longest wait, at the end
    env.setup_stats("Shop", "typical", interval=7200)
    packers = DynamicResource(env, "Shop", "packers")
    # Each step applies before every other event at its time
    env.process(run_scenario(env, schedule(rush_packers), until=3600))
    env.process(order_generator(env, Sampler(seed=42), packers))
    try:
        env.run(until=3600)
    finally:
        env.teardown()
    return env


def outcomes(rush_packers: int) -> dict:
    """Runs the hour and returns the orders packed and their waits."""
    r = run(rush_packers).stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "finished": r[name("finished")],
        "wait_mean": r[name("wait_mean")],
        "wait_max": r[name("wait_max")],
    }


if __name__ == "__main__":
    for packers in (1, 2):
        r = outcomes(packers)
        print(
            f"{packers} in the rush: {r['finished']} packed, "
            f"mean wait {r['wait_mean']:5.2f} s, longest {r['wait_max']:6.2f} s"
        )
```

Next, [Local Replay](https://jaehyeon.me/desflow/latest/tutorials/local-replay/index.md) asks what the shop does with the orders of a real morning.

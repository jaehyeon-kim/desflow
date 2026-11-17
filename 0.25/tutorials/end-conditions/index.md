# End Conditions

For an hour the shop now gets an order every 1.43 seconds on average (42 a minute, `rate: 0.7`). Its one packer takes 1.5 seconds per order, so it packs at most 40 a minute, a little fewer than the 42 that arrive. Orders are still waiting when the hour ends. A report that counts only the orders packed in the hour leaves them out. This page asks: **does a fixed horizon answer the same question as closing the doors and packing every order already in the shop?**

- **Decision:** which way of ending the run the shop's report uses.
- **Measures:** the orders started, finished and still in the shop, the mean wait, and the time the run ends.
- **Runs:** two runs, each with the seed 42: one stops at the end of the hour, and one stops taking orders then and runs until the last order is packed.
- **Assumptions:** orders arrive at random, and every order takes the same time to pack.

## What You Add

This page adds a **drain** at the end of the run, which packs every order still in the shop, and the counts that show which orders each way of ending leaves out. The shop is the one from [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/index.md), busier: one packer and 1.5 seconds of packing.

______________________________________________________________________

## Count Who Is Still There

An end condition decides which entities a run's results describe. These statistics count the orders:

- `flows.orders.started`, from the `basic` level, counts the orders that entered the shop. An order enters when its first task begins, before any queue.
- `flows.orders.finished`, from `basic`, counts the orders that left.
- `flows.orders.in_system`, from `basic`, counts the orders still in the shop.
- `tasks.pack_order.in_progress`, from `minimal`, counts the orders waiting to be packed or being packed.

The model switches on `typical`, which also keeps the mean wait:

```yaml
# Orders started, finished and still in the shop, and the mean wait, at the end
stats: {level: typical, interval: 2 h}
```

```python
# Orders started, finished and still in the shop, and the mean wait, at the end
.with_stats("typical", interval=7200)
```

```python
# Orders started, finished and still in the shop, and the mean wait, at the end
env.setup_stats("Shop", "typical", interval=7200)
```

A low-level process reports its entities with `env.stats.flow_started(arrival)` and `env.stats.flow_finished(arrival)`, as the full file below does.

Every order that started is either finished or still in the shop, so `started` is `finished` plus `in_system`. These counts say how many orders a mean wait leaves out.

______________________________________________________________________

## Close the Doors, Then Finish

Save the files from [Full Files](#full-files) in one folder and run the commands there. A **fixed horizon** stops the clock at `until`, whatever is still going on. A **drain** stops only the arrivals at `until` and keeps running until every entity in the model has left. In SimPy that is two steps: interrupt the source process, then run until every admitted process has ended. desflow writes both with `drain`:

```yaml
run:
  # No new orders after one hour
  until: 1 h
  # true: pack every order already in the shop before the run ends
  drain: ${DRAIN:-false}
```

`${DRAIN:-false}` reads the environment variable `DRAIN`, so `DRAIN=true desflow run end_conditions.yaml` runs the drain, as `desflow run end_conditions.yaml --drain` does.

```python
# No new orders after one hour; with drain, pack every order already in the shop
app.run(until="1 h", drain=drain or None)
```

```python
env.run(until=3600)  # no new orders after one hour
if drain:
    # Stop the source, then run until every admitted order has ended
    source.interrupt()
    env.run(until=env.all_of([o for o in orders if o.is_alive]))
```

`env.all_of(...)` is SimPy's `AllOf`: it happens when every order process has ended. [SimPy and desflow](https://jaehyeon.me/desflow/latest/des/simpy-and-desflow/#waiting-for-one-or-all) explains waiting for all events.

`source.interrupt()` raises an `Interrupt` inside the source process, which ends its loop. [SimPy and desflow](https://jaehyeon.me/desflow/latest/des/simpy-and-desflow/#interrupts) explains interrupts.

[`o.is_alive`](https://simpy.readthedocs.io/en/4.1.2/api_reference/simpy.events.html#simpy.events.Process.is_alive) is true while a process has not ended.

`env.now` is the clock, which the script reads to report the end time. [SimPy and desflow](https://jaehyeon.me/desflow/latest/des/simpy-and-desflow/#environment-and-the-event-list) explains the clock.

The stop at `until` is an event that SimPy schedules with the `URGENT` priority, so it comes before the other events at that time. A low-level script that stops its own run at an event can do the same with `env.schedule(event, URGENT, delay)`. In a blueprint, the `fire` action's `urgent` and `after` keys do it, as [Signals and Waiting](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens) shows.

A drain can also take a limit, `drain: {limit: 10 min}`, so a model that never empties still ends. A `stop` ends the run as soon as an event or a condition happens. [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#stop-early) describes both.

______________________________________________________________________

## Result

Does a fixed horizon answer the same question as a drain? Not for every order. Each figure here comes from a run with the seed 42: one that stops at the hour, and one that drains after it. The fixed horizon ends at **3,600** seconds with **2,436** orders started, **2,398** finished and **38** still in the shop, one being packed and 37 waiting. The drain ends **56.4** seconds later, at **3,656.4**, with all **2,436** finished:

`desflow explain` shows why so many orders are left, without a run:

```bash
desflow explain end_conditions.yaml
```

```text
Resources
+--------------------------------------------------------+
| resource |   λ |      μ | c |    ρ | Wq | L | status   |
|----------+-----+--------+---+------+----+---+----------|
| packers  | 0.7 | 0.6667 | 1 | 1.05 |  - | - | UNSTABLE |
+--------------------------------------------------------+
Not estimated:
  wait at packers: unstable: the queue grows for as long as the run lasts
```

The packer gets 1.05 seconds of work every second, more than it can do, so the queue grows for as long as orders arrive. The longer the horizon, the more orders it leaves in the shop. A drain packs them all. In Python, `context.analyze()` gives the same analysis for a builder model. A low-level model has no `explain`, because its SimPy code makes the links between arrivals, tasks and resources as it runs, so there are none to read.

The mean wait barely moves: **71.70** seconds with the fixed horizon against **71.51** with the drain. An order's wait is measured when its packing starts, and only 37 of the 2,436 orders start packing after the hour. The Builder and Low-level scripts print both runs:

```text
horizon: ends at  3600.0 s, 2436 started, 2398 finished, 38 left, mean wait 71.70 s
drain  : ends at  3656.4 s, 2436 started, 2436 finished, 0 left, mean wait 71.51 s
```

## Why It Works

A fixed horizon answers a question about a period, such as how many orders the shop packs in an hour. The orders still in the shop at the end are unfinished work, and `in_system` says how many there are. A drain answers a question about a batch of orders, such as how long the day's orders take to clear. It serves every admitted order, but the run goes on past the hour. Report the counts beside any mean, so a reader knows which orders it covers. [Output Analysis](https://jaehyeon.me/desflow/latest/des/output-analysis/#terminating-or-steady-state) explains the two kinds of run, and [Statistics](https://jaehyeon.me/desflow/latest/user-guide/statistics/index.md) describes each count.

______________________________________________________________________

## Full Files

Full files

```yaml
# The End Conditions tutorial: a fixed horizon against a drain.
# Run it with: desflow run end_conditions.yaml
# With a drain after the hour: DRAIN=true desflow run end_conditions.yaml
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42

resources:
  packers: {current_cap: 1, max_cap: 4}

services:
  packing: {dist: constant, value: 1.5 s}

arrivals:
  orders: {dist: exponential, rate: 0.7, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

# Orders started, finished and still in the shop, and the mean wait, at the end
stats: {level: typical, interval: 2 h}

egress:
  - type: Console

run:
  # No new orders after one hour
  until: 1 h
  # true: pack every order already in the shop before the run ends
  drain: ${DRAIN:-false}
```

```python
"""The End Conditions tutorial: a fixed horizon against a drain.

One packer, an order every 1.43 seconds on average (rate 0.7) and 1.5 seconds of
packing, for an hour. Every record is printed to the terminal, and the script prints the
counts and the mean wait with a fixed horizon and with a drain at the end. To see only
those lines, run:
python end_conditions.py 2>/dev/null
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop() -> SimulationContext:
    """The shop of First Simulation, busier: more orders than one packer can pack."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=4)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.7)
        # Orders started, finished and still in the shop, and the mean wait, at the end
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
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def outcomes(drain: bool) -> dict:
    """Runs the hour, with or without a drain, and returns the counts and the wait."""
    app = shop()
    # No new orders after one hour; with drain, pack every order already in the shop
    app.run(until="1 h", drain=drain or None)
    r = app.env.stats.readings()
    name = "Shop.stats.{}".format
    return {
        "started": r[name("flows.orders.started")],
        "finished": r[name("flows.orders.finished")],
        "in_system": r[name("flows.orders.in_system")],
        "wait_mean": r[name("tasks.pack_order.wait_mean")],
        "end": app.env.now,
    }


if __name__ == "__main__":
    for drain in (False, True):
        r = outcomes(drain)
        label = "drain  " if drain else "horizon"
        print(
            f"{label}: ends at {r['end']:7.1f} s, {r['started']} started, "
            f"{r['finished']} finished, {r['in_system']} left, "
            f"mean wait {r['wait_mean']:5.2f} s"
        )
```

```python
"""The End Conditions tutorial: a fixed horizon against a drain.

One packer, an order every 1.43 seconds on average (rate 0.7) and 1.5 seconds of
packing, for an hour. Every record is printed to the terminal, and the script prints the
counts and the mean wait with a fixed horizon and with a drain at the end. To see only
those lines, run:
python end_conditions_low_level.py 2>/dev/null
"""

import itertools
import logging

import simpy

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.7)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=4)},
)


def pack_order(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    env.stats.flow_started("orders")  # the order enters the shop
    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")


def order_generator(env, sampler, packers, orders):
    arrival = env.registry.get_config("Shop.arrival.orders")
    try:
        for task_id in itertools.count():
            yield env.timeout(sampler.sample(arrival))
            key = f"orders-{task_id + 1}"
            orders.append(env.process(pack_order(env, sampler, packers, key, task_id)))
    except simpy.Interrupt:
        pass  # no new orders after the hour


def run(drain=False, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Orders started, finished and still in the shop, and the mean wait, at the end
    env.setup_stats("Shop", "typical", interval=7200)
    packers = DynamicResource(env, "Shop", "packers")
    orders = []
    source = env.process(order_generator(env, Sampler(seed=42), packers, orders))
    try:
        env.run(until=3600)  # no new orders after one hour
        if drain:
            # Stop the source, then run until every admitted order has ended
            source.interrupt()
            env.run(until=env.all_of([o for o in orders if o.is_alive]))
    finally:
        env.teardown()
    return env


def outcomes(drain: bool) -> dict:
    """Runs the hour, with or without a drain, and returns the counts and the wait."""
    env = run(drain)
    r = env.stats.readings()
    name = "Shop.stats.{}".format
    return {
        "started": r[name("flows.orders.started")],
        "finished": r[name("flows.orders.finished")],
        "in_system": r[name("flows.orders.in_system")],
        "wait_mean": r[name("tasks.pack_order.wait_mean")],
        "end": env.now,
    }


if __name__ == "__main__":
    for drain in (False, True):
        r = outcomes(drain)
        label = "drain  " if drain else "horizon"
        print(
            f"{label}: ends at {r['end']:7.1f} s, {r['started']} started, "
            f"{r['finished']} finished, {r['in_system']} left, "
            f"mean wait {r['wait_mean']:5.2f} s"
        )
```

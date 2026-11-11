# Write Another Form

Ask for the Builder or Low-level form of a blueprint when the model needs Python, such as a payload function, or SimPy itself. An assistant with the skill writes a form that gives the same records as the blueprint, and checks that it does.

______________________________________________________________________

## Ask

```text
Write the Builder and Low-level forms of drafted_shop.yaml.
They must give the same records as the YAML at the same seed.
```

______________________________________________________________________

## What the Assistant Does

The skill gives the assistant the rules that keep the forms equal:

- A time in YAML and in the Builder takes a unit. The Low-level `DistributionConfig` takes seconds only, so the unit goes in a comment.
- The Low-level form draws each time through `sampler.sample(env.registry.get_config("<sim_id>.<path>"))`, as the YAML does, so it draws the same numbers at the same seed.
- Builder and Low-level code use only the names `from desflow import ...` gives, never a private attribute.
- A Python file runs with `python <file>` and prints its records with `ConsoleEgress(format="json")`.

Then it runs the skill's check on the three files. The check runs each form at the model's seed and the two after it, and compares every record with the YAML's:

```bash
python <skill folder>/scripts/check_model.py drafted_shop.yaml drafted_shop_builder.py drafted_shop_low_level.py
```

```text
ok desflow check
ok imports
ok runs of the Python files
ok seed and start time
ok records of the forms
ok desflow test
```

Each line is a check that passed. A failed check names the file and line, and what to write instead. `seed and start time` fails on a model without a fixed `logical_start_time`, because the timestamps of two runs then differ, so the drafted shop sets one.

______________________________________________________________________

## Full Files

The three forms of the shop from [Draft a Model](https://jaehyeon.me/desflow/latest/ai/draft-a-model/index.md). [Writing a Model](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/index.md) explains how the three ways relate.

Full files

```yaml
# Drafted from a description of the packing shop. Each value says where it came from:
# "said" means the description gives it, and "assumed" means the user has not confirmed
# it.
# Run it with: desflow run drafted_shop.yaml
simulation:
  sim_id: Shop
  factor: 0  # no pacing: the run ends as soon as it is done
  random_seed: 42
  # a fixed date, so two runs give the same records
  logical_start_time: '2026-01-05T09:00:00Z'

resources:
  packers: {current_cap: 1, max_cap: 1}  # said: one packer

services:
  # said: 1.5 seconds; assumed: every order takes the same time
  packing: {dist: constant, value: 1.5}

arrivals:
  # said: an order every 2 seconds on average; assumed: orders arrive at random
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

egress:
  - {type: Console}

run: {until: 1 hour}  # assumed: one hour answers the question

tests:
  # From the request: an order every 2 seconds is 18,000 orders in 10 hours. The
  # count varies by about 134 between seeds (the square root of 18,000), so 540 is
  # four standard deviations, and an arrival rate 10% higher, 19,800, fails
  - name: orders_packed_in_10_hours
    until: 10 hours  # long enough that a 10% change is more than the spread
    expect:
      tasks.pack_order.finished: {value: 18000, within: 540}
  # From the request: 1.5 seconds of packing for every 2 seconds is 75% of the
  # packer's time. Over 10 hours it varies by about 0.5 between seeds, so 2 is four
  # standard deviations, and the same 10% higher rate, 82.5%, fails
  - name: packer_busy_three_quarters
    until: 10 hours
    expect:
      resources.packers.utilization_avg: {value: 75, within: 2}
```

```python
"""The drafted packing shop with the SimulationContext builder.

The Builder form of drafted_shop.yaml: it gives the same records at the same seed.
Run it with: python drafted_shop_builder.py
"""

from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext

app = (
    SimulationContext(
        sim_id="Shop",
        factor=0,  # no pacing: the run ends as soon as it is done
        random_seed=42,
        # a fixed date, so two runs give the same records
        logical_start_time=datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc),
    )
    .add_resource("packers", current_cap=1, max_cap=1)  # said: one packer
    # said: 1.5 seconds; assumed: every order takes the same time
    .add_service("packing", dist="constant", value=1.5)
    # said: an order every 2 seconds on average; assumed: orders arrive at random
    .add_arrival("orders", dist="exponential", rate=0.5)
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


app.add_egress(ConsoleEgress(format="json"))

if __name__ == "__main__":
    app.run(until="1 hour")  # assumed: one hour answers the question
```

```python
"""The drafted packing shop on the low-level API.

The Low-level form of drafted_shop.yaml: it gives the same records at the same seed.
Times are in seconds. Run it with: python drafted_shop_low_level.py
"""

from datetime import datetime, timezone

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

PARAMS = SimParameter(
    sim_id="Shop",
    # said: an order every 2 seconds on average; assumed: orders arrive at random
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    # said: 1.5 seconds; assumed: every order takes the same time
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},  # said: one packer
)
SEED = 42
# a fixed date, so two runs give the same records
START = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)


def pack_order(env, sampler, packers, key, task_id):
    """One order: wait for a packer, pack it, release the packer."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish(
        {"path_id": path_id, "sim_id": "Shop", "service": "packing", "status": "queued"}
    )
    with packers.request() as request:
        yield request
        publish(
            {
                "path_id": path_id,
                "sim_id": "Shop",
                "service": "packing",
                "status": "started",
            }
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})


def order_generator(env, sampler, packers):
    """Starts one pack_order for every arrival, keyed orders-1, orders-2, ..."""
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


def run(until=3600):  # assumed: one hour answers the question
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=START)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress(format="json")])
    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=SEED)
    env.process(order_generator(env, sampler, packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


if __name__ == "__main__":
    run()
```

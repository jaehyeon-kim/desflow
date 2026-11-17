# Parameter Sweeps

[Comparing Designs](https://jaehyeon.me/desflow/latest/learn/studies/comparing-designs/index.md) found that a fourth packer cuts the busy hour's mean wait from 2.48 to 0.27 seconds over 20 [replications](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#replications) of each, at 1.8 orders a second. That rate is an estimate, and demand moves from day to day. This page asks: **how much does the answer depend on the arrival rate?**

- **Decision:** whether the choice of a fourth packer holds if demand differs from the estimate.
- **Measures:** the mean wait of each design at each rate.
- **Runs:** the busy hour after its 2-minute [warm-up](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#one-run-starts-empty), at four arrival rates around the estimate, 20 replications of each design with the same seeds, spawned from 42.
- **Assumptions:** those of Trusting Results: orders at random at a steady rate, and the 40 packing times measured at the bench.

## What You Will Learn

This page shows how to:

- sweep a registry path over several values in an experiment;
- read each design's result at each value;
- check whether a decision holds if an input differs from its estimate.

## What You Add

This page adds a `sweep` to the experiment: a registry path and the values it takes. The model and the two designs are those of Comparing Designs.

______________________________________________________________________

## Sweep One Input

A sweep runs every variant at every value of its paths. Here that is two variants, three and four packers, at four arrival rates from 1.6 to 1.9 orders a second. That makes 8 designs, each run 20 times with the same 20 seeds:

```yaml
variants:
  three: {}
  four: {Shop.resources.packers.current_cap: 4}
# Each variant at each rate: 8 designs, all run with the same 20 seeds
sweep:
  Shop.arrival.orders.rate: [1.6, 1.7, 1.8, 1.9]
```

```python
"variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
# Each variant at each rate: 8 designs, all run with the same 20 seeds
"sweep": {"Shop.arrival.orders.rate": [1.6, 1.7, 1.8, 1.9]},
```

```python
"variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
# Each variant at each rate: 8 designs, all run with the same 20 seeds
"sweep": {"Shop.arrival.orders.rate": [1.6, 1.7, 1.8, 1.9]},
```

Changing one input while every other input stays the same is called **one-factor sensitivity** analysis. It shows how much the answer depends on that input. A sweep with two paths runs every combination of their values, so its size is the product of the list lengths.

Save the files from [Full Files](#full-files) in one folder and run the commands there: `desflow experiment parameter_sweeps_experiment.yaml`, or `python parameter_sweeps.py` and `python parameter_sweeps_low_level.py`. Each design's name is its variant followed by its swept value, such as `three Shop.arrival.orders.rate=1.9`. `results.csv` and `summary.csv` have a column for the swept path. The table gives the mean wait of each design over its 20 replications, with the 95% [half-width](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#interval-and-half-width). The row for 1.8 orders a second is the experiment of Comparing Designs, as [Busy Hour Figures](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/#busy-hour-figures) lists:

| Orders a second | Three packers (s) | Four packers (s) |
| --------------- | ----------------- | ---------------- |
| 1.6             | 0.97 ± 0.07       | 0.17 ± 0.01      |
| 1.7             | 1.47 ± 0.14       | 0.21 ± 0.01      |
| 1.8             | 2.48 ± 0.32       | 0.27 ± 0.01      |
| 1.9             | 5.55 ± 1.01       | 0.35 ± 0.02      |

______________________________________________________________________

## What You Get and Why

The mean wait of each design against the arrival rate, with its 95% interval:

Each point is the mean of 20 replications after the 2-minute warm-up, with seeds spawned from 42, the same for every rate and design. How much does the answer depend on the arrival rate? With three packers, a great deal. A rate of 1.9 orders a second, about 6% above the estimate, more than doubles the mean wait, to **5.55 seconds**. A rate of 1.6 orders a second cuts it to 0.97 seconds. With four packers the mean wait stays under **0.35 seconds** at every rate. The decision to add a packer therefore holds at every rate in this range. The three-packer wait, on the other hand, cannot be predicted well without a better estimate of the rate.

Three packers that take 1.5 seconds an order on average pack 2 orders a second at most. At 1.9 orders a second they are busy 95% of the time. Close to that limit, a small rise in demand makes the queue much longer, so the wait grows faster than the rate, as [Queueing Basics](https://jaehyeon.me/desflow/latest/learn/concepts/queueing-basics/#waits-near-full-use) explains. Four packers stay below 72% busy at every rate in the sweep, far from their limit. Every design ran with the same seeds, so the differences between rows come from the rate, not from different random draws. The intervals widen with the rate, because a busier shop varies more from run to run.

## Key Takeaways

- A sweep shows how much an answer depends on an input that is only estimated.
- Near full use a small rise in demand makes the wait grow much faster than the demand.
- A decision that holds over the whole range of the sweep does not depend on the exact estimate.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Parameter Sweeps tutorial: how much does the mean wait depend on the demand?
# Run it with: desflow experiment parameter_sweeps_experiment.yaml
experiment: demand
question: How sensitive is the busy hour's mean wait to the arrival rate?
# The busy hour of Trusting Results, with its 2-minute warm-up
model: trusting_results.yaml
until: 1 h
replications: 20
seed: 42
measures: [tasks.pack_order.wait_mean]
variants:
  three: {}
  four: {Shop.resources.packers.current_cap: 4}
# Each variant at each rate: 8 designs, all run with the same 20 seeds
sweep:
  Shop.arrival.orders.rate: [1.6, 1.7, 1.8, 1.9]
output: results/demand
```

```yaml
# The Trusting Results tutorial: the busy hour, measured after a warm-up.
# Run it with: desflow run trusting_results.yaml
# Without the warm-up: WARMUP=0 desflow run trusting_results.yaml
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  # Records and statistics start after the first 2 minutes; 0 keeps everything
  warmup: ${WARMUP:-2 min}

resources:
  packers: {current_cap: 3, max_cap: 10}

arrivals:
  orders: {dist: exponential, rate: 1.8, spawn: pack_order}

services:
  # The 40 packing times measured at the bench, in seconds
  packing:
    dist: empirical
    values: [1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
             2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
             1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
             2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1]

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

# Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
stats: {level: typical, interval: 10}

egress:
  - type: Console

run:
  until: 1 h
```

```python
"""The Parameter Sweeps tutorial: the wait at four arrival rates, builder.

Runs the busy hour of Trusting Results with three packers and with four, each at four
arrival rates, 20 times with the same seeds. The experiment writes its artifacts and
report to results/demand, and the script prints each design's mean wait.
"""

import logging

from desflow import run_experiment
from trusting_results import shop

# The imported model logs at INFO; an experiment logs only its warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

EXPERIMENT = {
    "experiment": "demand",
    "question": "How sensitive is the busy hour's mean wait to the arrival rate?",
    "model": shop,
    "until": "1 h",
    "replications": 20,
    "seed": 42,
    "measures": ["tasks.pack_order.wait_mean"],
    "variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
    # Each variant at each rate: 8 designs, all run with the same 20 seeds
    "sweep": {"Shop.arrival.orders.rate": [1.6, 1.7, 1.8, 1.9]},
    "output": "results/demand",
}


def experiment(output=None):
    """Runs every design of the sweep."""
    return run_experiment(EXPERIMENT, output=output)


if __name__ == "__main__":
    for row in experiment().summary:
        if row["statistic"] == "tasks.pack_order.wait_mean":
            print(f"{row['design']}: {row['mean']:.2f} ± {row['half_width']:.2f} s")
```

```python
"""The Trusting Results tutorial: the busy hour, measured after a warm-up, builder.

Three packers pack orders that arrive at 1.8 a second on average, with the packing
times measured at the bench. Every record is printed to the terminal. At the end the
script prints the mean wait without and with the warm-up, the mean wait of 5 runs
after each of three candidate warm-ups, then the mean and half-width of two outputs
over 5, 10, 20 and 40 replications. To see only those lines, run:
python trusting_results.py 2>/dev/null
"""

import logging

from desflow import ConsoleEgress, SimulationContext, replicate
from desflow.analysis import interval

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# The 40 packing times measured at the bench, in seconds
MEASURED = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip


def shop(warmup="2 min") -> SimulationContext:
    """The busy hour; records and statistics start after `warmup`."""
    app = (
        # Records and statistics start after the first 2 minutes; None keeps everything
        SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup=warmup)
        .add_resource("packers", current_cap=3, max_cap=10)
        .add_arrival("orders", dist="exponential", rate=1.8)
        .add_service("packing", dist="empirical", values=MEASURED)
        # Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
        .with_stats("typical", interval=10)
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


def mean_wait(warmup) -> float:
    """Runs the busy hour and returns the mean wait it measured."""
    app = shop(warmup)
    app.run(until="1 h")
    return app.env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


MEASURES = ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"]


def replications(n=40):
    """Runs the busy hour n times, each after the warm-up, with seeds spawned from 42."""
    return replicate(shop, n, seed=42, until="1 h", measures=MEASURES)


def warmups(n=5) -> dict:
    """The mean wait of n runs after each candidate warm-up, with seeds spawned from 42."""
    wait = ["tasks.pack_order.wait_mean"]
    return {
        warmup: replicate(shop, n, seed=42, until="1 h", warmup=warmup, measures=wait)
        for warmup in ("1 min", "2 min", "4 min")
    }


def precision(result) -> dict:
    """Each output's mean and half-width over the first 5, 10, 20 and 40 replications."""
    return {
        (measure, n): interval(result.values[measure][:n])
        for measure in MEASURES
        for n in (5, 10, 20, 40)
    }


if __name__ == "__main__":
    print(f"without a warm-up: mean wait {mean_wait(None):.3f} s")
    print(f"with a 2 min warm-up: mean wait {mean_wait('2 min'):.3f} s")
    for warmup, result in warmups().items():
        row = result.summary[0]
        print(
            f"warm-up {warmup}: mean wait {row['mean']:.3f} ± {row['half_width']:.2f} s"
        )
    for (measure, n), result in precision(replications()).items():
        share = result.half_width / result.mean
        print(
            f"{measure}, {n} runs: {result.mean:.2f} ± {result.half_width:.2f} ({share:.1%})"
        )
```

```python
"""The Parameter Sweeps tutorial: the wait at four arrival rates, low-level.

Runs the busy hour of Trusting Results with three packers and with four, each at four
arrival rates, 20 times with the same seeds. The experiment writes its artifacts and
report to results/demand, and the script prints each design's mean wait.
"""

import logging

from desflow import LowLevelModel, run_experiment
from trusting_results_low_level import WARMUP, model

# The imported model logs at INFO; an experiment logs only its warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

EXPERIMENT = {
    "experiment": "demand",
    "question": "How sensitive is the busy hour's mean wait to the arrival rate?",
    "model": LowLevelModel(model, sim_id="Shop"),
    "warmup": WARMUP,
    "until": 3600,
    "replications": 20,
    "seed": 42,
    "measures": ["tasks.pack_order.wait_mean"],
    "variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
    # Each variant at each rate: 8 designs, all run with the same 20 seeds
    "sweep": {"Shop.arrival.orders.rate": [1.6, 1.7, 1.8, 1.9]},
    "output": "results/demand",
}


def experiment(output=None):
    """Runs every design of the sweep."""
    return run_experiment(EXPERIMENT, output=output)


if __name__ == "__main__":
    for row in experiment().summary:
        if row["statistic"] == "tasks.pack_order.wait_mean":
            print(f"{row['design']}: {row['mean']:.2f} ± {row['half_width']:.2f} s")
```

```python
"""The Trusting Results tutorial: the busy hour, measured after a warm-up, low-level.

Three packers pack orders that arrive at 1.8 a second on average, with the packing
times measured at the bench. Every record is printed to the terminal. At the end the
script prints the mean wait without and with the warm-up, the mean wait of 5 runs
after each of three candidate warm-ups, then the mean and half-width of two outputs
over 5, 10, 20 and 40 replications. To see only those lines, run:
python trusting_results_low_level.py 2>/dev/null
"""

import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    LowLevelModel,
    Sampler,
    SimParameter,
    replicate,
)
from desflow.analysis import interval

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# The 40 packing times measured at the bench, in seconds
MEASURED = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
    service={"packing": DistributionConfig(dist="empirical", values=MEASURED)},
    resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
)


def pack_order(env, sampler, packers, key, task_id):
    """One order, which reports each step to the statistics."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.flow_started("orders")  # the order enters the shop
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
    env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")  # the order leaves the shop


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


# Records and statistics start after the first 2 minutes; None keeps everything
WARMUP = 120.0  # seconds, passed to setup_egress in run below


def run(warmup=WARMUP, until=3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    # Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
    env.setup_stats("Shop", "typical", interval=10)
    # The warm-up is set with the sinks: no sink receives a record stamped before it
    # ends, and the statistics start again from zero when it ends
    env.setup_egress([ConsoleEgress()], warmup=warmup)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, Sampler(seed=42), packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def mean_wait(warmup) -> float:
    """Runs the busy hour and returns the mean wait it measured."""
    return run(warmup).stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


MEASURES = ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"]


def model(env, sampler):
    """The busy hour in a given environment: replicate sets the statistics."""
    env.registry.register_sim_parameter(PARAMS)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, sampler, packers))


def replications(n=40):
    """Runs the busy hour n times, each after the warm-up, with seeds spawned from 42."""
    shop = LowLevelModel(model, sim_id="Shop")
    return replicate(shop, n, seed=42, until=3600, warmup=WARMUP, measures=MEASURES)


def warmups(n=5) -> dict:
    """The mean wait of n runs after each candidate warm-up, with seeds spawned from 42."""
    shop = LowLevelModel(model, sim_id="Shop")
    wait = ["tasks.pack_order.wait_mean"]
    return {
        warmup: replicate(shop, n, seed=42, until=3600, warmup=warmup, measures=wait)
        for warmup in (60, WARMUP, 240)
    }


def precision(result) -> dict:
    """Each output's mean and half-width over the first 5, 10, 20 and 40 replications."""
    return {
        (measure, n): interval(result.values[measure][:n])
        for measure in MEASURES
        for n in (5, 10, 20, 40)
    }


if __name__ == "__main__":
    print(f"without a warm-up: mean wait {mean_wait(None):.3f} s")
    print(f"with a 2 min warm-up: mean wait {mean_wait(WARMUP):.3f} s")
    for warmup, result in warmups().items():
        row = result.summary[0]
        print(
            f"warm-up {warmup:g} s: mean wait {row['mean']:.3f} ± {row['half_width']:.2f} s"
        )
    for (measure, n), result in precision(replications()).items():
        share = result.half_width / result.mean
        print(
            f"{measure}, {n} runs: {result.mean:.2f} ± {result.half_width:.2f} ({share:.1%})"
        )
```

[Experiments](https://jaehyeon.me/desflow/latest/user-guide/results/experiments/index.md) describes every setting of an experiment, its files and its report.

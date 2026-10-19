# Parameter Sweeps

**Level:** Intermediate

[Comparing Designs](https://jaehyeon.me/desflow/latest/tutorials/comparing-designs/index.md) found that a fourth packer cuts the busy hour's mean wait from 2.48 to 0.27 seconds, at 1.8 orders a second. That rate is an estimate, and demand moves from day to day. This page asks: **how much does the answer depend on the arrival rate?** It runs both designs at four rates around the estimate.

## What You Add

A `sweep` in the experiment: a registry path and the values it takes. The model and the two designs are those of Comparing Designs.

______________________________________________________________________

## Sweep One Input

A sweep runs every variant at every value of its paths. Here that is two variants at four rates, 8 designs, each run 20 times with the same 20 seeds:

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

Changing one input while every other stays as it is shows how sensitive the answer is to that input. This is **one-factor sensitivity**. A sweep with two paths runs every combination of their values, so its size is the product of the list lengths.

Run it with `desflow experiment parameter_sweeps_experiment.yaml`, or `python parameter_sweeps.py` and `python parameter_sweeps_low_level.py`. Each design's name is its variant followed by its swept value, such as `three Shop.arrival.orders.rate=1.9`, and `results.csv` and `summary.csv` have a column for the swept path. The mean wait of each design, with the 95% half-width:

| Orders a second | Three packers (s) | Four packers (s) |
| --------------- | ----------------- | ---------------- |
| 1.6             | 0.97 ± 0.07       | 0.17 ± 0.01      |
| 1.7             | 1.47 ± 0.14       | 0.21 ± 0.01      |
| 1.8             | 2.48 ± 0.32       | 0.27 ± 0.01      |
| 1.9             | 5.55 ± 1.01       | 0.35 ± 0.02      |

______________________________________________________________________

## Result

How much does the answer depend on the arrival rate? With three packers, a great deal: 1.9 orders a second, about 6% above the estimate, more than doubles the mean wait to **5.55 seconds**, and 1.6 orders a second cuts it to 0.97. With four packers the mean wait stays under **0.35 seconds** at every rate. So the decision to add a packer holds whatever the demand within this range, while the three-packer wait cannot be predicted well without a better estimate of the rate.

## Why It Works

Three packers that take 1.5 seconds an order on average pack 2 orders a second at most. At 1.9 orders a second they are busy 95% of the time, and close to that limit a small rise in demand makes the queue much longer, so the wait grows faster than the rate. Four packers stay below 72% busy at every rate in the sweep, far from their limit. Every design ran with the same seeds, so the differences between rows come from the rate, not from chance. The intervals widen with the rate, because a busier shop varies more from run to run.

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

The model, `trusting_results.yaml`, is in the full files of [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/#full-files).

```python
"""The Parameter Sweeps tutorial: the wait at four arrival rates, builder.

The busy hour of Trusting Results with three packers and with four, each at four
arrival rates, run 20 times with the same seeds. The experiment writes its artifacts and
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

It imports from `trusting_results.py`, which is in the full files of [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/#full-files).

```python
"""The Parameter Sweeps tutorial: the wait at four arrival rates, low-level.

The busy hour of Trusting Results with three packers and with four, each at four
arrival rates, run 20 times with the same seeds. The experiment writes its artifacts and
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

It imports from `trusting_results_low_level.py`, which is in the full files of [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/#full-files).

This is the last page of the tutorial for now. [Experiments](https://jaehyeon.me/desflow/latest/user-guide/experiments/index.md) describes every setting of an experiment, its files and its report, and the rest of the [User Guide](https://jaehyeon.me/desflow/latest/user-guide/ways-to-write/index.md) describes each part of desflow in full.

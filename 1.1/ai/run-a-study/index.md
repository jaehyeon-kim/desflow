# Run a Study

Ask for a study when you need an answer you can act on. One run is one sample, so an assistant with the skill answers from replications, with a confidence interval, and compares designs with the same seeds.

______________________________________________________________________

## Ask

```text
Does a second packer cut the mean wait of drafted_shop.yaml, and by how much?
Run a study, and give the difference with its interval.
```

______________________________________________________________________

## What the Assistant Does

The skill gives the assistant these steps:

1. Set a seed, so the study can be repeated.
1. Set a warm-up when the model needs time to reach its usual state.
1. Run replications. Each has its own seed, spawned from the study's seed.
1. Report each mean with its 95% confidence interval.
1. To compare designs, write an experiment file and run `desflow experiment`. It runs every design with the same seeds, compares them seed by seed, and writes `report.md`.

For the drafted shop, the experiment runs one and two packers for an hour, 20 times each:

```yaml
model: drafted_shop.yaml

until: 1 h

replications: 20

seed: 42

# Every design runs with the same 20 seeds, so they are compared seed by seed
pairing: common

measures: [tasks.pack_order.wait_mean, resources.packers.utilization_avg]

variants:
  one: {}
  two: {Shop.resources.packers.max_cap: 2, Shop.resources.packers.current_cap: 2}

compare:
  - {baseline: one, variant: two}
```

```python
"model": shop,
"until": "1 h",
"replications": 20,
"seed": 42,
# Every design runs with the same 20 seeds, so they are compared seed by seed
"pairing": "common",
"measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
"variants": {
    "one": {},
    "two": {
        "Shop.resources.packers.max_cap": 2,
        "Shop.resources.packers.current_cap": 2,
    },
},
"compare": [{"baseline": "one", "variant": "two"}],
```

```python
"model": LowLevelModel(model, sim_id="Shop"),
"until": 3600,
"replications": 20,
"seed": 42,
# Every design runs with the same 20 seeds, so they are compared seed by seed
"pairing": "common",
"measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
"variants": {
    "one": {},
    "two": {
        "Shop.resources.packers.max_cap": 2,
        "Shop.resources.packers.current_cap": 2,
    },
},
"compare": [{"baseline": "one", "variant": "two"}],
```

```bash
desflow experiment drafted_shop_experiment.yaml
```

The Comparisons table of `report.md` gives each difference, two packers minus one, seed by seed:

```text
| Baseline | Variant | Measure | Difference | Half-width | Interval | Random streams |
|---|---|---|---|---|---|---|
| one | two | `tasks.pack_order.wait_mean` | -2.043 | 0.1261 | -2.169 to -1.917 | 1 shared |
| one | two | `resources.packers.utilization_avg` | -37.26 | 0.4302 | -37.69 to -36.83 | 1 shared |
```

A second packer cuts the mean wait by 2.04 ± 0.13 seconds, from 2.18 to 0.13. The packers' utilisation halves, from 75% to 37%. The interval of the difference does not hold 0, so the cut is not chance.

______________________________________________________________________

## Check the Reply

Check that the answer is an interval, not one number, and that both designs ran with the same seeds. Read the report's Missing Inputs section: it names each input whose source the experiment does not state. [Trusting Results](https://jaehyeon.me/desflow/latest/learn/studies/trusting-results/index.md) explains how many replications and how long a warm-up a study needs, and [Experiments](https://jaehyeon.me/desflow/latest/user-guide/results/experiments/index.md) lists every file the experiment writes.

______________________________________________________________________

## Full Files

Full files

```yaml
# Does a second packer cut the mean wait of the drafted shop, and by how much?
# Run it with: desflow experiment drafted_shop_experiment.yaml
experiment: second-packer

question: Does a second packer cut the mean wait of an order, and by how much?

model: drafted_shop.yaml

until: 1 h

replications: 20

seed: 42

# Every design runs with the same 20 seeds, so they are compared seed by seed
pairing: common

measures: [tasks.pack_order.wait_mean, resources.packers.utilization_avg]

variants:
  one: {}
  two: {Shop.resources.packers.max_cap: 2, Shop.resources.packers.current_cap: 2}

compare:
  - {baseline: one, variant: two}

assumptions:
  - Orders arrive at random, at a steady one every 2 seconds.
  - Every order takes 1.5 seconds to pack.

limits: [Each run starts with an empty shop and has no warm-up.]

output: results/second-packer
```

```python
"""Does a second packer cut the mean wait of the drafted shop, and by how much?

The Builder form of drafted_shop_experiment.yaml: it gives the same comparison. Keep it
beside drafted_shop_builder.py. Run it with: python drafted_shop_experiment_builder.py
"""

import logging
from pathlib import Path

from desflow import SimulationContext, load_context, run_experiment

# The model logs at INFO; an experiment logs only its warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

HERE = Path(__file__).resolve().parent


def shop() -> SimulationContext:
    """A new drafted shop for each run, built from the builder file."""
    return load_context(HERE, "drafted_shop_builder.py")


EXPERIMENT = {
    "experiment": "second-packer",
    "question": "Does a second packer cut the mean wait of an order, and by how much?",
    "model": shop,
    "until": "1 h",
    "replications": 20,
    "seed": 42,
    # Every design runs with the same 20 seeds, so they are compared seed by seed
    "pairing": "common",
    "measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
    "variants": {
        "one": {},
        "two": {
            "Shop.resources.packers.max_cap": 2,
            "Shop.resources.packers.current_cap": 2,
        },
    },
    "compare": [{"baseline": "one", "variant": "two"}],
    "assumptions": [
        "Orders arrive at random, at a steady one every 2 seconds.",
        "Every order takes 1.5 seconds to pack.",
    ],
    "limits": ["Each run starts with an empty shop and has no warm-up."],
    "output": "results/second-packer",
}

if __name__ == "__main__":
    result = run_experiment(EXPERIMENT)
    for row in result.comparisons:
        print(f"{row['measure']}: {row['difference']:.3f} ± {row['half_width']:.4f}")
```

```python
"""Does a second packer cut the mean wait of the drafted shop, and by how much?

The Low-level form of drafted_shop_experiment.yaml: it gives the same comparison. Keep
it beside drafted_shop_low_level.py. Times are in seconds. Run it with:
python drafted_shop_experiment_low_level.py
"""

import logging

from drafted_shop_low_level import PARAMS, order_generator

from desflow import DynamicResource, LowLevelModel, run_experiment

# The model logs at INFO; an experiment logs only its warnings
logging.getLogger("desflow").setLevel(logging.WARNING)


def model(env, sampler):
    """The drafted shop in a given environment: the experiment sets the statistics."""
    env.registry.register_sim_parameter(PARAMS)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, sampler, packers))


EXPERIMENT = {
    "experiment": "second-packer",
    "question": "Does a second packer cut the mean wait of an order, and by how much?",
    "model": LowLevelModel(model, sim_id="Shop"),
    "until": 3600,
    "replications": 20,
    "seed": 42,
    # Every design runs with the same 20 seeds, so they are compared seed by seed
    "pairing": "common",
    "measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
    "variants": {
        "one": {},
        "two": {
            "Shop.resources.packers.max_cap": 2,
            "Shop.resources.packers.current_cap": 2,
        },
    },
    "compare": [{"baseline": "one", "variant": "two"}],
    "assumptions": [
        "Orders arrive at random, at a steady one every 2 seconds.",
        "Every order takes 1.5 seconds to pack.",
    ],
    "limits": ["Each run starts with an empty shop and has no warm-up."],
    "output": "results/second-packer",
}

if __name__ == "__main__":
    result = run_experiment(EXPERIMENT)
    for row in result.comparisons:
        print(f"{row['measure']}: {row['difference']:.3f} ± {row['half_width']:.4f}")
```

# Experiments

An experiment answers a question with many runs rather than one. It runs designs of a model, each with replications, compares them seed by seed, checks them against bounds, places them beside values observed in the real system, and writes its results and a report to files. This page describes the experiment file, the Python call and every setting; [Experiment Results](experiment-results.md) describes the files an experiment writes and its report. It runs on one machine and needs no server.

---

## What It Is

A **design** is the model with some registry values changed before the clock starts: a variant, or a variant at one point of a sweep. An experiment runs every design with the same number of [replications](../replications.md), reads the built-in statistics at the end of each run, and summarises each with a mean and a confidence interval. [Comparing Designs](../../tutorials/comparing-designs.md) and [Parameter Sweeps](../../tutorials/parameter-sweeps.md) use one on the shop.

---

## Pages in This Section

- [Experiment Results](experiment-results.md): compare designs, check results against bounds, place observed values beside them, and read the files and the report.

---

## How to Use It

### Write an Experiment

An experiment names its model, how long and how often to run it, what to measure, the designs and what to compare and check. In a file:

```yaml title="examples/yaml/experiment.yaml"
# An experiment in YAML: does a third packer keep the mean wait under 2 seconds?
#
# The twin of examples/builder/experiment_example.py. The model is
# experiment_shop.yaml. The experiment runs one hour of it 20 times with two packers
# and 20 times with three, with the same seeds, after a 3-minute warm-up. It
# compares the two designs seed by seed, checks the three-packer design against a
# 2-second bound, and compares the two-packer design with the mean wait observed in
# the real shop. It writes its artifacts and report to results/packers, and exits 1
# if the check fails.
#
# Needs no containers.
# Run it with: desflow experiment experiment.yaml

experiment: packers
question: Does a third packer keep the mean wait under 2 seconds?
model: experiment_shop.yaml
assumptions:
  - Orders arrive at random, at a steady rate, through the hour.
  - Every packer works at the same speed.
inputs:
  Shop.service.packing: 40 packing times measured at the bench
until: 1 h
warmup: 3 min
replications: 20
seed: 42
# Both designs run with the same 20 seeds, so they are compared seed by seed
pairing: common
measures: [tasks.pack.wait_mean, resources.packers.utilization_avg]
variants:
  two: {}
  three: {Shop.resources.packers.current_cap: 3}
compare:
  - {baseline: two, variant: three}
checks:
  # The upper end of the interval must be under 2 seconds
  - {measure: tasks.pack.wait_mean, variant: three, max: 2}
observed:
  # The mean wait measured in the real shop, which has two packers
  file: ../observed_waits.csv
  variant: two
limits:
  - One hour of steady demand; the rest of the day is not modelled.
output: results/packers
```

In Python the same settings are a mapping, and the model is a blueprint path, a builder factory or a `LowLevelModel`. These are every setting:

| Setting | What it sets | Default |
|---|---|---|
| `experiment` | the experiment's name | required |
| `question` | the question the experiment answers, the report's first section | not stated |
| `model` | a blueprint path; `!python module.function`, a function returning a new `SimulationContext`; or `{function: !python module.function, sim_id: Shop}`, a [low-level function](../replications.md#run-replications) | required |
| `assumptions` | sentences the report lists | none |
| `inputs` | each distribution's registry path, such as `Shop.service.packing`, and where its values came from | none |
| `until` | where each run's arrivals stop, in seconds or a duration such as `8 h`; the run ends there unless `stop` ends it earlier or `drain` runs on | required |
| `stop` | ends each run when it happens: `{signal: name}` or `{condition: "..."}`; for a low-level model, given in Python, a function of the environment that returns the event | a blueprint's `run.stop` |
| `drain` | at `until`, stop the arrivals and run on until the model is empty: `true`, or `{limit: 1 h}`; a low-level model is drained when its function returns its arrival processes | a blueprint's `run.drain` |
| `warmup` | each run's warm-up | the model's own |
| `replications` | runs of each design, 2 or more | required |
| `seed` | the seed every run's seed is spawned from | required |
| `level` | the confidence level of every interval | 0.95 |
| `pairing` | `common` or `independent` | `common` |
| `measures` | the statistics to summarise, such as `tasks.pack.wait_mean` | required |
| `variants` | each variant's name and the registry values it sets | `{base: {}}` |
| `sweep` | registry paths and the values each takes | none |
| `compare` | pairs of variants, `{baseline: ..., variant: ...}` | none |
| `checks` | bounds on measures, `{measure, variant, max, min, on}` | none |
| `observed` | `{file: ..., variant: ...}`, values measured in the real system | none |
| `limits` | sentences the report lists | none |
| `output` | the folder the files are written to | `results/<experiment>` |
| `workers` | runs at once, each in its own process | 1 |

A key the experiment does not know is refused with its line, as in a blueprint. Paths in a file, the model's and the observed file's and `output`, are read from the experiment file's folder, and in a Python mapping from the working directory.

### Run It

=== "YAML"

    ```bash
    desflow experiment experiment.yaml
    ```

    `--output` writes the files to another folder. The command exits with status 1 when a check fails, after writing every file, and with status 2 when the experiment is invalid.

=== "Builder"

    ```python title="docs/snippets/pages/experiments.py:run"
    EXPERIMENT = {
        "experiment": "packers",
        "model": shop,  # a blueprint path, a builder factory or a LowLevelModel
        "until": "1 h",
        "warmup": "3 min",
        "replications": 10,
        "seed": 42,
        "measures": ["tasks.pack.wait_mean"],
        "variants": {"two": {}, "three": {"Shop.resources.packers.current_cap": 3}},
        # A sweep would run every variant at each rate: 6 designs
        # "sweep": {"Shop.arrival.orders.rate": [0.35, 0.4, 0.45]},
        "compare": [{"baseline": "two", "variant": "three"}],
        "checks": [{"measure": "tasks.pack.wait_mean", "variant": "two", "max": 2}],
    }
    try:
        result = run_experiment(EXPERIMENT)  # writes results/packers, then checks
    except CheckFailed as failed:
        result = failed.result  # every file is written before it is raised
        print(failed)
    ```

    `run_experiment` takes an experiment file, a mapping or an `ExperimentSpec`, and returns an `ExperimentResult`. With `raise_on_failure=False` a failed check raises nothing, and `result.failed` lists it. `output=` writes the files to another folder.

=== "Low-level"

    The same call, with `LowLevelModel(model, sim_id="Shop")` as the model, where `model` builds the shop in the environment and with the sampler it is given, as the [example](#example-an-experiment) shows.

The result holds every row the files hold:

```python title="docs/snippets/pages/experiments.py:read"
for row in result.summary:  # one row per design and statistic, counts included
    print(row["design"], row["statistic"], row["mean"], row["half_width"])
for row in result.comparisons:  # the variant minus the baseline, seed by seed
    print(row["measure"], row["difference"], row["half_width"])
print([row["design"] for row in result.failed])  # the failed checks
print(result.output / "report.md")
```

### Variants and Sweeps

Each variant sets registry paths before the clock starts, as a live update would. A path the model does not have, or a value the registry refuses or clamps, such as a capacity above `max_cap`, stops the experiment before anything runs, with the line of the variant. Paths are set in the order the variant lists them, so a variant that lowers `max_cap` below the model's `current_cap` sets `current_cap` first. A sweep runs every variant at every combination of its paths' values, so one variant and one swept path with three values give three designs: a one-factor sensitivity experiment. A path is set by a variant or by the sweep, not both. A design's name is its variant followed by its swept values, such as `three Shop.arrival.orders.rate=0.5`.

### Pairing and Random Streams

With `pairing: common`, every design runs with the same seeds, so a design's replication 3 and another design's replication 3 meet the same arrivals and service times, and their difference leaves most of the chance out ([Comparing Designs](../../tutorials/comparing-designs.md#compare-two-designs)). With `pairing: independent`, each design's seeds are spawned from a seed of its own, and `compare` is refused.

The same seeds pair two runs only where both draw from the same random streams. Each distribution has a stream of its own, named by its registry path without the sim id, such as `service.packing`. The experiment records the streams each run drew from, and each comparison lists the streams both designs used and any that only one used. A stream only one design uses, such as a gift-wrap step only one design has, is not paired.

### Example: An Experiment

The `experiment` example is the experiment above: one hour of the shop with two packers and with three, 20 replications of each with the same seeds, a comparison, a check, and the mean wait observed in the real shop. Its shop is not the busy hour of the tutorials: an order arrives every 2.5 seconds on average and packing takes 4 seconds on average, so its waits differ from theirs. The example needs no Docker service and ends on its own. [Common Mistakes](../../des/common-mistakes.md#taking-one-run-as-the-answer) explains why the tutorials' figures for a wait differ between pages.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get experiment --api yaml
        cd experiment/yaml

        # 3. Run it
        desflow experiment experiment.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get experiment --api yaml
        cd experiment/yaml

        # 3. Run it
        desflow experiment experiment.yaml
        ```

    The model is `experiment_shop.yaml`, beside the experiment file:

    ```yaml title="examples/yaml/experiment_shop.yaml"
    # The model of the experiment in experiment.yaml: one hour of the shop.
    #
    # The twin of the shop function in examples/builder/experiment_example.py. Two packers,
    # orders every 2.5 seconds on average and a packing time of 4 seconds on average.
    # It has no sinks and no statistics level: the experiment runs it without sinks and
    # keeps the statistics its measures need.
    #
    # Needs no containers.
    # Run the experiment with: desflow experiment experiment.yaml

    simulation:
      sim_id: Shop
      factor: 0
      # The experiment gives each run its own seed; 42 is for a run on its own.
      random_seed: 42

    resources:
      packers: {current_cap: 2, max_cap: 4}

    services:
      # A mean of 4 seconds (shape times scale).
      packing: {dist: gamma, shape: 4, scale: 1}

    arrivals:
      orders: {dist: exponential, rate: 0.4, spawn: pack}

    tasks:
      pack:
        service: packing
        resource: packers
        payload: {event_type: order_packed}
    ```

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get experiment --api builder
        cd experiment/builder

        # 3. Run it
        uv run --no-project --with desflow experiment_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get experiment --api builder
        cd experiment/builder

        # 3. Run it
        python experiment_example.py
        ```

    ```python title="examples/builder/experiment_example.py:compare"
    # Both designs run with the same 20 seeds, so they are compared seed by seed
    "pairing": "common",
    "measures": ["tasks.pack.wait_mean", "resources.packers.utilization_avg"],
    "variants": {"two": {}, "three": {"Shop.resources.packers.current_cap": 3}},
    "compare": [{"baseline": "two", "variant": "three"}],
    # The upper end of the interval must be under 2 seconds
    "checks": [{"measure": "tasks.pack.wait_mean", "variant": "three", "max": 2}],
    # The mean wait measured in the real shop, which has two packers
    "observed": {"file": str(HERE.parent / "observed_waits.csv"), "variant": "two"},
    ```

    The whole file is [`examples/builder/experiment_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/experiment_example.py).

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get experiment --api low-level
        cd experiment/low-level

        # 3. Run it
        uv run --no-project --with desflow experiment_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get experiment --api low-level
        cd experiment/low-level

        # 3. Run it
        python experiment_example.py
        ```

    ```python title="examples/low-level/experiment_example.py:compare"
    # Both designs run with the same 20 seeds, so they are compared seed by seed
    "pairing": "common",
    "measures": ["tasks.pack.wait_mean", "resources.packers.utilization_avg"],
    "variants": {"two": {}, "three": {"Shop.resources.packers.current_cap": 3}},
    "compare": [{"baseline": "two", "variant": "three"}],
    # The upper end of the interval must be under 2 seconds
    "checks": [{"measure": "tasks.pack.wait_mean", "variant": "three", "max": 2}],
    # The mean wait measured in the real shop, which has two packers
    "observed": {"file": str(HERE.parent / "observed_waits.csv"), "variant": "two"},
    ```

    The whole file is [`examples/low-level/experiment_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/experiment_example.py).

---

## How It Works

**Before running.** The experiment builds the model once without running it, records its compiled parameters, and refuses an `inputs` path that is not a distribution. It then builds each design the same way, so a bad registry path or value fails before any run. It reads the observed file and refuses a row that does not match its measure.

**Runs.** Each design runs its replications as [Replications](../replications.md#how-it-works) describes: each run builds the model again, unpaced, with no ingress and no egress, and reads the statistics at its end. The statistics level is the lowest that publishes every measure, and at least `basic`, so the counts are there. A low-level model's parameters are put back after each run, so a variant's value or a live change in one run is never where the next run starts.

**Intervals.** Every interval, of a design's mean and of a difference, is the Student t interval of [Replications](../replications.md#how-it-works), at the experiment's `level`.

---

## Stability and Limits

- **Finite designs only.** Variants and sweeps list their values. Latin hypercube designs, sampling uncertain inputs and searching for the best design are not part of an experiment.
- **No event records.** An experiment writes its files and sends nothing to the model's sinks. Run one design with one seed to see its records.
- **End of a run.** A run ends at `until`, unless `stop` ends it earlier or `drain` runs it on until the model is empty; a blueprint's own `run.stop` and `run.drain` apply when the experiment does not set them. Entities still in the model at the end are not finished, so a count of finished entities leaves them out; the Counts section of the report ([Read the Report](experiment-results.md#read-the-report)) shows what each design started, finished and left.
- **Observed values are outputs.** The observed comparison is a check of the model's output against the real system's for the question asked. It does not fit inputs, and one value inside an interval does not make a model valid for every question.
- **One machine.** `workers` runs replications in parallel processes on one machine; there is no distributed or server executor.

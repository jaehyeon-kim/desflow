# Comparing Designs

[Trusting Results](trusting-results.md) measured the busy hour with three packers: over 40 [replications](../des/output-analysis.md#replications) the mean wait was 2.32 ± 0.20 seconds. The shop could hire a fourth packer. This page asks: **does a fourth packer cut the mean wait, and by how much?** The decision is three packers or four. The measures are the mean wait and the packers' [utilisation](../des/queueing-basics.md#utilisation). The boundary is the busy hour after its 2-minute [warm-up](../des/output-analysis.md#one-run-starts-empty); the rest of the day is not modelled. The assumptions are those of Trusting Results: orders at random at 1.8 a second, and the 40 packing times measured at the bench.

## What You Add

This page adds an experiment: a file in YAML, or a mapping in Python, that names the model, the two designs, the comparison and a check. The model is the busy hour of Trusting Results, unchanged.

---

## Name the Model

An experiment runs a model written in any of the three ways, for a number of replications from one seed:

=== "YAML"

    ```yaml title="docs/snippets/yaml/comparing_designs_experiment.yaml:model"
    # The busy hour of Trusting Results, with its 2-minute warm-up
    model: trusting_results.yaml
    until: 1 h
    replications: 20
    seed: 42
    ```

    The path is read from the experiment file's folder.

=== "Builder"

    ```python title="docs/snippets/tutorials/comparing_designs.py:model"
    # A function returning a new busy hour, with its 2-minute warm-up
    "model": shop,
    "until": "1 h",
    "replications": 20,
    "seed": 42,
    ```

    `shop` is the function of Trusting Results that returns a new busy hour. The experiment calls it once per run.

=== "Low-level"

    ```python title="docs/snippets/tutorials/comparing_designs_low_level.py:model"
    # The function that builds the busy hour, and its 2-minute warm-up
    "model": LowLevelModel(model, sim_id="Shop"),
    "warmup": WARMUP,
    "until": 3600,
    "replications": 20,
    "seed": 42,
    ```

    The low-level model has no [warm-up](../des/output-analysis.md#one-run-starts-empty) of its own, so the experiment gives it one.

---

## Compare Two Designs

Each variant names the registry values it changes before the clock starts. `three` changes nothing, so it is the busy hour as written. `four` sets the packers' capacity to 4. `pairing: common` gives both designs the same 20 seeds, and `compare` asks for their difference seed by seed:

=== "YAML"

    ```yaml title="docs/snippets/yaml/comparing_designs_experiment.yaml:compare"
    # Every design runs with the same 20 seeds, so they are compared seed by seed
    pairing: common
    measures: [tasks.pack_order.wait_mean, resources.packers.utilization_avg]
    variants:
      three: {}
      four: {Shop.resources.packers.current_cap: 4}
    compare:
      - {baseline: three, variant: four}
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/comparing_designs.py:compare"
    # Every design runs with the same 20 seeds, so they are compared seed by seed
    "pairing": "common",
    "measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
    "variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
    "compare": [{"baseline": "three", "variant": "four"}],
    ```

=== "Low-level"

    The same keys, in the same mapping as the model.

Run it:

=== "YAML"

    ```bash
    desflow experiment comparing_designs_experiment.yaml
    ```

=== "Builder"

    ```bash
    python comparing_designs.py
    ```

=== "Low-level"

    ```bash
    python comparing_designs_low_level.py
    ```

The experiment runs 40 replications, 20 of each design, and writes what it found to `results/four-packers`. Each design's results, with the 95% [half-width](../des/output-analysis.md#interval-and-half-width):

| Design | Mean wait (s) | Utilisation (%) |
|---|---|---|
| three | 2.48 ± 0.32 | 90.33 ± 0.63 |
| four | 0.27 ± 0.01 | 67.70 ± 0.46 |

The three-packer design ran with the first 20 of the seeds that Trusting Results used. Its row therefore equals that page's row for 20 replications, not its mean of 2.32 seconds over 40. [Common Mistakes](../des/common-mistakes.md#taking-one-run-as-the-answer) explains why the busy hour's figures differ between pages. The comparison gives the four-packer value minus the three-packer value, seed by seed:

| Measure | Difference | Half-width | Random streams |
|---|---|---|---|
| `tasks.pack_order.wait_mean` | -2.21 s | 0.31 s | 2 shared |
| `resources.packers.utilization_avg` | -22.63 percentage points | 0.17 percentage points | 2 shared |

The difference in [utilisation](../des/queueing-basics.md#utilisation) has a half-width of 0.17 percentage points, although each design's own utilisation has a half-width of 0.63 or 0.46. The same seed gives both designs the same arrivals and packing times, so a busy run of one design is a busy run of the other. Their difference therefore leaves out most of the variation that comes from chance. Running every design with the same seeds is called **[common random numbers](../des/output-analysis.md#comparing-designs)**. The difference in waits has a half-width of 0.31 seconds, about the same as the half-width of the three-packer wait, 0.32 seconds. With four packers the wait hardly varies, so the difference varies about as much as the three-packer wait does.

"2 shared" says both designs drew from the same two random streams, `arrival.orders` and `service.packing`. desflow records which streams each run drew from. It lists any stream that only one design used, such as the stream of a gift-wrap step that only one design has, because draws from such a stream are not paired. With `pairing: independent` each design gets seeds of its own instead. A comparison is then refused, because no seed pairs one run of each design.

---

## Check the Answer

A check states what the answer must be. This one asks for a mean wait under half a second with four packers:

=== "YAML"

    ```yaml title="docs/snippets/yaml/comparing_designs_experiment.yaml:checks"
    checks:
      # The upper end of the interval must be under half a second
      - {measure: tasks.pack_order.wait_mean, variant: four, max: 0.5}
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/comparing_designs.py:checks"
    # The upper end of the interval must be under half a second
    "checks": [
        {"measure": "tasks.pack_order.wait_mean", "variant": "four", "max": 0.5}
    ],
    ```

=== "Low-level"

    The same key, in the same mapping.

A check compares its bound with the end of the interval that is nearer to failing it, here the upper end. A mean that is below the bound only by chance has an upper end above the bound, so it does not pass. The upper end is 0.286 seconds, so the check passes. When a check fails, `desflow experiment` still writes every file, then exits with status 1, and `run_experiment` raises `CheckFailed`. Either way, a script or a CI job stops at the failed check.

---

## Read the Report

The experiment writes `report.md` beside its results. It states the question, the model and its versions, the assumptions, each input distribution with where it came from, the run settings, the counts, the results, the comparison and the checks, and what the experiment does not state. The experiment file names its assumptions, the source of its inputs and its limits:

```yaml title="docs/snippets/yaml/comparing_designs_experiment.yaml:report"
assumptions:
  - Orders arrive at random, at a steady 1.8 a second, through the busy hour.
  - The 40 packing times measured at the bench stand for every packer.
inputs:
  Shop.service.packing: 40 packing times measured at the bench, in seconds
limits:
  - One busy hour only; the rest of the day is not modelled.
output: results/four-packers
```

The experiment states where the packing times came from but not the arrival rate, so the report's Missing Inputs section says: "The provenance of `Shop.arrival.orders` is not stated." It names the gap rather than filling it in. [Experiments](../user-guide/experiments/index.md) lists every file and section.

---

## Result

The mean wait of each design over its 20 replications, with its 95% interval:

![Bars of the busy hour's mean wait over 20 replications of each design with the same seeds: 2.48 ± 0.32 seconds with three packers and 0.27 ± 0.01 seconds with four, which is under the dashed half-second bound](../assets/charts/comparing-waits.png)

Does a fourth packer cut the mean wait, and by how much? Yes. Over 20 replications of each design with the same seeds, a fourth packer cuts the busy hour's mean wait by **2.21 ± 0.31 seconds**, from 2.48 to 0.27 seconds. The packers are busy **22.63 ± 0.17 percentage points** less of the time. The whole interval of the difference is below zero, and the four-packer wait stays under the half-second bound.

## Why It Works

Two designs run with different seeds differ by the design and by chance, and the chance part can hide a small effect. When both designs run with the same seeds, each pair of runs has the same arrivals and the same packing times. The chance part then partly cancels in their difference, so the interval on the differences is narrower than the interval an unpaired comparison of the same runs gives. Each difference comes from a seed of its own, so the 20 differences are independent of each other. The interval is therefore a [Student t interval](../des/output-analysis.md#interval-and-half-width) on the 20 differences, as in [Output Analysis](../des/output-analysis.md#comparing-designs).

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/comparing_designs_experiment.yaml"
        # The Comparing Designs tutorial: does a fourth packer cut the busy hour's mean wait?
        # Run it with: desflow experiment comparing_designs_experiment.yaml
        # Each design separately: change pairing to independent and remove compare.
        experiment: four-packers
        question: Does a fourth packer cut the mean wait of the busy hour, and by how much?
        # The busy hour of Trusting Results, with its 2-minute warm-up
        model: trusting_results.yaml
        until: 1 h
        replications: 20
        seed: 42
        # Every design runs with the same 20 seeds, so they are compared seed by seed
        pairing: common
        measures: [tasks.pack_order.wait_mean, resources.packers.utilization_avg]
        variants:
          three: {}
          four: {Shop.resources.packers.current_cap: 4}
        compare:
          - {baseline: three, variant: four}
        checks:
          # The upper end of the interval must be under half a second
          - {measure: tasks.pack_order.wait_mean, variant: four, max: 0.5}
        assumptions:
          - Orders arrive at random, at a steady 1.8 a second, through the busy hour.
          - The 40 packing times measured at the bench stand for every packer.
        inputs:
          Shop.service.packing: 40 packing times measured at the bench, in seconds
        limits:
          - One busy hour only; the rest of the day is not modelled.
        output: results/four-packers
        ```

        The model, `trusting_results.yaml`, is in the full files of [Trusting Results](trusting-results.md#full-files).

    === "Builder"

        ```python title="docs/snippets/tutorials/comparing_designs.py"
        """The Comparing Designs tutorial: a fourth packer, builder.

        Runs the busy hour of Trusting Results 20 times with three packers and 20 times with
        four, with the same seeds, and compares the two designs seed by seed. The experiment
        writes its artifacts and report to results/four-packers, and the script prints the
        comparison and the check.
        """

        import logging

        from desflow import run_experiment
        from trusting_results import shop

        # The imported model logs at INFO; an experiment logs only its warnings
        logging.getLogger("desflow").setLevel(logging.WARNING)

        EXPERIMENT = {
            "experiment": "four-packers",
            "question": "Does a fourth packer cut the mean wait of the busy hour, and by how much?",
            # A function returning a new busy hour, with its 2-minute warm-up
            "model": shop,
            "until": "1 h",
            "replications": 20,
            "seed": 42,
            # Every design runs with the same 20 seeds, so they are compared seed by seed
            "pairing": "common",
            "measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
            "variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
            "compare": [{"baseline": "three", "variant": "four"}],
            # The upper end of the interval must be under half a second
            "checks": [
                {"measure": "tasks.pack_order.wait_mean", "variant": "four", "max": 0.5}
            ],
            "assumptions": [
                "Orders arrive at random, at a steady 1.8 a second, through the busy hour.",
                "The 40 packing times measured at the bench stand for every packer.",
            ],
            "inputs": {
                "Shop.service.packing": "40 packing times measured at the bench, in seconds"
            },
            "limits": ["One busy hour only; the rest of the day is not modelled."],
            "output": "results/four-packers",
        }


        def experiment(pairing="common", output=None):
            """Runs the experiment; independent pairing gives each design seeds of its own."""
            settings = dict(EXPERIMENT, pairing=pairing)
            if pairing == "independent":
                settings["compare"] = []  # a comparison needs the same seeds
            return run_experiment(settings, output=output)


        if __name__ == "__main__":
            result = experiment()
            for row in result.comparisons:
                print(f"{row['measure']}: {row['difference']:.2f} ± {row['half_width']:.2f}")
            for row in result.checks:
                print(f"check {row['design']}: {row['value']:.3f} passed={row['passed']}")
        ```

        It imports from `trusting_results.py`, which is in the full files of [Trusting Results](trusting-results.md#full-files).

    === "Low-level"

        ```python title="docs/snippets/tutorials/comparing_designs_low_level.py"
        """The Comparing Designs tutorial: a fourth packer, low-level.

        Runs the busy hour of Trusting Results 20 times with three packers and 20 times with
        four, with the same seeds, and compares the two designs seed by seed. The experiment
        writes its artifacts and report to results/four-packers, and the script prints the
        comparison and the check.
        """

        import logging

        from desflow import LowLevelModel, run_experiment
        from trusting_results_low_level import WARMUP, model

        # The imported model logs at INFO; an experiment logs only its warnings
        logging.getLogger("desflow").setLevel(logging.WARNING)

        EXPERIMENT = {
            "experiment": "four-packers",
            "question": "Does a fourth packer cut the mean wait of the busy hour, and by how much?",
            # The function that builds the busy hour, and its 2-minute warm-up
            "model": LowLevelModel(model, sim_id="Shop"),
            "warmup": WARMUP,
            "until": 3600,
            "replications": 20,
            "seed": 42,
            "pairing": "common",
            "measures": ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"],
            "variants": {"three": {}, "four": {"Shop.resources.packers.current_cap": 4}},
            "compare": [{"baseline": "three", "variant": "four"}],
            "checks": [
                {"measure": "tasks.pack_order.wait_mean", "variant": "four", "max": 0.5}
            ],
            "assumptions": [
                "Orders arrive at random, at a steady 1.8 a second, through the busy hour.",
                "The 40 packing times measured at the bench stand for every packer.",
            ],
            "inputs": {
                "Shop.service.packing": "40 packing times measured at the bench, in seconds"
            },
            "limits": ["One busy hour only; the rest of the day is not modelled."],
            "output": "results/four-packers",
        }


        def experiment(pairing="common", output=None):
            """Runs the experiment; independent pairing gives each design seeds of its own."""
            settings = dict(EXPERIMENT, pairing=pairing)
            if pairing == "independent":
                settings["compare"] = []  # a comparison needs the same seeds
            return run_experiment(settings, output=output)


        if __name__ == "__main__":
            result = experiment()
            for row in result.comparisons:
                print(f"{row['measure']}: {row['difference']:.2f} ± {row['half_width']:.2f}")
            for row in result.checks:
                print(f"check {row['design']}: {row['value']:.3f} passed={row['passed']}")
        ```

        It imports from `trusting_results_low_level.py`, which is in the full files of [Trusting Results](trusting-results.md#full-files).

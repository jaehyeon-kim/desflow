# Comparing Designs

[Trusting Results](trusting-results.md) measured the busy hour with three packers: over 40 replications the mean wait was 2.32 ± 0.20 seconds. The shop could hire a fourth packer. This page asks: **does a fourth packer cut the mean wait, and by how much?** It runs both designs as an experiment, with the same seeds, and compares them seed by seed.

## What You Add

An experiment: a file in YAML, or a mapping in Python, that names the model, the two designs, the comparison and a check. The model is the busy hour of Trusting Results, unchanged.

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

    `shop` is the function of Trusting Results that returns a new busy hour; the experiment calls it once per run.

=== "Low-level"

    ```python title="docs/snippets/tutorials/comparing_designs_low_level.py:model"
    # The function that builds the busy hour, and its 2-minute warm-up
    "model": LowLevelModel(model, sim_id="Shop"),
    "warmup": WARMUP,
    "until": 3600,
    "replications": 20,
    "seed": 42,
    ```

    The low-level model has no warm-up of its own, so the experiment gives it one.

---

## Compare Two Designs

Each variant names the registry values it changes before the clock starts. `three` changes nothing, so it is the busy hour as written; `four` sets the packers' capacity to 4. `pairing: common` gives both designs the same 20 seeds, and `compare` asks for their difference seed by seed:

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

The experiment runs 40 replications, 20 of each design, and writes what it found to `results/four-packers`. Each design's results, with the 95% half-width:

| Design | Mean wait (s) | Utilisation (%) |
|---|---|---|
| three | 2.48 ± 0.32 | 90.33 ± 0.63 |
| four | 0.27 ± 0.01 | 67.70 ± 0.46 |

The three-packer design ran with the same first 20 seeds as Trusting Results, so its row is that page's 20-replication row. The comparison, the four-packer value minus the three-packer value, seed by seed:

| Measure | Difference | Half-width | Random streams |
|---|---|---|---|
| `tasks.pack_order.wait_mean` | -2.21 s | 0.31 s | 2 shared |
| `resources.packers.utilization_avg` | -22.63 points | 0.17 points | 2 shared |

The difference in utilisation is known to within 0.17 points, though each design's own utilisation is known only to within 0.63 and 0.46. The same seed gives both designs the same arrivals and packing times, so a busy run of one design is a busy run of the other, and their difference leaves most of the chance out. This is called **common random numbers**. The difference in waits is known to within 0.31 seconds, about as well as the three-packer wait itself, because with four packers the wait hardly varies.

"2 shared" says both designs drew from the same two random streams, `arrival.orders` and `service.packing`. desflow records which streams each run drew from and lists any that only one design used, such as a gift-wrap step only one design has, because draws from such a stream are not paired. With `pairing: independent` each design gets seeds of its own instead, and a comparison is refused, since no seed then pairs one run of each design.

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

A check compares the far end of the interval with its bound, the upper end here, so a mean that is below the bound only by chance does not pass. The upper end is 0.286 seconds, so the check passes. When a check fails, `desflow experiment` still writes every file and then exits with status 1, and `run_experiment` raises `CheckFailed`, so a script or a CI job stops on it.

---

## Read the Report

The experiment writes `report.md` beside its results. It states the question, the model and its versions, the assumptions, each input distribution with where it came from, the run settings, the counts, the results, the comparison and the checks, and what the experiment does not state. The experiment names its assumptions, the source of its inputs and its limits:

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

Does a fourth packer cut the mean wait, and by how much? Yes: with the same seeds, a fourth packer cuts the busy hour's mean wait by **2.21 ± 0.31 seconds**, from 2.48 to 0.27 seconds, and the packers are busy **22.63 ± 0.17 points** less of the time. The whole interval of the difference is below zero, and the four-packer wait stays under the half-second bound.

## Why It Works

Two designs run with different seeds differ by the design and by chance, and the chance part can hide a small effect. Run with the same seeds, each pair of runs meets the same arrivals and the same packing times, so the chance part partly cancels in their difference, and the interval on the differences is narrower than the interval an unpaired comparison of the same runs gives. The interval is a Student t interval on the 20 differences, as in [Output Analysis](../des/output-analysis.md#comparing-designs), because each difference comes from a seed of its own.

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

        The busy hour of Trusting Results, run 20 times with three packers and 20 times with
        four, with the same seeds, and compared seed by seed. The experiment writes its artifacts
        and report to results/four-packers, and the script prints the comparison and the
        check.
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

        The busy hour of Trusting Results, run 20 times with three packers and 20 times with
        four, with the same seeds, and compared seed by seed. The experiment writes its artifacts
        and report to results/four-packers, and the script prints the comparison and the
        check.
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

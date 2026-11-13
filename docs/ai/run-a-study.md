# Run a Study

Ask for a study when you need an answer you can act on. One run is one sample, so an assistant with the skill answers from replications, with a confidence interval, and compares designs with the same seeds.

---

## Ask

```text
Does a second packer cut the mean wait of drafted_shop.yaml, and by how much?
Run a study, and give the difference with its interval.
```

---

## What the Assistant Does

The skill gives the assistant these steps:

1. Set a seed, so the study can be repeated.
2. Set a warm-up when the model needs time to reach its usual state.
3. Run replications. Each has its own seed, spawned from the study's seed.
4. Report each mean with its 95% confidence interval.
5. To compare designs, write an experiment file and run `desflow experiment`. It runs every design with the same seeds, compares them seed by seed, and writes `report.md`.

For the drafted shop, the experiment runs one and two packers for an hour, 20 times each:

=== "YAML"

    ```yaml title="docs/snippets/ai/drafted_shop_experiment.yaml:study"
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

=== "Builder"

    `run_experiment` runs the same study in Python, as [Comparing Designs](../learn/studies/comparing-designs.md) shows.

=== "Low-level"

    `run_experiment` runs the same study in Python, as [Comparing Designs](../learn/studies/comparing-designs.md) shows.

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

---

## Check the Reply

Check that the answer is an interval, not one number, and that both designs ran with the same seeds. Read the report's Missing Inputs section: it names each input whose source the experiment does not state. [Trusting Results](../learn/studies/trusting-results.md) explains how many replications and how long a warm-up a study needs, and [Experiments](../user-guide/results/experiments.md) lists every file the experiment writes.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/ai/drafted_shop_experiment.yaml"
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

    === "Builder"

        [Comparing Designs](../learn/studies/comparing-designs.md#full-files) has a study in Python with `run_experiment`.

    === "Low-level"

        [Comparing Designs](../learn/studies/comparing-designs.md#full-files) has a study in Python with `run_experiment`.

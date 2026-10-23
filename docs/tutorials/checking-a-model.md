# Checking a Model

[Comparing Designs](comparing-designs.md) chose a fourth packer from the busy hour model of [Trusting Results](trusting-results.md), and [Parameter Sweeps](parameter-sweeps.md) found the choice holds for rates from 1.6 to 1.9 orders a second. Both answers are only as good as the model. This page asks: **does the program do what the model on paper says, and is that model good enough to decide the number of packers?** The decision is three packers or four. The measures are the mean wait and the packers' utilisation. The boundary is the busy hour, after its 2-minute warm-up. The assumptions are those of Trusting Results: orders at random at 1.8 a second, and the 40 packing times measured at the bench.

## What You Add

Checks on the model, written as tests that fail when a check fails: a run with fixed inputs against a trace worked by hand, runs at the extremes against estimates made by hand, Little's law and the queueing formulas of `desflow explain --verify`, a comparison with values observed in the real shop, and a check that the decision survives a higher rate. [Checking and Validating](../des/checking-and-validating.md) explains each kind of check; this page applies them to the shop.

---

## Trace Fixed Inputs

Replace every random input with a constant, and the run has one answer that can be worked out by hand. Three packers, an order every second and 4 seconds to pack each, for 10 seconds:

=== "YAML"

    ```yaml title="docs/snippets/yaml/checking_a_model.yaml:fixed"
    # Three packers, an order every second and 4 seconds to pack each
    resources:
      packers: {current_cap: 3, max_cap: 10}

    arrivals:
      orders: {dist: constant, value: 1, spawn: pack_order}

    services:
      packing: {dist: constant, value: 4}
    ```

    `desflow run checking_a_model.yaml` prints the 20 records of the run, which are its trace, then the statistics at 10 seconds.

=== "Builder"

    ```python title="docs/snippets/tutorials/checking_a_model.py:fixed"
    def fixed_shop() -> SimulationContext:
        """Three packers, an order every second and 4 seconds to pack each, for 10 s."""
        app = (
            SimulationContext(sim_id="Shop", factor=0, random_seed=42)
            .add_resource("packers", current_cap=3, max_cap=10)
            .add_arrival("orders", dist="constant", value=1)
            .add_service("packing", dist="constant", value=4)
            .with_stats("typical", interval=3600)
        )
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/checking_a_model_low_level.py:fixed"
    # Three packers, an order every second and 4 seconds to pack each
    FIXED = SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="constant", value=1)},
        service={"packing": DistributionConfig(dist="constant", value=4)},
        resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
    )
    ```

    The full file reuses `order_generator` and `pack_order` from Trusting Results: SimPy's `env.timeout(...)` makes each gap, and `packers.request()` waits for a free packer, as [SimPy and desflow](../des/simpy-and-desflow.md) describes.

Worked by hand, orders arrive at 1, 2, ... 9 seconds. The first three start at once. Each later order starts when a packer frees, 4 seconds after that packer started. The run stops at 10 seconds, before the events at that time:

| Order | Arrives | Starts | Waits | Finishes |
|---|---|---|---|---|
| 1, 2, 3 | 1, 2, 3 | 1, 2, 3 | 0 | 5, 6, 7 |
| 4 | 4 | 5 | 1 | 9 |
| 5, 6 | 5, 6 | 6, 7 | 1 | after 10 |
| 7 | 7 | 9 | 2 | after 10 |
| 8, 9 | 8, 9 | not by 10 | | |

So **4** orders finish and **5** are still in the shop. Seven orders started packing, and all 9 that arrived count in `flows.orders.started`. The seven have waits that add up to 5 seconds, so the mean wait is 5 / 7 = **0.714** seconds and the longest is **2**. The packers are busy 24 of their 30 seconds, **80%**, and the queue holds 8 order-seconds in 10 seconds, an average of **0.8**. The run reports the same six numbers. A difference here is a bug in the program or in the trace by hand, and either one needs finding before any experiment.

---

## Push to the Extremes

A fixed trace checks the logic, not the busy hour itself. Run the busy hour with settings whose results can be estimated without the model. The checks run as an [experiment](../user-guide/experiments/index.md) on the busy hour, 20 replications of each design:

=== "YAML"

    ```yaml title="docs/snippets/yaml/checking_a_model_experiment.yaml:model"
    # The busy hour of Trusting Results, with its 2-minute warm-up
    model: trusting_results.yaml
    until: 1 h
    replications: 20
    seed: 42
    measures:
      - tasks.pack_order.wait_mean
      - resources.packers.utilization_avg
      - resources.packers.queue_length_avg
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/checking_a_model.py:model"
    # The busy hour of Trusting Results, with its 2-minute warm-up
    "model": shop,
    "until": "1 h",
    "replications": 20,
    "seed": 42,
    "measures": [
        "tasks.pack_order.wait_mean",
        "resources.packers.utilization_avg",
        "resources.packers.queue_length_avg",
    ],
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/checking_a_model_low_level.py:model"
    # The function that builds the busy hour, and its 2-minute warm-up
    "model": LowLevelModel(model, sim_id="Shop"),
    "warmup": WARMUP,
    "until": 3600,
    "replications": 20,
    "seed": 42,
    "measures": [
        "tasks.pack_order.wait_mean",
        "resources.packers.utilization_avg",
        "resources.packers.queue_length_avg",
    ],
    ```

The summary also has the task's counts, such as `tasks.pack_order.finished`, without naming them. The designs are variants of the number of packers:

=== "YAML"

    ```yaml title="docs/snippets/yaml/checking_a_model_experiment.yaml:extremes"
    # One packer cannot keep up, and ten are mostly idle
    three: {}
    one: {Shop.resources.packers.current_cap: 1}
    ten: {Shop.resources.packers.current_cap: 10}
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/checking_a_model.py:extremes"
    # One packer cannot keep up, and ten are mostly idle
    "variants": {
        "three": {},
        "one": {"Shop.resources.packers.current_cap": 1},
        "ten": {"Shop.resources.packers.current_cap": 10},
    ```

=== "Low-level"

    The same keys, in the same mapping as the model.

One packer takes 1.5 seconds an order on average, so it can finish at most 3,480 / 1.5 = 2,320 orders in the 3,480 seconds after the warm-up. The run finishes **2,318**, with the packer busy **100%** of the time and a mean wait of about **1,179** seconds. Ten packers would be busy 1.8 × 1.5 / 10 = 27% of the time; the run measures **27.08%**, and the mean wait rounds to **0.00** seconds. Turn each extreme into a check, so a later change that breaks it fails. A check tests the far end of the interval, so it passes only when the whole interval is within the bound:

=== "YAML"

    ```yaml title="docs/snippets/yaml/checking_a_model_experiment.yaml:extreme-checks"
    checks:
      # One packer is busy all the time: the lower end of the interval is 99.9% or more
      - {measure: resources.packers.utilization_avg, variant: one, min: 99.9}
      # With ten packers almost no order waits
      - {measure: tasks.pack_order.wait_mean, variant: ten, max: 0.01}
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/checking_a_model.py:extreme-checks"
    "checks": [
        # One packer is busy all the time: the lower end of the interval is 99.9% or more
        {
            "measure": "resources.packers.utilization_avg",
            "variant": "one",
            "min": 99.9,
        },
        # With ten packers almost no order waits
        {"measure": "tasks.pack_order.wait_mean", "variant": "ten", "max": 0.01},
    ```

=== "Low-level"

    The same keys, in the same mapping as the model.

---

## Check It Against Theory

Two checks compare the run with queueing theory: Little's law, which holds for any model that settles, and the formulas `desflow explain` applies where their assumptions hold.

### Little's Law

[Little's law](../des/queueing-basics.md#littles-law) says the average queue equals the arrival rate times the mean wait. The experiment measures the three separately, so they test each other. The arrival rate is the orders that arrived after the warm-up, `flows.orders.started`, divided by the 3,480 seconds:

=== "YAML"

    Read the means of `flows.orders.started`, `resources.packers.queue_length_avg` and `tasks.pack_order.wait_mean` from `summary.csv` or the report, and multiply by hand.

=== "Builder"

    ```python title="docs/snippets/tutorials/checking_a_model.py:little"
    HORIZON = 3600 - 120  # the measured part of the hour, after the warm-up, in seconds


    def littles_law(result, design):
        """The mean queue, and the arrival rate and mean wait whose product it should be."""
        means = {r["statistic"]: r["mean"] for r in result.summary if r["design"] == design}
        rate = means["flows.orders.started"] / HORIZON
        return (
            means["resources.packers.queue_length_avg"],
            rate,
            means["tasks.pack_order.wait_mean"],
        )
    ```

=== "Low-level"

    The same function, with `HORIZON = 3600 - WARMUP`.

With three packers the queue averages **4.488** orders, and 1.8065 orders a second × 2.479 seconds = **4.478**. They agree. With one packer they also roughly agree: a queue of **2,127.6** against 1.8065 × 1,179.0 = **2,129.9**. One packer's queue grows through the whole run and never settles, so its mean depends on how long the run lasts, and the agreement there proves less than it does for three packers. Agreement with the law shows the statistics are consistent with each other. It does not show the model is right for the shop: a model with the wrong packing times would agree just as well.

### Queueing Formulas With explain

`desflow explain --verify` puts the queueing formulas beside runs of the model. It works out what the formulas give for each resource, runs the model unpaced, and marks each pair as agreeing or differing. Here it runs the busy hour of Trusting Results ten times:

=== "YAML"

    ```bash
    desflow explain trusting_results.yaml --verify --replications 10
    ```

=== "Builder"

    `context.analyze(verify=True, replications=10)` returns the same comparison for a context loaded from a blueprint, or from a builder file with `load_context`. A context built in a script's own code cannot be built again for the runs, so it refuses `verify=True`; use `desflow explain --verify` on the file.

=== "Low-level"

    A low-level model declares no link between its arrivals, tasks and resources, so `explain` cannot analyse it. Its Little's law check above is the theory check that applies.

The end of its output:

<!-- records: checking-explain -->
```text
Not estimated:
  wait at packers: no closed formula for c > 1 with non-exponential times; run --verify
Hints for utilisation 0.85:
  packers with 4 units: utilisation 0.675, at most 0.85
  packers with 3 units: utilisation 0.85 at 1.7 requests a second
Verify: 10 runs to 3600 s, seed 42
+---------------------------------------------------------------------------+
| node       | measure      | analysis | measured |        ± | result       |
|------------+--------------+----------+----------+----------+--------------|
| packers    | utilization  |      0.9 |   0.9034 | 0.009148 | agrees       |
| packers    | queue_length |        - |    4.066 |    0.469 | not compared |
| pack_order | wait         |        - |    2.252 |   0.2476 | not compared |
| orders     | throughput   |      1.8 |    1.807 |  0.01546 | agrees       |
+---------------------------------------------------------------------------+
Not compared:
  packers queue_length: no closed formula for c > 1 with non-exponential times
  pack_order wait: no closed formula for c > 1 with non-exponential times
--verify checks the implementation against the queueing analysis where it applies; it does not show that the model is close enough to the real system
```

The utilisation of 0.9 and the throughput of 1.8 orders a second agree with the runs. The wait and the queue length have no analytical value, and the `Not estimated` line says why: three packers with measured packing times have no closed formula. For them the interval of the experiment is the check, as the next section shows. Agreement shows the program computes what the model on paper says; it says nothing about whether the model is close enough to the real shop. [Check Against Theory](../guides/check-against-theory.md) runs the same check on a shop the formulas describe in full.

---

## Compare With the Real Shop

Only the real shop can show the model is adequate. The observed values below are illustrative; a real check uses the shop's own records. Say the shop recorded a mean wait of 2.6 seconds and a utilisation of 91% over 58 minutes of a busy hour with three packers. [`checking_a_model_observed.csv`](https://raw.githubusercontent.com/jaehyeon-kim/desflow/main/docs/snippets/yaml/checking_a_model_observed.csv) holds them, with the unit, population and horizon of each, as [Experiment Results](../user-guide/experiments/experiment-results.md#compare-with-observed-values) describes; download it into the folder that holds the experiment and the scripts. The experiment places each value beside the three-packer interval:

=== "YAML"

    ```yaml title="docs/snippets/yaml/checking_a_model_experiment.yaml:observed"
    # Illustrative values for the real shop, which has three packers;
    # a real check reads the shop's own records
    observed: {file: checking_a_model_observed.csv, variant: three}
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/checking_a_model.py:observed"
    # Illustrative values for the real shop, which has three packers;
    # a real check reads the shop's own records
    "observed": {
        "file": str(HERE / "checking_a_model_observed.csv"),
        "variant": "three",
    },
    ```

=== "Low-level"

    The same key, in the same mapping as the model.

The busy-hour wait with three packers and measured packing times has no exact formula in [Queueing Basics](../des/queueing-basics.md), so the experiment's interval is the check. The interval covers only the model's spread from run to run, not the error in the shop's own measurement. One rule applies to both measures: a value outside its interval is a disagreement that the study must explain before it trusts the model for the decision. The observed mean wait of 2.6 seconds is inside its interval, **2.16 to 2.80**, so the wait agrees. The observed utilisation of 91% is outside its interval, **89.70 to 90.96**, so the utilisation is a disagreement to explain, for example by checking how the shop measured it or whether the packing times are right.

---

## Test the Sensitivity

[Parameter Sweeps](parameter-sweeps.md) found that the four-packer wait stays low at every rate. Keep that as a check, so a change to the model that breaks it fails. Add the chosen design at a rate 6% above the estimate:

=== "YAML"

    ```yaml title="docs/snippets/yaml/checking_a_model_experiment.yaml:sensitivity"
    # The design chosen in Comparing Designs, at a rate 6% above the estimate
    four-busier: {Shop.resources.packers.current_cap: 4, Shop.arrival.orders.rate: 1.9}
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/checking_a_model.py:sensitivity"
    # The design chosen in Comparing Designs, at a rate 6% above the estimate
    "four-busier": {
        "Shop.resources.packers.current_cap": 4,
        "Shop.arrival.orders.rate": 1.9,
    },
    ```

=== "Low-level"

    The same keys, in the same mapping as the model.

Then bound it:

=== "YAML"

    ```yaml title="docs/snippets/yaml/checking_a_model_experiment.yaml:sensitivity-check"
    # Four packers keep the mean wait under half a second at the higher rate
    - {measure: tasks.pack_order.wait_mean, variant: four-busier, max: 0.5}
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/checking_a_model.py:sensitivity-check"
    # Four packers keep the mean wait under half a second at the higher rate
    {
        "measure": "tasks.pack_order.wait_mean",
        "variant": "four-busier",
        "max": 0.5,
    },
    ```

=== "Low-level"

    The same keys, in the same mapping as the model.

At 1.9 orders a second four packers wait **0.35 ± 0.02** seconds, so the upper end, 0.37, is under the bound of 0.5.

---

## Run the Checks

Run the checks after every change to the model, in the same way as the tests of any program:

=== "YAML"

    ```bash
    desflow experiment checking_a_model_experiment.yaml
    ```

    The command writes the results and the report, then exits with status 1 when a check fails, so a CI job that runs it fails too. Run `desflow run checking_a_model.yaml` and compare its statistics with the table above after a change to the model's logic.

=== "Builder"

    ```python title="docs/snippets/tutorials/checking_a_model.py:tests"
    def test_fixed_inputs_match_the_hand_trace():
        readings = fixed_run()
        assert readings["tasks.pack_order.finished"] == 4
        assert readings["tasks.pack_order.in_progress"] == 5
        assert readings["tasks.pack_order.wait_mean"] == 5 / 7
        assert readings["tasks.pack_order.wait_max"] == 2


    def test_extremes_sensitivity_and_littles_law(tmp_path):
        result = experiment(output=tmp_path)  # a failed check raises CheckFailed
        queue, rate, wait = littles_law(result, "three")
        assert abs(queue - rate * wait) < 0.05 * queue
    ```

    ```bash
    pytest checking_a_model.py
    ```

    pytest collects the `test_` functions of a file named on its command line. `run_experiment` raises `CheckFailed` when a check fails, which fails the second test.

=== "Low-level"

    The same two tests, in the full file:

    ```bash
    pytest checking_a_model_low_level.py
    ```

---

## Result

Does the program do what the model on paper says, and is the model good enough to decide the number of packers? The program does what the model on paper says. The fixed run matches the trace by hand: **4** orders finished, **5** still in the shop and a mean wait of **0.714** seconds. One packer finishes **2,318** orders against 2,320 by hand, and ten are busy **27.08%** of the time against 27%. Little's law holds for three packers, **4.488** against 4.478, and the measured utilisation agrees with the formula's 0.9. Four packers stay at **0.35 ± 0.02** seconds at a rate 6% higher, and all three checks pass. The observed values are illustrative; a real check uses the shop's own records. Against them the mean wait of 2.6 seconds lies inside the simulated **2.16 to 2.80**, but the utilisation of 91% lies outside **89.70 to 90.96**. The model is good enough for the decision only once the study has explained that disagreement.

## Why It Works

The trace, the extremes, Little's law and the queueing formulas test the program against the model on paper: verification, in the terms of [Checking and Validating](../des/checking-and-validating.md#verification-and-validation). The observed values and the sensitivity check test the model against the shop and the decision: validation. Written as tests, the checks run again after every change, so a model that was checked once stays checked.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/checking_a_model.yaml"
        # The Checking a Model tutorial: the shop with fixed inputs, to compare with a trace
        # worked by hand. Run it with: desflow run checking_a_model.yaml
        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42

        # Three packers, an order every second and 4 seconds to pack each
        resources:
          packers: {current_cap: 3, max_cap: 10}

        arrivals:
          orders: {dist: constant, value: 1, spawn: pack_order}

        services:
          packing: {dist: constant, value: 4}

        tasks:
          pack_order:
            service: packing
            resource: packers
            payload: {event_type: order_packed}

        # Every record is the trace; the statistics are published once, at the end
        stats: {level: typical, interval: 1 h}

        egress:
          - type: Console

        run:
          until: 10
        ```

        ```yaml title="docs/snippets/yaml/checking_a_model_experiment.yaml"
        # The Checking a Model tutorial: extremes, sensitivity and the real shop, as checks.
        # Run it with: desflow experiment checking_a_model_experiment.yaml
        # It exits 1 when a check fails, after writing every file.
        experiment: checking-a-model
        question: Is the busy hour model adequate for choosing the number of packers?
        # The busy hour of Trusting Results, with its 2-minute warm-up
        model: trusting_results.yaml
        until: 1 h
        replications: 20
        seed: 42
        measures:
          - tasks.pack_order.wait_mean
          - resources.packers.utilization_avg
          - resources.packers.queue_length_avg
        variants:
          # One packer cannot keep up, and ten are mostly idle
          three: {}
          one: {Shop.resources.packers.current_cap: 1}
          ten: {Shop.resources.packers.current_cap: 10}
          # The design chosen in Comparing Designs, at a rate 6% above the estimate
          four-busier: {Shop.resources.packers.current_cap: 4, Shop.arrival.orders.rate: 1.9}
        checks:
          # One packer is busy all the time: the lower end of the interval is 99.9% or more
          - {measure: resources.packers.utilization_avg, variant: one, min: 99.9}
          # With ten packers almost no order waits
          - {measure: tasks.pack_order.wait_mean, variant: ten, max: 0.01}
          # Four packers keep the mean wait under half a second at the higher rate
          - {measure: tasks.pack_order.wait_mean, variant: four-busier, max: 0.5}
        # Illustrative values for the real shop, which has three packers;
        # a real check reads the shop's own records
        observed: {file: checking_a_model_observed.csv, variant: three}
        output: results/checking-a-model
        ```

        The experiment runs `trusting_results.yaml`, which is in the full files of [Trusting Results](trusting-results.md#full-files).

    === "Builder"

        ```python title="docs/snippets/tutorials/checking_a_model.py"
        """The Checking a Model tutorial: checks of the busy hour, written as tests, builder.

        A run of the shop with fixed inputs is compared with a trace worked by hand. An
        experiment then runs the busy hour of Trusting Results with one, three and ten packers
        and with four packers at a higher rate, checks each against a bound, and places the
        three-packer design beside the values observed in the real shop. Run the checks with:
        pytest checking_a_model.py
        """

        import logging
        from pathlib import Path

        from desflow import SimulationContext, run_experiment
        from trusting_results import shop

        # The imported model logs at INFO; the checks log only their warnings
        logging.getLogger("desflow").setLevel(logging.WARNING)

        HERE = Path(__file__).resolve().parent


        def fixed_shop() -> SimulationContext:
            """Three packers, an order every second and 4 seconds to pack each, for 10 s."""
            app = (
                SimulationContext(sim_id="Shop", factor=0, random_seed=42)
                .add_resource("packers", current_cap=3, max_cap=10)
                .add_arrival("orders", dist="constant", value=1)
                .add_service("packing", dist="constant", value=4)
                .with_stats("typical", interval=3600)
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


        def fixed_run() -> dict:
            """The statistics of the fixed shop after 10 seconds, without the Shop.stats. prefix."""
            app = fixed_shop()
            app.run(until=10)
            readings = app.env.stats.readings()
            return {key.removeprefix("Shop.stats."): value for key, value in readings.items()}


        EXPERIMENT = {
            "experiment": "checking-a-model",
            "question": "Is the busy hour model adequate for choosing the number of packers?",
            # The busy hour of Trusting Results, with its 2-minute warm-up
            "model": shop,
            "until": "1 h",
            "replications": 20,
            "seed": 42,
            "measures": [
                "tasks.pack_order.wait_mean",
                "resources.packers.utilization_avg",
                "resources.packers.queue_length_avg",
            ],
            # One packer cannot keep up, and ten are mostly idle
            "variants": {
                "three": {},
                "one": {"Shop.resources.packers.current_cap": 1},
                "ten": {"Shop.resources.packers.current_cap": 10},
                # The design chosen in Comparing Designs, at a rate 6% above the estimate
                "four-busier": {
                    "Shop.resources.packers.current_cap": 4,
                    "Shop.arrival.orders.rate": 1.9,
                },
            },
            "checks": [
                # One packer is busy all the time: the lower end of the interval is 99.9% or more
                {
                    "measure": "resources.packers.utilization_avg",
                    "variant": "one",
                    "min": 99.9,
                },
                # With ten packers almost no order waits
                {"measure": "tasks.pack_order.wait_mean", "variant": "ten", "max": 0.01},
                # Four packers keep the mean wait under half a second at the higher rate
                {
                    "measure": "tasks.pack_order.wait_mean",
                    "variant": "four-busier",
                    "max": 0.5,
                },
            ],
            # Illustrative values for the real shop, which has three packers;
            # a real check reads the shop's own records
            "observed": {
                "file": str(HERE / "checking_a_model_observed.csv"),
                "variant": "three",
            },
            "output": "results/checking-a-model",
        }


        def experiment(output=None):
            """Runs the checks; raises CheckFailed when one fails, after writing every file."""
            return run_experiment(EXPERIMENT, output=output)


        HORIZON = 3600 - 120  # the measured part of the hour, after the warm-up, in seconds


        def littles_law(result, design):
            """The mean queue, and the arrival rate and mean wait whose product it should be."""
            means = {r["statistic"]: r["mean"] for r in result.summary if r["design"] == design}
            rate = means["flows.orders.started"] / HORIZON
            return (
                means["resources.packers.queue_length_avg"],
                rate,
                means["tasks.pack_order.wait_mean"],
            )


        def test_fixed_inputs_match_the_hand_trace():
            readings = fixed_run()
            assert readings["tasks.pack_order.finished"] == 4
            assert readings["tasks.pack_order.in_progress"] == 5
            assert readings["tasks.pack_order.wait_mean"] == 5 / 7
            assert readings["tasks.pack_order.wait_max"] == 2


        def test_extremes_sensitivity_and_littles_law(tmp_path):
            result = experiment(output=tmp_path)  # a failed check raises CheckFailed
            queue, rate, wait = littles_law(result, "three")
            assert abs(queue - rate * wait) < 0.05 * queue


        if __name__ == "__main__":
            print(fixed_run())
            result = experiment()
            for design in ("three", "one"):
                queue, rate, wait = littles_law(result, design)
                print(
                    f"{design}: queue {queue:.3f}, {rate:.4f} x {wait:.3f} = {rate * wait:.3f}"
                )
            for row in result.observed:
                print(f"observed {row['measure']}: {row['value']} inside={row['inside']}")
        ```

        It imports from `trusting_results.py`, which is in the full files of [Trusting Results](trusting-results.md#full-files).

    === "Low-level"

        ```python title="docs/snippets/tutorials/checking_a_model_low_level.py"
        """The Checking a Model tutorial: checks of the busy hour, written as tests, low-level.

        A run of the shop with fixed inputs is compared with a trace worked by hand. An
        experiment then runs the busy hour of Trusting Results with one, three and ten packers
        and with four packers at a higher rate, checks each against a bound, and places the
        three-packer design beside the values observed in the real shop. Run the checks with:
        pytest checking_a_model_low_level.py
        """

        import logging
        from pathlib import Path

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            LowLevelModel,
            Sampler,
            SimParameter,
            run_experiment,
        )
        from trusting_results_low_level import WARMUP, model, order_generator

        # The imported model logs at INFO; the checks log only their warnings
        logging.getLogger("desflow").setLevel(logging.WARNING)

        HERE = Path(__file__).resolve().parent

        # Three packers, an order every second and 4 seconds to pack each
        FIXED = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="constant", value=1)},
            service={"packing": DistributionConfig(dist="constant", value=4)},
            resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
        )


        def fixed_run() -> dict:
            """The statistics of the fixed shop after 10 seconds, without the Shop.stats. prefix."""
            env = DynamicRealtimeEnvironment(factor=0.0)
            env.registry.register_sim_parameter(FIXED)
            env.setup_stats("Shop", "typical", interval=3600)
            packers = DynamicResource(env, "Shop", "packers")
            env.process(order_generator(env, Sampler(seed=42), packers))
            try:
                env.run(until=10)
            finally:
                env.teardown()
            readings = env.stats.readings()
            return {key.removeprefix("Shop.stats."): value for key, value in readings.items()}


        EXPERIMENT = {
            "experiment": "checking-a-model",
            "question": "Is the busy hour model adequate for choosing the number of packers?",
            # The function that builds the busy hour, and its 2-minute warm-up
            "model": LowLevelModel(model, sim_id="Shop"),
            "warmup": WARMUP,
            "until": 3600,
            "replications": 20,
            "seed": 42,
            "measures": [
                "tasks.pack_order.wait_mean",
                "resources.packers.utilization_avg",
                "resources.packers.queue_length_avg",
            ],
            "variants": {
                "three": {},
                "one": {"Shop.resources.packers.current_cap": 1},
                "ten": {"Shop.resources.packers.current_cap": 10},
                "four-busier": {
                    "Shop.resources.packers.current_cap": 4,
                    "Shop.arrival.orders.rate": 1.9,
                },
            },
            "checks": [
                {
                    "measure": "resources.packers.utilization_avg",
                    "variant": "one",
                    "min": 99.9,
                },
                {"measure": "tasks.pack_order.wait_mean", "variant": "ten", "max": 0.01},
                {
                    "measure": "tasks.pack_order.wait_mean",
                    "variant": "four-busier",
                    "max": 0.5,
                },
            ],
            # Illustrative values for the real shop, which has three packers;
            # a real check reads the shop's own records
            "observed": {
                "file": str(HERE / "checking_a_model_observed.csv"),
                "variant": "three",
            },
            "output": "results/checking-a-model",
        }


        def experiment(output=None):
            """Runs the checks; raises CheckFailed when one fails, after writing every file."""
            return run_experiment(EXPERIMENT, output=output)


        HORIZON = 3600 - WARMUP  # the measured part of the hour, in seconds


        def littles_law(result, design):
            """The mean queue, and the arrival rate and mean wait whose product it should be."""
            means = {r["statistic"]: r["mean"] for r in result.summary if r["design"] == design}
            rate = means["flows.orders.started"] / HORIZON
            return (
                means["resources.packers.queue_length_avg"],
                rate,
                means["tasks.pack_order.wait_mean"],
            )


        def test_fixed_inputs_match_the_hand_trace():
            readings = fixed_run()
            assert readings["tasks.pack_order.finished"] == 4
            assert readings["tasks.pack_order.in_progress"] == 5
            assert readings["tasks.pack_order.wait_mean"] == 5 / 7
            assert readings["tasks.pack_order.wait_max"] == 2


        def test_extremes_sensitivity_and_littles_law(tmp_path):
            result = experiment(output=tmp_path)  # a failed check raises CheckFailed
            queue, rate, wait = littles_law(result, "three")
            assert abs(queue - rate * wait) < 0.05 * queue


        if __name__ == "__main__":
            print(fixed_run())
            result = experiment()
            for design in ("three", "one"):
                queue, rate, wait = littles_law(result, design)
                print(
                    f"{design}: queue {queue:.3f}, {rate:.4f} x {wait:.3f} = {rate * wait:.3f}"
                )
            for row in result.observed:
                print(f"observed {row['measure']}: {row['value']} inside={row['inside']}")
        ```

        It imports from `trusting_results_low_level.py`, which is in the full files of [Trusting Results](trusting-results.md#full-files).

Next, [Live Changes](live-changes.md) changes a parameter while the shop runs.

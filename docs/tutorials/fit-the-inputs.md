# Fit the Inputs

[Choosing Inputs](choosing-inputs.md) compared four candidates for the packing time by hand and chose the 40 times measured at the bench. desflow can fit every distribution family to those times and say which fits best. This page asks: **which supported distribution fits the 40 packing times, and does the choice change the busy hour's answer?** The decision is which fitted distribution the busy hour uses for packing. The measures are how well each distribution fits the times, and the busy hour's mean wait with the two best fits and with the 40 times. The boundary is the busy hour with 3 packers, run as [Does the Choice Matter?](#does-the-choice-matter) sets out. The assumptions are that orders arrive at random at 1.8 a second, and that the 40 times stand for every packer.

## What You Add

This page runs `desflow fit`, or `fit_column` in Python, on the 40 packing times. It then uses the fitted settings in the busy hour of [Choosing Inputs](choosing-inputs.md), in place of the times themselves. Fitting needs the `fit` extra: `pip install "desflow[fit]"`.

---

## Fit the Packing Times

Fit the times, which are in seconds and cannot be below 0:

=== "YAML"

    ```bash
    desflow fit packing_times.csv --column packing_seconds --lower 0 --out fitted/packing.yaml --plot fit.png
    ```

    `packing_times.csv` has a column `packing_seconds` with the 40 times. `desflow examples get fitting` downloads it as `fitting/packing_times.csv`. `--out` writes the best candidate's settings, and `--plot` draws the data against it.

=== "Builder"

    ```python title="docs/snippets/tutorials/fit_the_inputs.py:fit"
    # The 40 packing times, in seconds; none can be below 0
    result = fit_column(MEASURED, unit="s", lower=0)
    print(result.text())  # every candidate, its status and diagnostics
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/fit_the_inputs_low_level.py:fit"
    # The 40 packing times, in seconds; none can be below 0
    result = fit_column(MEASURED, unit="s", lower=0)
    print(result.text())  # every candidate, its status and diagnostics
    ```

desflow fits each family by maximum likelihood: it chooses the parameters under which the measured values are most probable. It then checks each fit and prints one row per family:

<!-- records: fit-table -->
```text
40 rows: 40 used, 0 missing, 0 invalid
family            status              AIC      KS p (approx.)  parameters or reason
exponential       rejected                                     its std is 1.5, the data's is 0.4832: more than 25% apart
normal            fitted            59.33   0.157      0.2504  mean=1.5, std=0.483218 (can draw below the lower bound 0)
lognormal         fitted            57.33 0.09406      0.8385  mean=1.50251, std=0.511854
truncated_normal  fitted            59.25  0.1558      0.2579  mean=1.49836, std=0.48579, low=0
constant          skipped                                      the values are not all equal
uniform           fitted            59.45   0.225     0.02923  low=0.7, high=2.7
triangular *      fitted            56.68  0.1258       0.511  low=0.546332, mode=1.2, high=2.83766
gamma             fitted            56.79  0.1157      0.6165  shape=9.55777, scale=0.15694
weibull           fitted            58.99  0.1502      0.2972  shape=3.33522, scale=1.67203
empirical         fitted                                       values: 40 numbers
* best: triangular
```

Lower AIC is better, and a small KS p-value is a reason to reject a family; [A Simulation Study](../des/a-simulation-study.md#aic-and-the-ks-test) explains both scores. `exponential` was rejected: its standard deviation equals its mean, 1.5 seconds, more than three times the data's 0.4832. `constant` was skipped, because the times are not all equal. `normal` fitted, but it can draw below 0, so desflow does not choose it while another family that stays within the bounds fits. `empirical`, the 40 times themselves, is always offered. The `triangular` has the lowest AIC, 56.68, and is marked best; `gamma` is 0.11 behind it.

---

## Look at the Fit

`--plot`, or `result.plot("fit.png")`, draws the data against 10,000 values drawn from the best fit through desflow's `Sampler`, as a run draws them:

![On the left, the histogram of the 40 packing times over the histogram of 10,000 values drawn from the fitted triangular between 0.55 and 2.84 seconds with its mode at 1.2. On the right, a Q-Q plot whose points lie close to the diagonal from 0.7 to 2.7 seconds](../assets/charts/fit-the-inputs.png)

On the left, the data and the generated values cover the same range. On the right, each sorted packing time is plotted against the generated value at the same quantile; points on the diagonal mean the two agree. Forty values cannot show much more than that.

---

## Load the Settings

The fitted settings go where the 40 times were:

=== "YAML"

    ```yaml title="docs/snippets/yaml/fit_the_inputs.yaml:include"
    services:
      # The best fit of the 40 packing times, as desflow fit wrote it
      packing: !include fitted/packing.yaml
    ```

    `!include` reads a mapping from another file, relative to the blueprint. The file `desflow fit` wrote:

    ```yaml title="docs/snippets/yaml/fitted/packing.yaml"
    # Fitted by desflow fit from packing_times.csv, column packing_seconds: triangular, from 40 values in s, written in seconds.
    dist: triangular
    low: 0.546332
    mode: 1.2
    high: 2.83766
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/fit_the_inputs.py:service"
    # The best fit's settings: dist="triangular", low, mode and high
    .add_service("packing", **packing)
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/fit_the_inputs_low_level.py:service"
    params = SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
        # The best fit's settings: dist="triangular", low, mode and high
        service={"packing": DistributionConfig(**packing)},
        resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
    )
    ```

`result.settings()` returns the best candidate's settings, and `result.settings("gamma")` returns the gamma's. Both keep six significant digits, as the file does, so a script and a blueprint that use the same fit draw the same values.

---

## Does the Choice Matter?

Run the busy hour 40 times with each distribution, each time with a different seed. These runs are **[replications](../des/output-analysis.md#replications)**: the mean wait differs from replication to replication, and the 95% [half-width](../des/output-analysis.md#interval-and-half-width) below says how far the mean of the 40 replications can be from the true mean wait. Each replication also leaves out its first 2 minutes, a **[warm-up](../des/output-analysis.md#one-run-starts-empty)**, because the shop starts empty and its first orders wait less than later ones. [Trusting Results](trusting-results.md) shows how to choose both numbers. In YAML, `desflow run fit_the_inputs.yaml --replications 40 --measure tasks.pack_order.wait_mean` runs the best fit. `--candidates gamma` on `desflow fit` writes the gamma fit instead. The table gives each distribution's mean packing time, and the mean wait over the 40 replications with its half-width:

| Packing time | Mean packing time (s) | Mean wait over 40 replications (s) |
|---|---|---|
| triangular, the lowest AIC | 1.53 | 2.93 ± 0.25 |
| gamma | 1.50 | 2.33 ± 0.19 |
| the 40 times | 1.50 | 2.32 ± 0.20 |

---

## Result

Which supported distribution fits the 40 packing times? The `triangular` has the lowest AIC, but the `gamma` fits almost as well, 0.11 behind. A common rule says that an AIC difference below 2 does not tell two fits apart. The choice does change the answer: the triangular gives a mean wait of **2.93 ± 0.25 seconds**, and the gamma **2.33 ± 0.19**, which agrees with the 2.32 ± 0.20 of the 40 times. The gamma fits almost as well and gives the same answer as the measured times, so it is the better choice for this question.

## Why It Works

The triangular's mean is (0.546 + 1.2 + 2.838) ÷ 3 = 1.53 seconds, 2% above the data's 1.50. The gamma keeps the data's mean exactly: shape × scale = 9.558 × 0.157 = 1.50. With the packers busy 90% of the time, 2% more packing raises utilisation to about 0.92, and the packers' spare time falls from 10% to about 8%, a fifth less. The wait depends on the spare time, as [Waits Near Full Use](../des/queueing-basics.md#waits-near-full-use) shows, so it grows far more than 2%: 2.93 seconds against 2.33. [Common Mistakes](../des/common-mistakes.md#choosing-a-normal-or-uniform-by-habit) lists the choice of a family by habit, and [Input Fitting](../user-guide/input-fitting.md) describes every option and outcome.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/fit_the_inputs.yaml"
        # The Fit the Inputs tutorial: the busy hour, with a packing time fitted to the data.
        # Fit, then run it:
        #   desflow fit packing_times.csv --column packing_seconds --lower 0 --out fitted/packing.yaml
        #   desflow run fit_the_inputs.yaml
        # 40 replications: desflow run fit_the_inputs.yaml --replications 40 --measure tasks.pack_order.wait_mean
        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42
          warmup: 2 min

        resources:
          packers: {current_cap: 3, max_cap: 10}

        arrivals:
          orders: {dist: exponential, rate: 1.8, spawn: pack_order}

        services:
          # The best fit of the 40 packing times, as desflow fit wrote it
          packing: !include fitted/packing.yaml

        tasks:
          pack_order:
            service: packing
            resource: packers
            payload: {event_type: order_packed}

        stats: typical

        run:
          until: 1 h
        ```

    === "Builder"

        ```python title="docs/snippets/tutorials/fit_the_inputs.py"
        """The Fit the Inputs tutorial: which distribution fits the 40 packing times? builder.

        Fits every family to the 40 packing times measured at the bench and prints the table
        of candidates. Then it runs the busy hour 40 times with each of three packing
        distributions: the best fit, the gamma fit and the 40 times themselves. Each run
        leaves out a 2-minute warm-up. The script prints the mean wait for each distribution.
        """

        import logging

        from desflow import SimulationContext, fit_column, replicate
        from measuring_a_run import MEASURED

        # The imported model logs at INFO; replications log only their warnings
        logging.getLogger("desflow").setLevel(logging.WARNING)


        def fitted():
            """Every family fitted to the measured times."""
            # The 40 packing times, in seconds; none can be below 0
            result = fit_column(MEASURED, unit="s", lower=0)
            print(result.text())  # every candidate, its status and diagnostics
            return result


        def shop(packing=None) -> SimulationContext:
            """The busy hour; `packing` is a fit's settings, or None for the 40 times."""
            if packing is None:
                packing = {"dist": "empirical", "values": MEASURED}
            app = (
                SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup="2 min")
                .add_resource("packers", current_cap=3, max_cap=10)
                .add_arrival("orders", dist="exponential", rate=1.8)
                # The best fit's settings: dist="triangular", low, mode and high
                .add_service("packing", **packing)
                .with_stats("typical")
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


        def mean_waits(result) -> dict:
            """The mean wait over 40 replications, with each packing distribution."""
            choices = {
                "triangular": result.settings(),
                "gamma": result.settings("gamma"),
                "empirical": None,
            }
            return {
                name: replicate(
                    lambda: shop(packing),
                    40,
                    seed=42,
                    until="1 h",
                    measures=["tasks.pack_order.wait_mean"],
                ).summary[0]
                for name, packing in choices.items()
            }


        if __name__ == "__main__":
            for name, row in mean_waits(fitted()).items():
                print(f"{name}: mean wait {row['mean']:.2f} ± {row['half_width']:.2f} s")
        ```

        It imports from `measuring_a_run.py`, which is in the full files of [Measuring a Run](measuring-a-run.md#full-files).

    === "Low-level"

        ```python title="docs/snippets/tutorials/fit_the_inputs_low_level.py"
        """The Fit the Inputs tutorial: which distribution fits the 40 packing times? low-level.

        Fits every family to the 40 packing times measured at the bench and prints the table
        of candidates. Then it runs the busy hour 40 times with each of three packing
        distributions: the best fit, the gamma fit and the 40 times themselves. Each run
        leaves out a 2-minute warm-up. The script prints the mean wait for each distribution.
        """

        import logging

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicResource,
            LowLevelModel,
            SimParameter,
            fit_column,
            replicate,
        )
        from measuring_a_run_low_level import MEASURED, order_generator

        # The imported model logs at INFO; replications log only their warnings
        logging.getLogger("desflow").setLevel(logging.WARNING)

        # Records and statistics start after the first 2 minutes of each run
        WARMUP = 120.0  # seconds


        def fitted():
            """Every family fitted to the measured times."""
            # The 40 packing times, in seconds; none can be below 0
            result = fit_column(MEASURED, unit="s", lower=0)
            print(result.text())  # every candidate, its status and diagnostics
            return result


        def busy_hour(packing):
            """The busy hour's model, with a packing distribution's settings."""
            params = SimParameter(
                sim_id="Shop",
                arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
                # The best fit's settings: dist="triangular", low, mode and high
                service={"packing": DistributionConfig(**packing)},
                resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
            )

            def model(env, sampler):
                env.registry.register_sim_parameter(params)
                packers = DynamicResource(env, "Shop", "packers")
                env.process(order_generator(env, sampler, packers))

            return LowLevelModel(model, sim_id="Shop")


        def mean_waits(result) -> dict:
            """The mean wait over 40 replications, with each packing distribution."""
            choices = {
                "triangular": result.settings(),
                "gamma": result.settings("gamma"),
                "empirical": {"dist": "empirical", "values": MEASURED},
            }
            return {
                name: replicate(
                    busy_hour(packing),
                    40,
                    seed=42,
                    until=3600,
                    warmup=WARMUP,
                    measures=["tasks.pack_order.wait_mean"],
                ).summary[0]
                for name, packing in choices.items()
            }


        if __name__ == "__main__":
            for name, row in mean_waits(fitted()).items():
                print(f"{name}: mean wait {row['mean']:.2f} ± {row['half_width']:.2f} s")
        ```

        It imports from `measuring_a_run_low_level.py`, which is in the full files of [Measuring a Run](measuring-a-run.md#full-files).

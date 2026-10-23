# Trusting Results

[Measuring a Run](measuring-a-run.md) read five numbers from one run of the busy hour. The run started with an empty shop: no orders waiting and every packer free. A real shop at that hour is never empty. The first orders of the run find free packers, so they wait less than orders in the real shop would. This page asks: **is one run enough?** The decision is how much of the start to leave out, and how many runs give an answer precise enough to act on. The measures are the mean wait and the packers' [utilisation](../des/queueing-basics.md#utilisation), and how much each varies from run to run. The boundary is the busy hour with 3 packers, run once with the seed 42 and then with other seeds. The assumptions are those of Measuring a Run: orders at random at 1.8 a second, and the 40 packing times measured at the bench.

## What You Add

This page adds a `warmup` to the run, then [replications](../des/output-analysis.md#replications): the same run repeated with other seeds. The model simulates the same busy hour as Measuring a Run.

---

## Plot the Start

Some runs have a natural start and end, such as a shop that opens empty at 9:00 and closes at 17:00. These are **[terminating](../des/output-analysis.md#terminating-or-steady-state)** runs, and the empty start is part of what they measure. Other runs stand for a system that keeps going, such as the shop in the middle of its busy hour. These are **steady-state** runs, and their empty start is a distortion called **[initialisation bias](../des/output-analysis.md#one-run-starts-empty)**. The busy hour is a steady-state run.

To see how long the distortion lasts, plot an output over time and look for where it stops trending. Publish the statistics every 10 seconds: `wait_mean` is then the mean wait of every order so far, and `queue_length` is the queue at that moment.

=== "YAML"

    ```yaml title="docs/snippets/yaml/trusting_results.yaml:plot"
    # Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
    stats: {level: typical, interval: 10}

    egress:
      - type: Console
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/trusting_results.py:plot"
    # Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
    .with_stats("typical", interval=10)
    .add_egress(ConsoleEgress())
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/trusting_results_low_level.py:plot"
    # Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
    env.setup_stats("Shop", "typical", interval=10)
    # The warm-up is set with the sinks: no sink receives a record stamped before it
    # ends, and the statistics start again from zero when it ends
    env.setup_egress([ConsoleEgress()], warmup=warmup)
    ```

Plot the two against `sim_ts` for the first 15 minutes:

![Over the first 15 minutes of the busy hour, the queue read every 10 seconds moves between 0 and 13 from the first minute on. The mean wait so far climbs from 0.46 seconds to about 1.5 seconds in the first two minutes, then moves up and down between 1.1 and 1.6 seconds. A dashed line marks the end of a 2-minute warm-up](../assets/charts/trusting-warmup.png)

The queue reaches its usual range within the first minute. The mean wait so far climbs for about a minute and a half, because the first orders found free packers. After that it moves up and down with no trend. The plot shows one seed only, and another seed could climb for longer, so the plot cannot settle the [warm-up](../des/output-analysis.md#one-run-starts-empty) on its own. It suggests three candidates:

- 1 minute, when the queue reaches its usual range;
- 2 minutes, which covers the climb of the mean wait;
- 4 minutes, twice that, to see whether a longer warm-up still moves the mean.

The next section sets 2 minutes. [Compare Warm-ups](#compare-warm-ups), after the replications, checks the three candidates with five seeds.

---

## Leave Out the Start

A `warmup` time leaves the start out of everything the run reports:

=== "YAML"

    ```yaml title="docs/snippets/yaml/trusting_results.yaml:warmup"
    # Records and statistics start after the first 2 minutes; 0 keeps everything
    warmup: ${WARMUP:-2 min}
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/trusting_results.py:warmup"
    # Records and statistics start after the first 2 minutes; None keeps everything
    SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup=warmup)
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/trusting_results_low_level.py:warmup"
    # Records and statistics start after the first 2 minutes; None keeps everything
    WARMUP = 120.0  # seconds, passed to setup_egress in run below
    ```

    In the low-level API the warm-up is an argument of `env.setup_egress`, as the `plot` step above shows, because the sinks are what leave the records out.

A run has one warm-up, in simulation time. Every sink leaves out the records and telemetry stamped before the warm-up ends. The built-in statistics start again from zero when it ends, so they describe only the rest of the run. An order that queued before the warm-up ended and started after it counts with its whole wait. The warm-up differs from `go_live_at`, which sends early records to one sink and later ones to another, and drops nothing. [Time and Pacing](../user-guide/time-and-pacing/index.md) compares the two.

Run the hour without and with the warm-up. `WARMUP=0 desflow run trusting_results.yaml` runs the YAML version without it; the Builder and Low-level scripts print both:

<!-- records: trusting-table -->
```text
without a warm-up: mean wait 1.382 s
with a 2 min warm-up: mean wait 1.381 s
```

---

## Run It With Other Seeds

The run in [Leave Out the Start](#leave-out-the-start) used one seed, 42. Another seed draws other arrival gaps and packing times, and gives another mean wait. One run cannot show how far apart those answers are. Within one run, an order's wait depends on the orders queued ahead of it, so the waits are not independent samples. **Replications** show it: the same run repeated with other seeds, each giving one independent value of every statistic.

Run the busy hour 40 times. The 40 seeds are spawned from the seed 42, so every run of the command uses the same 40 seeds. Each replication leaves out the same 2-minute warm-up:

=== "YAML"

    ```bash
    desflow run trusting_results.yaml --replications 40 --measure tasks.pack_order.wait_mean --measure resources.packers.utilization_avg
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/trusting_results.py:replicate"
    MEASURES = ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"]


    def replications(n=40):
        """Runs the busy hour n times, each after the warm-up, with seeds spawned from 42."""
        return replicate(shop, n, seed=42, until="1 h", measures=MEASURES)
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/trusting_results_low_level.py:replicate"
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
    ```

    `LowLevelModel` names the function that builds the model in an environment it is given, and the warm-up is an argument of `replicate`, because a replication has no sinks.

Each replication runs unpaced with no sinks and reads the built-in statistics at its end. [Replications](../user-guide/replications.md) describes every option.

The mean of the 40 values is the estimate. Its **[95% interval](../des/output-analysis.md#interval-and-half-width)** says how far off it may be: with 95% confidence, the true mean lies within the estimate plus or minus the **half-width**. The seeds are spawned in order, so the first 10 of the 40 replications are the 10 a run with `--replications 10` makes. The table reads the estimate after the first 5, 10 and 20 replications and after all 40:

| Replications | Mean wait (s) | Half-width (s) | Wait half-width share | Utilisation (%) | Half-width (%) | Utilisation half-width share |
|---|---|---|---|---|---|---|
| 5 | 2.21 | 0.47 | 21.2% | 89.72 | 1.37 | 1.5% |
| 10 | 2.25 | 0.25 | 11.0% | 90.34 | 0.91 | 1.0% |
| 20 | 2.48 | 0.32 | 13.0% | 90.33 | 0.63 | 0.7% |
| 40 | 2.32 | 0.20 | 8.4% | 90.03 | 0.45 | 0.5% |

The mean waits of the 40 replications range from 1.45 to 4.39 seconds. The single run with the seed 42 measured 1.38 seconds, lower than any of them. [Common Mistakes](../des/common-mistakes.md#taking-one-run-as-the-answer) explains why other pages give other figures for this wait.

---

## Choose the Precision

How many replications are enough depends on the decision, not on a fixed number such as 30. Choose the precision each output needs. Here it is a half-width within 10% of the mean. [Utilisation](../des/queueing-basics.md#utilisation) meets it with 5 replications. Of the replication counts in the table of [Run It With Other Seeds](#run-it-with-other-seeds), the mean wait first meets it at 40, because waits vary far more from run to run than how busy the packers are. Each output therefore needs its own number of replications, and the study needs enough replications for the least precise output the decision uses.

The half-width shrinks in proportion to 1 ÷ √n, so a half-width k times smaller needs about k² times as many replications. 10 replications gave 11.0%, so 10% needs about 10 × (11.0 ÷ 10)² ≈ 12. That figure is a guide, not a promise: the half-width is itself an estimate, and 20 replications gave 13.0%. Add replications, then read the half-width again.

---

## Compare Warm-ups

One seed gives one path through the hour. Run five replications after each candidate warm-up, with the same five seeds spawned from 42, and compare the mean waits. Five replications are enough here, although the mean wait needs 40 for 10% precision. The comparison does not need a precise mean: it asks only whether a longer warm-up moves the mean by more than its half-width. Each candidate runs the same five seeds, so a difference between candidates comes from the warm-up and not from the seeds:

=== "YAML"

    ```bash
    WARMUP="1 min" desflow run trusting_results.yaml --replications 5 --measure tasks.pack_order.wait_mean
    ```

    `WARMUP="2 min"` and `WARMUP="4 min"` run the other two candidates.

=== "Builder"

    ```python title="docs/snippets/tutorials/trusting_results.py:warmups"
    def warmups(n=5) -> dict:
        """The mean wait of n runs after each candidate warm-up, with seeds spawned from 42."""
        wait = ["tasks.pack_order.wait_mean"]
        return {
            warmup: replicate(shop, n, seed=42, until="1 h", warmup=warmup, measures=wait)
            for warmup in ("1 min", "2 min", "4 min")
        }
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/trusting_results_low_level.py:warmups"
    def warmups(n=5) -> dict:
        """The mean wait of n runs after each candidate warm-up, with seeds spawned from 42."""
        shop = LowLevelModel(model, sim_id="Shop")
        wait = ["tasks.pack_order.wait_mean"]
        return {
            warmup: replicate(shop, n, seed=42, until=3600, warmup=warmup, measures=wait)
            for warmup in (60, WARMUP, 240)
        }
    ```

    `model` is the function from [Run It With Other Seeds](#run-it-with-other-seeds) above.

The mean wait over the five replications, in seconds, with its 95% half-width:

| Warm-up | Mean wait (s) |
|---|---|
| 1 minute | 2.196 ± 0.47 |
| 2 minutes | 2.207 ± 0.47 |
| 4 minutes | 2.232 ± 0.47 |

Keep the shortest warm-up after which a longer one no longer moves the mean by more than its half-width, as [Output Analysis](../des/output-analysis.md#one-run-starts-empty) sets out. After 1 minute, the longer warm-ups move the mean by 0.011 and 0.036 seconds, far less than the half-width of 0.47 seconds, so the comparison gives **1 minute**. The plot of the seed 42 still climbs until about a minute and a half, and Output Analysis advises choosing the warm-up conservatively. So the page keeps **2 minutes**: it passes the comparison, and it is the shortest candidate that also covers the climb in the plot.

---

## Result

Is one run enough? No. Its empty start is not the problem. Over five seeds, the comparison of three candidates finds a warm-up of **1 minute** enough, and the page keeps **2 minutes** to cover the climb in the plot. In the run with the seed 42, a 2-minute warm-up changes the mean wait only from 1.382 to 1.381 seconds. The shop fills within a minute or two, and those minutes are a small part of one hour. A shorter run, or a start further from the usual state, such as a shop that opens with a backlog of orders, needs the warm-up much more. The problem is that one run's answer depends on its seed. The run with the seed 42 measured 1.38 seconds, lower than every one of 40 replications. Over 40 replications the mean wait is **2.32 ± 0.20 seconds** with 95% confidence, within the chosen 10% precision, and the packers are busy **90.03 ± 0.45%** of the time.

## Why It Works

Initialisation bias ([Output Analysis](../des/output-analysis.md#one-run-starts-empty) explains it) fades once the state of the shop no longer depends on how the run started. Here the queue, empty for the first orders, reaches its usual range within a minute. The warmed-up numbers still pass the [Little's law](../des/queueing-basics.md#littles-law) check from Measuring a Run. In the 3,480 seconds after the warm-up, 6,024 orders were packed, 1.73 a second. 1.73 orders a second × 1.381 seconds of mean wait = 2.39 orders waiting on average, which matches the `queue_length_avg` of 2.40 that the same statistics report. [Statistics](../user-guide/statistics.md) describes how the reset works.

Replications ([Output Analysis](../des/output-analysis.md#replications) explains them) are independent of each other because each has its own seed, so the spread of their values measures how much the answer depends on chance. With few replications the spread itself is uncertain. The [Student t quantile](../des/output-analysis.md#interval-and-half-width) allows for it: the half-width is the standard error times 2.776 for 5 replications, and times 2.023 for 40.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/trusting_results.yaml"
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

    === "Builder"

        ```python title="docs/snippets/tutorials/trusting_results.py"
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

    === "Low-level"

        ```python title="docs/snippets/tutorials/trusting_results_low_level.py"
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

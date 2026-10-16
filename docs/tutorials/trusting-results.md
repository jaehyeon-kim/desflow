# Trusting Results

[Measuring a Run](measuring-a-run.md) read five numbers from one run of the busy hour. The run started with an empty shop: no orders waiting and every packer free. A real shop at that hour is never empty, so the first orders of the run wait less than they would. This page asks: **is one run enough?** It answers the first part of that question: how much of the start to leave out.

## What You Add

A `warmup` on the run. The shop is the busy hour from Measuring a Run, unchanged.

---

## Plot the Start

Some runs have a natural start and end, such as a shop that opens empty at 9:00 and closes at 17:00; these are **terminating** runs, and the empty start is part of what they measure. Others stand for a system that keeps going, such as the shop in the middle of its busy hour; these are **steady-state** runs, and the empty start is a distortion called **initialisation bias**. The busy hour is a steady-state run.

To see how long the distortion lasts, plot an output over time and look for where it stops trending. Publish the statistics every 10 seconds: `wait_mean` is then the mean wait of every order so far, and `queue_length` is the queue at that moment.

=== "YAML"

    ```yaml title="docs/snippets/yaml/trusting_results.yaml:plot"
    # wait_mean every 10 seconds is the mean wait so far: plot it against sim_ts
    stats: {level: typical, interval: 10}

    egress:
      - type: Console
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/trusting_results.py:plot"
    # wait_mean every 10 seconds is the mean wait so far: plot it against sim_ts
    .with_stats("typical", interval=10)
    .add_egress(ConsoleEgress())
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/trusting_results_low_level.py:plot"
    # wait_mean every 10 seconds is the mean wait so far: plot it against sim_ts
    env.setup_stats("Shop", "typical", interval=10)
    # The warm-up is set with the sinks: none receives a record before it, and the
    # statistics start again when it ends
    env.setup_egress([ConsoleEgress()], warmup=warmup)
    ```

Plot the two against `sim_ts` for the first 15 minutes:

![Over the first 15 minutes of the busy hour, the queue read every 10 seconds moves between 0 and 13 from the first minute on; the mean wait so far climbs from 0.46 seconds to about 1.5 seconds in the first two minutes, then wanders between 1.1 and 1.6 seconds; a dashed line marks the end of a 2-minute warm-up](../assets/charts/trusting-warmup.png)

The queue reaches its usual range within the first minute. The mean wait so far climbs for about a minute and a half, because the first orders found free packers, then moves up and down with no trend. **2 minutes** covers the climb with a margin, so that is the warm-up.

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

    In the low-level API the warm-up is an argument of `env.setup_egress`, as the `plot` step above shows, because it is the sinks that leave the records out.

A run has one warm-up, in simulation time. Every sink leaves out the records and telemetry stamped before it, and the built-in statistics start again from zero at that moment, so they describe the rest of the run only. An order that queued before the warm-up ended and started after it counts with its whole wait. The warm-up is not `go_live_at`, which sends early records to one sink and later ones to another and drops nothing; [Time and Pacing](../user-guide/time-and-pacing.md) compares the two.

Run the hour without and with the warm-up. `WARMUP=0 desflow run trusting_results.yaml` runs the YAML version without it; the Builder and Low-level scripts print both:

<!-- records: trusting-table -->
```text
without a warm-up: mean wait 1.382 s
with a 2 min warm-up: mean wait 1.381 s
```

---

## Result

Is one run enough? Its start does not distort it: read from the plot, the busy hour needs a **2-minute warm-up**. With it the mean wait is **1.381 seconds**, against 1.382 seconds without it. For this shop and this run, the empty start changes the answer by less than 0.1%: the shop fills within a minute or two, and one hour is long enough that those minutes hardly count. A shorter run, or a start further from the usual state, such as a shop that opens with a backlog of orders, needs the warm-up much more.

So one run of the busy hour is long enough not to be distorted by its start. Whether one run is enough at all, when another seed gives another answer, is the second half of the question, which the next release answers with replications.

## Why It Works

Initialisation bias fades as the model forgets how it started: here the queue that the first orders found empty is full again within a minute. The warmed-up numbers still pass the check from Measuring a Run. 6,024 orders were packed in the 3,480 seconds after the warm-up, 1.73 a second, and 1.73 × 1.381 seconds = 2.39, which is the `queue_length_avg` of 2.40 that the same statistics report. [Statistics](../user-guide/statistics.md) describes how the reset works.

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

        # wait_mean every 10 seconds is the mean wait so far: plot it against sim_ts
        stats: {level: typical, interval: 10}

        egress:
          - type: Console

        run:
          until: 1 h
        ```

    === "Builder"

        ```python title="docs/snippets/tutorials/trusting_results.py"
        """The Trusting Results tutorial: the busy hour, measured after a warm-up, builder.

        Three packers, orders at 1.8 a second and the measured packing times. Every record is
        printed to the terminal, and the script prints the mean wait without and with the
        warm-up at the end: python trusting_results.py 2>/dev/null shows only those lines.
        """

        import logging

        from desflow import ConsoleEgress, SimulationContext

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
                # wait_mean every 10 seconds is the mean wait so far: plot it against sim_ts
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


        if __name__ == "__main__":
            print(f"without a warm-up: mean wait {mean_wait(None):.3f} s")
            print(f"with a 2 min warm-up: mean wait {mean_wait('2 min'):.3f} s")
        ```

    === "Low-level"

        ```python title="docs/snippets/tutorials/trusting_results_low_level.py"
        """The Trusting Results tutorial: the busy hour, measured after a warm-up, low-level.

        Three packers, orders at 1.8 a second and the measured packing times. Every record is
        printed to the terminal, and the script prints the mean wait without and with the
        warm-up at the end: python trusting_results_low_level.py 2>/dev/null shows only
        those lines.
        """

        import logging

        from desflow import (
            CapacityConfig,
            ConsoleEgress,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            Sampler,
            SimParameter,
        )

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
            # wait_mean every 10 seconds is the mean wait so far: plot it against sim_ts
            env.setup_stats("Shop", "typical", interval=10)
            # The warm-up is set with the sinks: none receives a record before it, and the
            # statistics start again when it ends
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


        if __name__ == "__main__":
            print(f"without a warm-up: mean wait {mean_wait(None):.3f} s")
            print(f"with a 2 min warm-up: mean wait {mean_wait(WARMUP):.3f} s")
        ```

Next, [Stock](stock.md) asks what happens when the shop runs out of boxes.

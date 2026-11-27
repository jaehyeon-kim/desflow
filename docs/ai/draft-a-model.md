# Draft a Model

Describe a process in your own words, and an assistant with the skill turns it into a blueprint. It marks every number it had to assume, checks the load against queueing theory, and runs a study before it gives you an answer.

The GIF follows one request, the packing shop of the [Learn tutorial](../learn/first-model/first-simulation.md), through the steps the skill teaches:

![The shop in four steps. You ask how long an order waits with one packer. The assistant drafts a blueprint and marks each number it assumed. It checks the load with desflow explain: the packer is busy 75% of the time and an order waits 2.25 seconds on average. It runs 20 replications, which give a mean wait of 2.18 seconds, with an interval that holds 2.25](../assets/diagrams/agent-skill.gif)

The sections below hold each step's text, so you can copy the request and check each answer.

---

## Ask

Paste the request into an assistant that has the skill:

```text
A shop packs customer orders.
An order arrives about every 2 seconds.
One packer packs each order in about 1.5 seconds.
How long does an order wait?
```

---

## Draft

The skill tells the assistant to start from the nearest example (`desflow examples list`), and to build the model in stages, running the file after each one. It asks you for the numbers the description does not give. When it has to assume one, it marks it with a comment and lists the assumed numbers first in its reply, because you must confirm them.

The description does not say whether orders arrive at random or how much the packing time varies. The draft assumes both:

=== "YAML"

    ```yaml title="docs/snippets/ai/drafted_shop.yaml:draft"
    simulation:
      sim_id: Shop
      factor: 0  # no pacing: the run ends as soon as it is done
      random_seed: 42
      # a fixed date, so two runs give the same records
      logical_start_time: '2026-01-05T09:00:00Z'

    resources:
      packers: {current_cap: 1, max_cap: 1}  # said: one packer

    services:
      # said: 1.5 seconds; assumed: every order takes the same time
      packing: {dist: constant, value: 1.5}

    arrivals:
      # said: an order every 2 seconds on average; assumed: orders arrive at random
      orders: {dist: exponential, rate: 0.5, spawn: pack_order}

    tasks:
      pack_order:
        service: packing
        resource: packers
        payload: {event_type: order_packed}
    ```

=== "Builder"

    ```python title="docs/snippets/ai/drafted_shop_builder.py:draft"
    app = (
        SimulationContext(
            sim_id="Shop",
            factor=0,  # no pacing: the run ends as soon as it is done
            random_seed=42,
            # a fixed date, so two runs give the same records
            logical_start_time=datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc),
        )
        .add_resource("packers", current_cap=1, max_cap=1)  # said: one packer
        # said: 1.5 seconds; assumed: every order takes the same time
        .add_service("packing", dist="constant", value=1.5)
        # said: an order every 2 seconds on average; assumed: orders arrive at random
        .add_arrival("orders", dist="exponential", rate=0.5)
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
    ```

=== "Low-level"

    ```python title="docs/snippets/ai/drafted_shop_low_level.py:draft"
    PARAMS = SimParameter(
        sim_id="Shop",
        # said: an order every 2 seconds on average; assumed: orders arrive at random
        arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
        # said: 1.5 seconds; assumed: every order takes the same time
        service={"packing": DistributionConfig(dist="constant", value=1.5)},
        resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},  # said: one packer
    )
    SEED = 42
    # a fixed date, so two runs give the same records
    START = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)


    def pack_order(env, sampler, packers, key, task_id):
        """One order: wait for a packer, pack it, release the packer."""
        path_id = "Shop.service.packing"

        def publish(value):
            env.publish_event(key, value, task="pack_order", instance=task_id)

        publish(
            {"path_id": path_id, "sim_id": "Shop", "service": "packing", "status": "queued"}
        )
        env.stats.task_queued("pack_order", task_id)
        with packers.request() as request:
            yield request
            publish(
                {
                    "path_id": path_id,
                    "sim_id": "Shop",
                    "service": "packing",
                    "status": "started",
                }
            )
            env.stats.task_started("pack_order", task_id)
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            publish({"event_type": "order_packed"})
        env.stats.task_finished("pack_order", task_id)


    def order_generator(env, sampler, packers):
        """Starts one pack_order for every arrival, keyed orders-1, orders-2, ..."""
        arrival = env.registry.get_config("Shop.arrival.orders")
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(arrival))
            env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
            task_id += 1
    ```

---

## Check the Load

Before it runs anything, the assistant checks that the packer can keep up. `desflow explain` gives the queueing formulas' answer:

```bash
desflow explain drafted_shop.yaml
```

```text
Shop
`-- orders (arrival, rate 0.5/s)
    `-- spawn -> pack_order (task, rate 0.5/s)
        +-- uses packers (resource)
        `-- next (share 1) -> exit
Resources
+------------------------------------------------------------+
| resource |   λ |      μ | c |    ρ |   Wq |     L | status |
|----------+-----+--------+---+------+------+-------+--------|
| packers  | 0.5 | 0.6667 | 1 | 0.75 | 2.25 | 1.875 | OK     |
+------------------------------------------------------------+
Bottleneck: packers
Hints for utilisation 0.85:
  packers with 1 unit: utilisation 0.75, at most 0.85
  packers with 1 unit: utilisation 0.85 at a request every 1.76 seconds (34 a minute, 0.5667 a second)
```

The packer is busy 75% of the time (`ρ`), so the queue does not grow without limit, and the formulas give a mean wait of 2.25 seconds (`Wq`). When a model routes on a variable or has scenario steps, `desflow explain` gives no estimate, and the skill tells the assistant to check the load by hand: the work of one case, times the arrival rate, must fit inside the capacity. [Command Line](../cli/index.md#check-it-before-a-long-run) shows more of `desflow explain`, and [Run a Study](run-a-study.md) compares designs.

---

## Answer From Replications

One run is one sample, so the skill tells the assistant to answer from replications. 20 runs of an hour, each with its own seed spawned from 42:

```bash
desflow run drafted_shop.yaml --replications 20 | grep -E "^measure|wait_mean"
```

```text
measure                                n          mean    half-width (95%)
tasks.pack_order.wait_mean            20         2.175              0.1305
```

The mean wait is 2.18 ± 0.13 seconds. The interval, 2.05 to 2.31, holds the 2.25 of the formulas, so the model and the theory agree. The run starts with an empty shop and has no warm-up, which [Trusting Results](../learn/studies/trusting-results.md) explains how to choose.

---

## Check the Reply

The skill tells the assistant to copy a checklist into its reply and tick each line: every behaviour the request names, the key that makes it, the record that shows it, and a run of the skill's own check on the files. Read the assumed numbers first. Then check that the answer is an interval from replications, not the result of one run.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/ai/drafted_shop.yaml"
        # Drafted from a description of the packing shop. Each value says where it came from:
        # "said" means the description gives it, and "assumed" means the user has not confirmed
        # it.
        # Run it with: desflow run drafted_shop.yaml
        simulation:
          sim_id: Shop
          factor: 0  # no pacing: the run ends as soon as it is done
          random_seed: 42
          # a fixed date, so two runs give the same records
          logical_start_time: '2026-01-05T09:00:00Z'

        resources:
          packers: {current_cap: 1, max_cap: 1}  # said: one packer

        services:
          # said: 1.5 seconds; assumed: every order takes the same time
          packing: {dist: constant, value: 1.5}

        arrivals:
          # said: an order every 2 seconds on average; assumed: orders arrive at random
          orders: {dist: exponential, rate: 0.5, spawn: pack_order}

        tasks:
          pack_order:
            service: packing
            resource: packers
            payload: {event_type: order_packed}

        egress:
          - {type: Console}

        run: {until: 1 hour}  # assumed: one hour answers the question

        tests:
          # From the request: an order every 2 seconds is 18,000 orders in 10 hours. The
          # count varies by about 134 between seeds (the square root of 18,000), so 540 is
          # four standard deviations, and an arrival rate 10% higher, 19,800, fails
          - name: orders_packed_in_10_hours
            until: 10 hours  # long enough that a 10% change is more than the spread
            expect:
              tasks.pack_order.finished: {value: 18000, within: 540}
          # From the request: 1.5 seconds of packing for every 2 seconds is 75% of the
          # packer's time. Over 10 hours it varies by about 0.5 between seeds, so 2 is four
          # standard deviations, and the same 10% higher rate, 82.5%, fails
          - name: packer_busy_three_quarters
            until: 10 hours
            expect:
              resources.packers.utilization_avg: {value: 75, within: 2}
        ```

    === "Builder"

        ```python title="docs/snippets/ai/drafted_shop_builder.py"
        """The drafted packing shop with the SimulationContext builder.

        The Builder form of drafted_shop.yaml: it gives the same records at the same seed.
        Run it with: python drafted_shop_builder.py
        """

        from datetime import datetime, timezone

        from desflow import ConsoleEgress, SimulationContext

        app = (
            SimulationContext(
                sim_id="Shop",
                factor=0,  # no pacing: the run ends as soon as it is done
                random_seed=42,
                # a fixed date, so two runs give the same records
                logical_start_time=datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc),
            )
            .add_resource("packers", current_cap=1, max_cap=1)  # said: one packer
            # said: 1.5 seconds; assumed: every order takes the same time
            .add_service("packing", dist="constant", value=1.5)
            # said: an order every 2 seconds on average; assumed: orders arrive at random
            .add_arrival("orders", dist="exponential", rate=0.5)
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


        app.add_egress(ConsoleEgress(format="json"))

        if __name__ == "__main__":
            app.run(until="1 hour")  # assumed: one hour answers the question
        ```

    === "Low-level"

        ```python title="docs/snippets/ai/drafted_shop_low_level.py"
        """The drafted packing shop on the low-level API.

        The Low-level form of drafted_shop.yaml: it gives the same records at the same seed.
        Times are in seconds. Run it with: python drafted_shop_low_level.py
        """

        from datetime import datetime, timezone

        from desflow import (
            CapacityConfig,
            ConsoleEgress,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            Sampler,
            SimParameter,
        )

        PARAMS = SimParameter(
            sim_id="Shop",
            # said: an order every 2 seconds on average; assumed: orders arrive at random
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
            # said: 1.5 seconds; assumed: every order takes the same time
            service={"packing": DistributionConfig(dist="constant", value=1.5)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},  # said: one packer
        )
        SEED = 42
        # a fixed date, so two runs give the same records
        START = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)


        def pack_order(env, sampler, packers, key, task_id):
            """One order: wait for a packer, pack it, release the packer."""
            path_id = "Shop.service.packing"

            def publish(value):
                env.publish_event(key, value, task="pack_order", instance=task_id)

            publish(
                {"path_id": path_id, "sim_id": "Shop", "service": "packing", "status": "queued"}
            )
            env.stats.task_queued("pack_order", task_id)
            with packers.request() as request:
                yield request
                publish(
                    {
                        "path_id": path_id,
                        "sim_id": "Shop",
                        "service": "packing",
                        "status": "started",
                    }
                )
                env.stats.task_started("pack_order", task_id)
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                publish({"event_type": "order_packed"})
            env.stats.task_finished("pack_order", task_id)


        def order_generator(env, sampler, packers):
            """Starts one pack_order for every arrival, keyed orders-1, orders-2, ..."""
            arrival = env.registry.get_config("Shop.arrival.orders")
            task_id = 0
            while True:
                yield env.timeout(sampler.sample(arrival))
                env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
                task_id += 1


        def run(until=3600):  # assumed: one hour answers the question
            env = DynamicRealtimeEnvironment(factor=0, logical_start_time=START)
            env.registry.register_sim_parameter(PARAMS)
            env.setup_egress([ConsoleEgress(format="json")])
            packers = DynamicResource(env, "Shop", "packers")
            sampler = Sampler(seed=SEED)
            env.process(order_generator(env, sampler, packers))
            try:
                env.run(until=until)
            finally:
                env.teardown()
            return env


        if __name__ == "__main__":
            run()
        ```

# Timestamps and Gaps

Put times into records and make the gaps between records vary: an order about every 0.4 seconds, with the time it was placed, a delivery date up to 2 days later, and the same time in milliseconds since 1970.

---

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

    ```bash
    git clone https://github.com/jaehyeon-kim/desflow.git
    cd desflow
    ```

- [First Simulation](../../learn/first-model/first-simulation.md) explains arrivals, tasks and the records a run writes.
- [Arrivals and Services](../../user-guide/model-parts/arrivals-and-services.md) lists the distributions an arrival can use, and [`now`](../../generators/time/now.md) and [`format_time`](../../generators/time/format-time.md) list the keys of the two time generators.

---

## Steps

### Draw the Gaps

An arrival takes its gaps from `dist` or from `gap`. A `gap` is a number of seconds, a duration, an expression or a generator, and it is read at the start of each gap. A `normal` generator with a mean of 0.4 and a standard deviation of 0.03 gives an order about every 0.4 seconds, and each gap differs by a few hundredths of a second.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/timestamps_and_gaps.yaml:arrivals" hl_lines="3"
    arrivals:
      orders:
        gap: {gen: normal, mean: 0.4, std: 0.03}
        spawn: order
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/timestamps_and_gaps_builder.py:arrival"
    app.add_arrival(
        "orders",
        gap=generator({"gen": "normal", "mean": 0.4, "std": 0.03}),
        spawn="order",
    )
    ```

=== "Low-level"

    ```python title="docs/snippets/generate-data/timestamps_and_gaps_low_level.py:arrival"
    sampler = Sampler(seed=42)
    gap = sampler.generator("arrivals.orders.gap")
    offset = sampler.generator("tasks.order.payload.deliver_by.offset")
    ```

### Write the Times

[`now`](../../generators/time/now.md) gives the time of the record: the `logical_start_time` of the run plus the simulation time. `offset` adds seconds, and here a draw between 0 and 172,800 seconds (2 days) makes the delivery date. [`format_time`](../../generators/time/format-time.md) with `format: epoch_ms` writes the same time as a number of milliseconds since 1970. An offset below 0 gives an earlier time.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/timestamps_and_gaps.yaml:tasks" hl_lines="4 7 8"
    tasks:
      order:
        payload:
          placed_at: {gen: now}
          deliver_by:
            gen: now
            offset: {gen: uniform, low: 0, high: 172800}
          epoch_ms: {gen: format_time, format: epoch_ms}
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/timestamps_and_gaps_builder.py:payload"
    order_payload = Template(
        {
            "placed_at": {"gen": "now"},
            "deliver_by": {
                "gen": "now",
                "offset": {"gen": "uniform", "low": 0, "high": 172800},
            },
            "epoch_ms": {"gen": "format_time", "format": "epoch_ms"},
        },
        name="tasks.order",
    )

    @app.task(name="order")
    def order(task_id):
        """Gives one order record."""
        return order_payload
    ```

=== "Low-level"

    ```python title="docs/snippets/generate-data/timestamps_and_gaps_low_level.py:payload"
    def order_time(seconds=0.0):
        """The logical time now plus `seconds`, as the environment counts it."""
        return env.start_datetime + timedelta(seconds=env.now + seconds)

    def orders_source():
        for number in itertools.count(1):
            yield env.timeout(float(gap.normal(0.4, 0.03)))
            now = order_time()
            deliver_by = order_time(float(offset.uniform(0, 172800)))
            payload = {
                "placed_at": now.isoformat(timespec="milliseconds"),
                "deliver_by": deliver_by.isoformat(timespec="milliseconds"),
                "epoch_ms": int((now - EPOCH) // timedelta(milliseconds=1)),
            }
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=number - 1
            )

    env.process(orders_source())
    ```

    The Low-level file computes each time itself, from `env.start_datetime` and `env.now`.

---

## Check the Result

Run the model for 10 simulated seconds and print its first 3 records. `placed_at` is the logical start time plus the record's `sim_ts`, `deliver_by` is up to 2 days after it, and `epoch_ms` counts from 1 January 1970.

=== "YAML"

    ```bash
    desflow run docs/snippets/generate-data/timestamps_and_gaps.yaml --sample 3
    ```

=== "Builder"

    ```bash
    python docs/snippets/generate-data/timestamps_and_gaps_builder.py
    ```

    This prints every record of the run as an `[EVT]` line with the same fields and values.

=== "Low-level"

    ```bash
    python docs/snippets/generate-data/timestamps_and_gaps_low_level.py
    ```

    This prints every record of the run as an `[EVT]` line with the same fields and values.

<!-- records: gd-timestamps-and-gaps -->
```text
{"stream_type":"event","sim_ts":0.42657339329429356,"timestamp":"...","key":"orders-1","task":"order","instance":0,"value":{"placed_at":"2026-01-01T00:00:00.426+00:00","deliver_by":"2026-01-01T18:31:03.590+00:00","epoch_ms":1767225600426}}
{"stream_type":"event","sim_ts":0.8902423871403649,"timestamp":"...","key":"orders-2","task":"order","instance":1,"value":{"placed_at":"2026-01-01T00:00:00.890+00:00","deliver_by":"2026-01-02T07:57:50.425+00:00","epoch_ms":1767225600890}}
{"stream_type":"event","sim_ts":1.2975278008631643,"timestamp":"...","key":"orders-3","task":"order","instance":2,"value":{"placed_at":"2026-01-01T00:00:01.297+00:00","deliver_by":"2026-01-01T12:04:22.699+00:00","epoch_ms":1767225601297}}
```

The times repeat from run to run, because `now` counts from the fixed `logical_start_time` and not from the wall clock. The docs test runs the model for 2,000 simulated seconds: the gaps average 0.4 seconds with a standard deviation of 0.03, and `deliver_by` reaches both ends of 0 to 2 days after `placed_at`.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/generate-data/timestamps_and_gaps.yaml"
        # Timestamps and gaps: generated times and random gaps between records.
        #
        # An order event arrives on average every 0.4 s, with a gap drawn from a normal
        # distribution with a standard deviation of 30 ms, so the gaps vary around 0.4 s. Each
        # order holds the time it was placed, a delivery date up to 2 days later, and the same
        # time as a number of milliseconds since 1970. An offset below 0 gives an earlier time.
        #
        # Needs: nothing.
        # Run it with: desflow run docs/snippets/generate-data/timestamps_and_gaps.yaml

        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42
          logical_start_time: 2026-01-01T00:00:00

        egress:
          - {type: Console}

        arrivals:
          orders:
            gap: {gen: normal, mean: 0.4, std: 0.03}
            spawn: order

        tasks:
          order:
            payload:
              placed_at: {gen: now}
              deliver_by:
                gen: now
                offset: {gen: uniform, low: 0, high: 172800}
              epoch_ms: {gen: format_time, format: epoch_ms}

        run: {until: 10}
        ```

    === "Builder"

        ```python title="docs/snippets/generate-data/timestamps_and_gaps_builder.py"
        """Timestamps and gaps: generated times and random gaps between records, builder API.

        The builder version of `timestamps_and_gaps.yaml`: an order on average every 0.4 s with
        a random gap, each with generated times.

        Needs: nothing.
        Run it with: python docs/snippets/generate-data/timestamps_and_gaps_builder.py
        """

        import logging
        from datetime import datetime, timezone

        from desflow import ConsoleEgress, SimulationContext
        from desflow.values import Template, generator

        UNTIL = 10


        def build() -> SimulationContext:
            """Builds the model."""
            app = SimulationContext(
                sim_id="Shop",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            app.add_egress(ConsoleEgress())
            app.add_arrival(
                "orders",
                gap=generator({"gen": "normal", "mean": 0.4, "std": 0.03}),
                spawn="order",
            )
            order_payload = Template(
                {
                    "placed_at": {"gen": "now"},
                    "deliver_by": {
                        "gen": "now",
                        "offset": {"gen": "uniform", "low": 0, "high": 172800},
                    },
                    "epoch_ms": {"gen": "format_time", "format": "epoch_ms"},
                },
                name="tasks.order",
            )

            @app.task(name="order")
            def order(task_id):
                """Gives one order record."""
                return order_payload


            return app


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            build().run(until=UNTIL)
        ```

    === "Low-level"

        ```python title="docs/snippets/generate-data/timestamps_and_gaps_low_level.py"
        """Timestamps and gaps: generated times and random gaps between records, low-level API.

        The low-level version of `timestamps_and_gaps.yaml`: plain SimPy on a
        `DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. The gap
        and the delivery date are drawn with `Sampler`, from the random sequence the blueprint's
        generator at that place uses, so the draws are the blueprint's. The times come from the
        environment: the start time plus the simulation time, written as the blueprint's `now`
        and `format_time` write them.

        Needs: nothing.
        Run it with: python docs/snippets/generate-data/timestamps_and_gaps_low_level.py
        """

        import itertools
        import logging
        from datetime import datetime, timedelta, timezone

        from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

        UNTIL = 10
        START = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}
        EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


        def build(env: DynamicRealtimeEnvironment) -> None:
            """Starts the arrival on `env`."""
            sampler = Sampler(seed=42)
            gap = sampler.generator("arrivals.orders.gap")
            offset = sampler.generator("tasks.order.payload.deliver_by.offset")

            def order_time(seconds=0.0):
                """The logical time now plus `seconds`, as the environment counts it."""
                return env.start_datetime + timedelta(seconds=env.now + seconds)

            def orders_source():
                for number in itertools.count(1):
                    yield env.timeout(float(gap.normal(0.4, 0.03)))
                    now = order_time()
                    deliver_by = order_time(float(offset.uniform(0, 172800)))
                    payload = {
                        "placed_at": now.isoformat(timespec="milliseconds"),
                        "deliver_by": deliver_by.isoformat(timespec="milliseconds"),
                        "epoch_ms": int((now - EPOCH) // timedelta(milliseconds=1)),
                    }
                    env.publish_event(
                        f"orders-{number}", payload, task="order", instance=number - 1
                    )

            env.process(orders_source())


        def run() -> None:
            """Runs the model unpaced into the console."""
            env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
            env.setup_egress([ConsoleEgress()])
            build(env)
            try:
                env.run(until=UNTIL)
            finally:
                env.teardown()


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            run()
        ```

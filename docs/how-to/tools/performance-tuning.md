# Tune Performance

Make a run that generates data go as fast as the machine allows, and measure how many records a second it publishes.

---

## Before You Start

- [Time and Pacing](../../user-guide/time-and-pacing/index.md) explains `factor`.
- [Egress](../../user-guide/live-runs-and-connectors/egress.md) explains batches.

---

## Steps

1. Run at `factor: 0`, without `go_live_at`. The run then waits for no wall clock, each event is processed by SimPy's own step, and no lag telemetry is published. A pending go-live keeps the real-time step until it is reached.
2. Ask only for the statistics you read: `minimal` reads a few values at each interval, while `typical` and `all` add work at every task step, as [Statistics](../../user-guide/results/statistics.md#stability-and-limits) explains.
3. Use the console sink only to look at a run. It writes one line per record. A file or a stream sink costs less per record, and a counting sink almost nothing.
4. Give a lake sink a large `batch_size`, such as 10,000, because each batch becomes one file. A stream sink can keep a small one.
5. After building a large model, `gc.freeze()` leaves the objects built so far out of garbage collection. In the measurements below it saved about 9 percent.

This shop takes ten orders a second with 25 packers, and has no sink of its own:

=== "YAML"

    ```yaml title="docs/snippets/yaml/performance_tuning.yaml:shop"
    simulation:
      sim_id: Shop
      factor: 0  # no wall clock to wait for
      random_seed: 42

    resources:
      packers: {current_cap: 25, max_cap: 25}

    services:
      packing: {dist: normal, mean: 2 s, std: 0.4 s}

    arrivals:
      orders: {dist: exponential, rate: 10, spawn: pack_order}

    tasks:
      pack_order:
        service: packing
        resource: packers
        payload: {event_type: order_packed}

    # minimal: readings every 10 s, not the costlier task statistics
    stats: {level: minimal, interval: 10}

    run:
      until: 1 h
    ```

=== "Builder"

    ```python title="docs/snippets/guides/performance_tuning.py:shop"
    def shop() -> SimulationContext:
        """Ten orders a second and 25 packers, with no sink of its own."""
        app = (
            # factor 0: no wall clock to wait for
            SimulationContext(sim_id="Shop", factor=0, random_seed=42)
            .add_resource("packers", current_cap=25, max_cap=25)
            .add_service("packing", dist="normal", mean=2.0, std=0.4)
            .add_arrival("orders", dist="exponential", rate=10.0)
            # minimal: readings every 10 s, not the costlier task statistics
            .with_stats("minimal", interval=10)
        )

        @app.task(service_id="packing", resource_id="packers")
        def pack_order(task_id: int):
            return {"event_type": "order_packed"}

        @app.arrival_loop("orders")
        def order_loop(context: SimulationContext):
            while True:
                order = yield context.wait_for_arrival("orders")
                context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

        return app
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/performance_tuning_low_level.py:shop"
    def pack_order(env, packers, sampler, key, task_id):
        path_id, stats = "Shop.service.packing", env.stats
        env.publish_event(
            key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
        )
        stats.task_queued("pack_order", task_id)
        with packers.request() as unit:
            yield unit
            env.publish_event(
                key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
            )
            stats.task_started("pack_order", task_id)
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)
            stats.task_finished("pack_order", task_id)


    def orders(env, packers, sampler):
        gap = env.registry.get_config("Shop.arrival.orders")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


    def shop(egress: list) -> DynamicRealtimeEnvironment:
        """Ten orders a second and 25 packers, publishing to the sinks given."""
        env = DynamicRealtimeEnvironment(factor=0)  # no wall clock to wait for
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress(egress)
        # minimal: readings every 10 s, not the costlier task statistics
        env.setup_stats("Shop", "minimal", interval=10)
        packers = DynamicResource(env, "Shop", "packers")
        env.process(orders(env, packers, Sampler(seed=42)))
        return env
    ```

At `factor: 0` no `system.simulation.lag_seconds` is published, so the count holds only the shop's own records. With a lag reading every second it would be 3,600 more.

---

## Check the Result

This function adds a counting sink to a model, runs it, and returns the number of records it published and the records published per second, counted from the start of `run()` to the end of its teardown. A blueprint loads as a context, so the YAML form uses the builder's function. The low-level form adds the sink to the environment and calls the teardown itself:

=== "YAML"

    ```python title="docs/snippets/guides/performance_tuning.py:blueprint"
    app = SimulationContext.from_yaml(path)  # a context, as the builder makes one
    return throughput(app, until=3600)
    ```

=== "Builder"

    ```python title="docs/snippets/guides/performance_tuning.py:measure"
    def throughput(app: SimulationContext, until: float) -> dict:
        """Runs a model with a counting sink: the record count and the records per second."""
        count = Count()
        app.add_egress(count)
        gc.collect()
        gc.freeze()  # leave the objects built so far out of garbage collection
        started = time.perf_counter()
        app.run(until=until)
        seconds = time.perf_counter() - started
        gc.unfreeze()
        return {"records": count.records, "per_second": round(count.records / seconds)}
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/performance_tuning_low_level.py:measure"
    def throughput(until: float) -> dict:
        """Runs the shop with a counting sink: the record count and the records per second."""
        count = Count()
        env = shop([count])
        gc.collect()
        gc.freeze()  # leave the objects built so far out of garbage collection
        started = time.perf_counter()
        env.run(until=until)
        env.teardown()  # waits until the sink has taken every batch
        seconds = time.perf_counter() - started
        gc.unfreeze()
        return {"records": count.records, "per_second": round(count.records / seconds)}
    ```

For one simulated hour, each form returns **108,688** records, at about 100,000 records a second of wall-clock time on the machine below.

### Measurements

`benchmarks/throughput.py` in the repository runs one low-level workload, 5,000 simulation seconds of 10 orders a second, at `factor: 0` with a fixed seed, and gives the median of seven runs. Each line changes one thing, so the effect of a change is the ratio of two lines. These are from an arm64 Mac (macOS 26) with Python 3.12.13. "Run" is `env.run()` alone, and "total" adds the teardown, which waits until every sink has taken every batch:

| Configuration | Run, seconds | Total, seconds | Records |
|---|---|---|---|
| core only, nothing published | 0.248 | 0.248 | 0 |
| as shipped, counting sink | 0.531 | 1.138 | 157,771 |
| with 0.21.0's real-time step at `factor: 0` | 0.555 | 1.164 | 157,771 |
| with 0.21.0's timestamp | 0.555 | 1.163 | 157,771 |
| with a lag reading every second, 0.21.0's default | 0.535 | 1.144 | 162,771 |
| 0.21.0: the three lines above together | 0.604 | 1.211 | 162,771 |
| as shipped, with `gc.freeze()` after setup | 0.485 | 1.092 | 157,771 |
| `ConsoleEgress`, to `/dev/null` | 0.648 | 1.251 | 157,771 |
| 0.21.0's `ConsoleEgress`, one log call per record | 0.672 | 1.902 | 157,771 |

- **0.22.0 against 0.21.0:** the run takes 0.531 seconds against 0.604, which is 1.14 times faster. The gain comes from three changes: SimPy's own step at `factor: 0`, a timestamp formatted once per second of the clock (300 against 580 nanoseconds a record), and no lag readings at `factor: 0`.
- **Console:** in the total, which covers the run and the teardown, logging a batch with one call takes 0.11 seconds more than a counting sink. One call per record took 0.76 seconds more. A terminal is slower than `/dev/null`, so a run that prints to one costs more than this.
- **Teardown:** about 0.6 seconds of every total is the teardown. Once the queues are empty, it waits half a second for a file sink to close its last file, so a short run spends most of its time there.

The table below shows where the time of the run "as shipped" goes. It comes from a sampling profile: a thread reads the main thread's stack every 0.2 milliseconds, as py-spy does from outside the process (py-spy needs root on macOS):

| Part | Share of the run |
|---|---|
| SimPy's scheduler | 28 percent |
| publishing a record: the value made JSON-safe | 18 percent |
| publishing a record: building it, its timestamp, buffering and flushing | 21 percent |
| resource request and release | 14 percent |
| drawing samples | 9 percent |
| SimPy events and processes | 8 percent |
| the model's own generators and the rest | 3 percent |

A draw through `Sampler.sample` costs about 360 nanoseconds, against 200 for numpy's `Generator.normal` alone. A request granted and released costs about 1,760 nanoseconds, and a timeout about 260.

**Python versions.** The run "as shipped" takes 0.517 seconds on Python 3.11.15, 0.531 on 3.12.13, 0.492 on 3.13.14 and 0.556 on 3.14.6. No version is more than 13 percent faster than another.

**PyPy.** desflow does not install on PyPy, because orjson, which it depends on, does not support PyPy. With the other dependencies installed by hand, the run "as shipped" took 0.851 seconds on PyPy 3.11.15, 1.6 times slower than CPython 3.12. Drawing samples, which goes through numpy, took 23 percent of a short profile (96 samples), against 9 percent on CPython.

**Real sinks.** With the same workload, the total was 1.138 seconds with a counting sink, 1.262 with Parquet on the local disk, and 3.558 with Kafka. The Kafka run used the odctl `kafka-lite` profile on the same machine, with JSON values. A sink that sends over the network can take longer than the model.

Run `uv run --all-extras python benchmarks/throughput.py configs` to measure your machine, and `micro`, `profile` or `sinks` for the other parts.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/performance_tuning.yaml"
        # The Tune Performance guide: ten orders a second and 25 packers, with no sink of its
        # own. performance_tuning.py measures it with a counting sink.
        simulation:
          sim_id: Shop
          factor: 0  # no wall clock to wait for
          random_seed: 42

        resources:
          packers: {current_cap: 25, max_cap: 25}

        services:
          packing: {dist: normal, mean: 2 s, std: 0.4 s}

        arrivals:
          orders: {dist: exponential, rate: 10, spawn: pack_order}

        tasks:
          pack_order:
            service: packing
            resource: packers
            payload: {event_type: order_packed}

        # minimal: readings every 10 s, not the costlier task statistics
        stats: {level: minimal, interval: 10}

        run:
          until: 1 h
        ```

    === "Builder"

        ```python title="docs/snippets/guides/performance_tuning.py"
        """The Tune Performance guide: records per second of a model, without its sinks.

        python performance_tuning.py prints the records a one-hour run of the shop below
        published, and how many a second. `blueprint()` measures performance_tuning.yaml.
        """

        import asyncio
        import gc
        import queue
        import time

        from desflow import SimulationContext


        class Count:
            """An egress that counts the records it receives and keeps none."""

            def __init__(self):
                self.records = 0

            async def run(self, egress_queue):
                try:
                    while True:
                        try:
                            self.records += len(egress_queue.get_nowait())
                        except queue.Empty:
                            await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    pass


        def shop() -> SimulationContext:
            """Ten orders a second and 25 packers, with no sink of its own."""
            app = (
                # factor 0: no wall clock to wait for
                SimulationContext(sim_id="Shop", factor=0, random_seed=42)
                .add_resource("packers", current_cap=25, max_cap=25)
                .add_service("packing", dist="normal", mean=2.0, std=0.4)
                .add_arrival("orders", dist="exponential", rate=10.0)
                # minimal: readings every 10 s, not the costlier task statistics
                .with_stats("minimal", interval=10)
            )

            @app.task(service_id="packing", resource_id="packers")
            def pack_order(task_id: int):
                return {"event_type": "order_packed"}

            @app.arrival_loop("orders")
            def order_loop(context: SimulationContext):
                while True:
                    order = yield context.wait_for_arrival("orders")
                    context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

            return app


        def throughput(app: SimulationContext, until: float) -> dict:
            """Runs a model with a counting sink: the record count and the records per second."""
            count = Count()
            app.add_egress(count)
            gc.collect()
            gc.freeze()  # leave the objects built so far out of garbage collection
            started = time.perf_counter()
            app.run(until=until)
            seconds = time.perf_counter() - started
            gc.unfreeze()
            return {"records": count.records, "per_second": round(count.records / seconds)}


        def blueprint(path: str = "performance_tuning.yaml") -> dict:
            """The same measure of the blueprint, which loads as a context."""
            app = SimulationContext.from_yaml(path)  # a context, as the builder makes one
            return throughput(app, until=3600)


        if __name__ == "__main__":
            print(throughput(shop(), until=3600))
        ```

    === "Low-level"

        ```python title="docs/snippets/guides/performance_tuning_low_level.py"
        """The Tune Performance guide, low-level API: records per second of a model, without
        its sinks.

        python performance_tuning_low_level.py prints the records a one-hour run of the shop
        below published, and how many a second.
        """

        import gc
        import itertools
        import time

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            Sampler,
            SimParameter,
        )
        from performance_tuning import Count

        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=10.0)},
            service={"packing": DistributionConfig(dist="normal", mean=2.0, std=0.4)},
            resources={"packers": CapacityConfig(current_cap=25, max_cap=25)},
        )


        def pack_order(env, packers, sampler, key, task_id):
            path_id, stats = "Shop.service.packing", env.stats
            env.publish_event(
                key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
            )
            stats.task_queued("pack_order", task_id)
            with packers.request() as unit:
                yield unit
                env.publish_event(
                    key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
                )
                stats.task_started("pack_order", task_id)
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)
                stats.task_finished("pack_order", task_id)


        def orders(env, packers, sampler):
            gap = env.registry.get_config("Shop.arrival.orders")
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


        def shop(egress: list) -> DynamicRealtimeEnvironment:
            """Ten orders a second and 25 packers, publishing to the sinks given."""
            env = DynamicRealtimeEnvironment(factor=0)  # no wall clock to wait for
            env.registry.register_sim_parameter(PARAMS)
            env.setup_egress(egress)
            # minimal: readings every 10 s, not the costlier task statistics
            env.setup_stats("Shop", "minimal", interval=10)
            packers = DynamicResource(env, "Shop", "packers")
            env.process(orders(env, packers, Sampler(seed=42)))
            return env


        def throughput(until: float) -> dict:
            """Runs the shop with a counting sink: the record count and the records per second."""
            count = Count()
            env = shop([count])
            gc.collect()
            gc.freeze()  # leave the objects built so far out of garbage collection
            started = time.perf_counter()
            env.run(until=until)
            env.teardown()  # waits until the sink has taken every batch
            seconds = time.perf_counter() - started
            gc.unfreeze()
            return {"records": count.records, "per_second": round(count.records / seconds)}


        if __name__ == "__main__":
            print(throughput(until=3600))
        ```

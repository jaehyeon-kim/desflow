# Analyse the Event Log

Write a run's events to Parquet, read them back with pyarrow and turn them into an event log: one row for each step an order took, with the entity, the event type, the event, the time and the run. Then find each order's wait and time in the shop, and the run's throughput.

---

## Before You Start

- [Streaming Out](../../learn/running-live/streaming-out.md) writes records to files.
- [Egress](../../user-guide/live-runs-and-connectors/egress.md) describes the sink, and [Records and Telemetry](../../reference/records-and-telemetry.md) the fields of a record.
- The analysis uses pyarrow only, with no pandas. Install it with `pip install "desflow[parquet]"`.

---

## Steps

### Write the Events

Add a Parquet sink, `ParquetStorageEgress`, to the shop. It writes one flat row for each event, in files named `events_<8 hex digits>.parquet`. The finished event carries a `status`, as the queued and started events do:

=== "YAML"

    ```yaml title="docs/snippets/yaml/analyse_the_event_log.yaml:run"
    simulation:
      sim_id: Shop
      factor: 0
      random_seed: 42

    resources:
      packers: {current_cap: 1, max_cap: 1}

    services:
      packing: {dist: constant, value: 1.5}

    arrivals:
      orders: {dist: exponential, rate: 0.5, spawn: pack_order}

    tasks:
      pack_order:
        service: packing
        resource: packers
        # The finished event's value, so every event of a task has a status
        payload: {status: finished}

    egress:
      # One flat row per event, in files named events_<8 hex digits>.parquet
      - type: Parquet
        config:
          default_path: ${OUT:-analyse_output}/events.parquet

    run:
      until: 120
    ```

    `run_yaml` loads it with the folder in `OUT`, and runs it:

    ```python title="docs/snippets/guides/analyse_the_event_log.py:run-yaml"
    def run_yaml(
        out: Path, blueprint: str = "analyse_the_event_log.yaml"
    ) -> SimulationContext:
        """Runs the blueprint for two minutes into an empty folder and returns it."""
        shutil.rmtree(out, ignore_errors=True)  # a run adds files; start empty
        with patch.dict(os.environ, {"OUT": str(out)}):  # the folder the blueprint reads
            app = SimulationContext.from_yaml(blueprint)
        app.run()  # until 120, from the blueprint
        return app
    ```

=== "Builder"

    ```python title="docs/snippets/guides/analyse_the_event_log.py:run"
    def shop(out: Path) -> SimulationContext:
        """One packer, orders about every 2 seconds, events written to Parquet in out."""
        app = (
            SimulationContext(sim_id="Shop", factor=0, random_seed=42)
            .add_resource("packers", current_cap=1, max_cap=1)
            .add_service("packing", dist="constant", value=1.5)
            .add_arrival("orders", dist="exponential", rate=0.5)
            # One flat row per event, in files named events_<8 hex digits>.parquet
            .add_egress(ParquetStorageEgress(default_path=f"{out}/events.parquet"))
        )

        @app.task(service_id="packing", resource_id="packers")
        def pack_order(task_id: int):
            # The finished event's value, so every event of a task has a status
            return {"status": "finished"}

        @app.arrival_loop("orders")
        def order_generator(context: SimulationContext):
            task_id = 0
            while True:
                order = yield context.wait_for_arrival("orders")
                context.spawn(pack_order(task_id, entity=order))
                task_id += 1

        return app
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/analyse_the_event_log_low_level.py:run"
    def pack_order(env, packers, sampler, key, task_id):
        path_id = "Shop.service.packing"
        env.publish_event(
            key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
        )
        with packers.request() as unit:
            yield unit
            env.publish_event(
                key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
            )
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            # The finished event's value, so every event of a task has a status
            env.publish_event(key, {"status": "finished"}, "pack_order", task_id)


    def orders(env, packers, sampler):
        gap = env.registry.get_config("Shop.arrival.orders")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


    def shop(out: Path) -> DynamicRealtimeEnvironment:
        """One packer, orders about every 2 seconds, events written to Parquet in out."""
        env = DynamicRealtimeEnvironment(factor=0)
        env.registry.register_sim_parameter(PARAMS)
        # One flat row per event, in files named events_<8 hex digits>.parquet
        env.setup_egress([ParquetStorageEgress(default_path=f"{out}/events.parquet")])
        packers = DynamicResource(env, "Shop", "packers")
        env.process(orders(env, packers, Sampler(seed=42)))
        return env
    ```

A run adds files to the folder and never replaces them, so the script empties it before a run.

### Read Them Back

From here on, the code is the same for every form. `pyarrow.dataset` reads every file under a folder as one table:

```python title="docs/snippets/guides/analyse_the_event_log.py:read"
def read_events(out: Path) -> Table:
    """Every file the sink wrote under out, as one table of events."""
    return ds.dataset(out, format="parquet").to_table()
```

One 2-minute run of the shop writes **191** rows in every form: 64 queued events, 64 started and 63 finished, because one order is still being packed when the run ends.

### Build the Event Log

Each order's three events become the rows of an event log. A queued event is the order's arrival and its wait in the queue, a started event is the start of packing, and a finished event is the end of packing and the order leaving. The files are read in no fixed order, so the rows are sorted by time, and by the order of the statuses where times are equal:

```python title="docs/snippets/guides/analyse_the_event_log.py:log"
# Each status of a task becomes these rows of the log, in this order
ROWS = {
    "queued": [("arrival_departure", "arrival"), ("queue", "waiting_for_packer")],
    "started": [("resource_use", "packing_start")],
    "finished": [("resource_use_end", "packing_end"), ("arrival_departure", "depart")],
}
ORDER = list(ROWS)  # queued, started, finished: the order of an order's events


def event_log(events: Table, run: int = 1) -> Table:
    """The event log: one row for each step an order took, sorted by time.

    The files are read in no fixed order, so the rows are sorted by time and, at the
    same time, by the order of the statuses.
    """
    packing = events.filter(pc.equal(events["task"], "pack_order")).to_pylist()
    packing.sort(key=lambda row: (row["sim_ts"], ORDER.index(row["status"])))
    rows = [
        {
            "entity_id": row["key"],
            "event_type": event_type,
            "event": event,
            "time": row["sim_ts"],
            "run": run,
        }
        for row in packing
        for event_type, event in ROWS[row["status"]]
    ]
    return Table.from_pylist(rows)
```

The log has **318** rows for **64** orders, and its columns are `entity_id`, `event_type`, `event`, `time` and `run`. This is the form [Animate a Run](animate-a-run.md) takes.

### Find Waits and Throughput

An order's wait is the time from its arrival to the start of packing, and its time in the shop is the time from its arrival to its departure. Throughput is the number of orders that left in each minute of the run:

```python title="docs/snippets/guides/analyse_the_event_log.py:waits"
def times(log: Table, event: str) -> dict:
    """The time of `event` for each order that reached it."""
    rows = log.filter(pc.equal(log["event"], event))
    return dict(zip(rows["entity_id"].to_pylist(), rows["time"].to_pylist()))


def waits_and_times(log: Table) -> dict:
    """Each order's wait, and its time in the shop if it left before the run ended."""
    arrived = times(log, "arrival")
    started = times(log, "packing_start")
    left = times(log, "depart")
    return {
        order: {
            "wait": round(started[order] - arrived[order], 3),
            "in_shop": round(left[order] - arrived[order], 3)
            if order in left
            else None,
        }
        for order in started
    }


def mean_wait(per_order: dict) -> float:
    """The mean wait of the orders that started packing, in seconds."""
    return pc.mean(pa.array([row["wait"] for row in per_order.values()])).as_py()


def throughput_per_minute(log: Table, seconds: float) -> float:
    """Orders that left the shop in each minute of the run."""
    return len(times(log, "depart")) / (seconds / 60)
```

---

## Check the Result

Save the files of your form from [Full Files](#full-files) in a folder of their own, each under the last part of its title. Every form needs `analyse_the_event_log.py`, which holds the analysis. Run from that folder, each writes the files to `analyse_output/` and prints the counts. The forms write the same events, so they print the same counts:

=== "YAML"

    The files are `analyse_the_event_log.yaml` and `analyse_the_event_log.py`. The argument `yaml` makes the script run the blueprint with `run_yaml`:

    ```bash
    python analyse_the_event_log.py yaml
    ```

=== "Builder"

    The file is `analyse_the_event_log.py`:

    ```bash
    python analyse_the_event_log.py
    ```

=== "Low-level"

    The files are `analyse_the_event_log_low_level.py` and `analyse_the_event_log.py`:

    ```bash
    python analyse_the_event_log_low_level.py
    ```

```text
318 rows in the log, for 64 orders
mean wait 1.250 s
throughput 31.5 orders a minute
```

The first order waits **0.0** seconds and is in the shop for **1.5** seconds. The mean wait is **1.250** seconds and the longest wait is **3.66** seconds. These come from one 2-minute run of a shop with one packer, not from the busy hour of the tutorials. **63** orders leave in two minutes, a throughput of **31.5** orders a minute. The 64th is still being packed, so it has a wait and no time in the shop.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/analyse_the_event_log.yaml"
        # The Analyse the Event Log guide: one packer, orders about every 2 seconds, events
        # written to Parquet. The files go to analyse_output/, or to the folder in OUT, which
        # run_yaml() in analyse_the_event_log.py sets. Run it with:
        # desflow run analyse_the_event_log.yaml
        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42

        resources:
          packers: {current_cap: 1, max_cap: 1}

        services:
          packing: {dist: constant, value: 1.5}

        arrivals:
          orders: {dist: exponential, rate: 0.5, spawn: pack_order}

        tasks:
          pack_order:
            service: packing
            resource: packers
            # The finished event's value, so every event of a task has a status
            payload: {status: finished}

        egress:
          # One flat row per event, in files named events_<8 hex digits>.parquet
          - type: Parquet
            config:
              default_path: ${OUT:-analyse_output}/events.parquet

        run:
          until: 120
        ```

        The analysis and `run_yaml` are in `analyse_the_event_log.py`, in the Builder tab.

    === "Builder"

        ```python title="docs/snippets/guides/analyse_the_event_log.py"
        """Analyse the Event Log: a run written to Parquet, read back as an event log.

        The shop runs for two minutes with one packer and writes its events to Parquet. The
        script reads the files with pyarrow, turns each order's three events into the rows of
        an event log (entity, event type, event, time, run), and computes each order's wait
        and time in the shop and the run's throughput. It uses pyarrow only, no pandas.
        python analyse_the_event_log.py writes the files to analyse_output/. With the
        argument yaml, `run_yaml` runs analyse_the_event_log.yaml (beside the script) instead,
        and analyse_the_event_log_low_level.py is the low-level API; each writes the same
        events.
        """

        import logging
        import os
        import shutil
        import sys
        from pathlib import Path
        from unittest.mock import patch

        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.dataset as ds
        from pyarrow import Table

        from desflow import ParquetStorageEgress, SimulationContext

        logging.basicConfig(level=logging.WARNING)

        RUN_SECONDS = 120


        def shop(out: Path) -> SimulationContext:
            """One packer, orders about every 2 seconds, events written to Parquet in out."""
            app = (
                SimulationContext(sim_id="Shop", factor=0, random_seed=42)
                .add_resource("packers", current_cap=1, max_cap=1)
                .add_service("packing", dist="constant", value=1.5)
                .add_arrival("orders", dist="exponential", rate=0.5)
                # One flat row per event, in files named events_<8 hex digits>.parquet
                .add_egress(ParquetStorageEgress(default_path=f"{out}/events.parquet"))
            )

            @app.task(service_id="packing", resource_id="packers")
            def pack_order(task_id: int):
                # The finished event's value, so every event of a task has a status
                return {"status": "finished"}

            @app.arrival_loop("orders")
            def order_generator(context: SimulationContext):
                task_id = 0
                while True:
                    order = yield context.wait_for_arrival("orders")
                    context.spawn(pack_order(task_id, entity=order))
                    task_id += 1

            return app


        def run(out: Path) -> SimulationContext:
            """Runs the shop for two minutes into an empty folder and returns it."""
            shutil.rmtree(out, ignore_errors=True)  # a run adds files; start empty
            app = shop(out)
            app.run(until=RUN_SECONDS)
            return app


        def run_yaml(
            out: Path, blueprint: str = "analyse_the_event_log.yaml"
        ) -> SimulationContext:
            """Runs the blueprint for two minutes into an empty folder and returns it."""
            shutil.rmtree(out, ignore_errors=True)  # a run adds files; start empty
            with patch.dict(os.environ, {"OUT": str(out)}):  # the folder the blueprint reads
                app = SimulationContext.from_yaml(blueprint)
            app.run()  # until 120, from the blueprint
            return app


        def read_events(out: Path) -> Table:
            """Every file the sink wrote under out, as one table of events."""
            return ds.dataset(out, format="parquet").to_table()


        # Each status of a task becomes these rows of the log, in this order
        ROWS = {
            "queued": [("arrival_departure", "arrival"), ("queue", "waiting_for_packer")],
            "started": [("resource_use", "packing_start")],
            "finished": [("resource_use_end", "packing_end"), ("arrival_departure", "depart")],
        }
        ORDER = list(ROWS)  # queued, started, finished: the order of an order's events


        def event_log(events: Table, run: int = 1) -> Table:
            """The event log: one row for each step an order took, sorted by time.

            The files are read in no fixed order, so the rows are sorted by time and, at the
            same time, by the order of the statuses.
            """
            packing = events.filter(pc.equal(events["task"], "pack_order")).to_pylist()
            packing.sort(key=lambda row: (row["sim_ts"], ORDER.index(row["status"])))
            rows = [
                {
                    "entity_id": row["key"],
                    "event_type": event_type,
                    "event": event,
                    "time": row["sim_ts"],
                    "run": run,
                }
                for row in packing
                for event_type, event in ROWS[row["status"]]
            ]
            return Table.from_pylist(rows)


        def times(log: Table, event: str) -> dict:
            """The time of `event` for each order that reached it."""
            rows = log.filter(pc.equal(log["event"], event))
            return dict(zip(rows["entity_id"].to_pylist(), rows["time"].to_pylist()))


        def waits_and_times(log: Table) -> dict:
            """Each order's wait, and its time in the shop if it left before the run ended."""
            arrived = times(log, "arrival")
            started = times(log, "packing_start")
            left = times(log, "depart")
            return {
                order: {
                    "wait": round(started[order] - arrived[order], 3),
                    "in_shop": round(left[order] - arrived[order], 3)
                    if order in left
                    else None,
                }
                for order in started
            }


        def mean_wait(per_order: dict) -> float:
            """The mean wait of the orders that started packing, in seconds."""
            return pc.mean(pa.array([row["wait"] for row in per_order.values()])).as_py()


        def throughput_per_minute(log: Table, seconds: float) -> float:
            """Orders that left the shop in each minute of the run."""
            return len(times(log, "depart")) / (seconds / 60)


        if __name__ == "__main__":
            folder = Path("analyse_output")
            run_yaml(folder) if sys.argv[1:] == ["yaml"] else run(folder)
            log = event_log(read_events(folder))
            per_order = waits_and_times(log)
            print(f"{log.num_rows} rows in the log, for {len(per_order)} orders")
            print(f"mean wait {mean_wait(per_order):.3f} s")
            print(f"throughput {throughput_per_minute(log, RUN_SECONDS):.1f} orders a minute")
        ```

    === "Low-level"

        ```python title="docs/snippets/guides/analyse_the_event_log_low_level.py"
        """The Analyse the Event Log guide, low-level API: the shop written to Parquet.

        The low-level version of the shop in analyse_the_event_log.py: one packer, orders
        about every 2 seconds, each event written to Parquet with its status. python
        analyse_the_event_log_low_level.py writes the files to analyse_output/ and analyses
        them with the functions of analyse_the_event_log.py, which must be beside it.
        """

        import itertools
        import logging
        import shutil
        from pathlib import Path

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            ParquetStorageEgress,
            Sampler,
            SimParameter,
        )

        logging.basicConfig(level=logging.WARNING)

        RUN_SECONDS = 120
        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
            service={"packing": DistributionConfig(dist="constant", value=1.5)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
        )


        def pack_order(env, packers, sampler, key, task_id):
            path_id = "Shop.service.packing"
            env.publish_event(
                key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
            )
            with packers.request() as unit:
                yield unit
                env.publish_event(
                    key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
                )
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                # The finished event's value, so every event of a task has a status
                env.publish_event(key, {"status": "finished"}, "pack_order", task_id)


        def orders(env, packers, sampler):
            gap = env.registry.get_config("Shop.arrival.orders")
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


        def shop(out: Path) -> DynamicRealtimeEnvironment:
            """One packer, orders about every 2 seconds, events written to Parquet in out."""
            env = DynamicRealtimeEnvironment(factor=0)
            env.registry.register_sim_parameter(PARAMS)
            # One flat row per event, in files named events_<8 hex digits>.parquet
            env.setup_egress([ParquetStorageEgress(default_path=f"{out}/events.parquet")])
            packers = DynamicResource(env, "Shop", "packers")
            env.process(orders(env, packers, Sampler(seed=42)))
            return env


        def run(out: Path) -> DynamicRealtimeEnvironment:
            """Runs the shop for two minutes into an empty folder and returns it."""
            shutil.rmtree(out, ignore_errors=True)  # a run adds files; start empty
            env = shop(out)
            env.run(until=RUN_SECONDS)
            env.teardown()  # writes the last files
            return env


        if __name__ == "__main__":
            from analyse_the_event_log import (
                event_log,
                mean_wait,
                read_events,
                throughput_per_minute,
                waits_and_times,
            )

            folder = Path("analyse_output")
            run(folder)
            log = event_log(read_events(folder))
            per_order = waits_and_times(log)
            print(f"{log.num_rows} rows in the log, for {len(per_order)} orders")
            print(f"mean wait {mean_wait(per_order):.3f} s")
            print(f"throughput {throughput_per_minute(log, RUN_SECONDS):.1f} orders a minute")
        ```

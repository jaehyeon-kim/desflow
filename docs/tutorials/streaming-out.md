# Streaming Out

The shop of [Changing Demand](changing-demand.md), without the rush, packs orders for an hour. It has one packer, which takes 1.5 seconds to pack an order, and an order arrives every 2.5 seconds on average (24 a minute, `rate: 0.4`). A team downstream wants a file of the finished orders. This page asks: **which records reach the team's files, how does the team find every record of one order, and when are the files complete?**

- **Decision:** what the file sink receives, and when the team's job may read the folder.
- **Measures:** the records each sink receives, the files the file sink writes and the rows in each.
- **Runs:** one unpaced run (`factor: 0`) of the hour with the seed 42.
- **Assumptions:** the default batch of 500 records.

## What You Add

This page adds two **sinks**: `ConsoleEgress`, which prints every record to the terminal, and `JsonlStorageEgress`, which writes JSON Lines files. The file sink has a `when` predicate that passes only the finished orders. [Egress, Batching and Delivery](../user-guide/egress.md) describes every sink.

---

## Choose What Each Sink Gets

Every sink receives every event and every telemetry record, unless its `when` says otherwise. `when` is a function that takes one record and returns True to send it to that sink. This one passes the events whose value says `order_packed`:

=== "YAML"

    ```python title="docs/snippets/yaml/streaming_out_logic.py:packed"
    def packed_only(record: dict) -> bool:
        """Keeps the finished orders: events whose value says order_packed."""
        return (
            record["stream_type"] == "event"
            and record["value"].get("event_type") == "order_packed"
        )
    ```

    A blueprint names the function with `!python module.function`, and the module sits beside the blueprint:

    ```yaml title="docs/snippets/yaml/streaming_out.yaml:sinks"
    egress:
      # Every event and every telemetry record
      - type: Console
      # The finished orders only, one flat row each
      - type: Jsonl
        config: {default_path: packed/orders.jsonl}
        when: !python streaming_out_logic.packed_only
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/streaming_out.py:packed"
    def packed_only(record: dict) -> bool:
        """Keeps the finished orders: events whose value says order_packed."""
        return (
            record["stream_type"] == "event"
            and record["value"].get("event_type") == "order_packed"
        )
    ```

    ```python title="docs/snippets/tutorials/streaming_out.py:sinks"
    # Every event and every telemetry record
    .add_egress(ConsoleEgress())
    # The finished orders only, one flat row each
    .add_egress(
        JsonlStorageEgress(default_path=f"{out}/packed/orders.jsonl"),
        when=packed_only,
    )
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/streaming_out_low_level.py:sinks"
    env.setup_egress(
        [
            # Every event and every telemetry record
            ConsoleEgress(),
            # The finished orders only, one flat row each
            JsonlStorageEgress(default_path=f"{out}/packed/orders.jsonl"),
        ],
        predicates=[None, packed_only],
    )
    ```

    `predicates` takes one entry per sink, in the order of the list, and `None` passes every record. `packed_only` is the same function as in the Builder tab.

The predicate sees the record as published, so an event's payload is still under `value`. It checks `stream_type` first, because a telemetry record's `value` is a single number. A record that no sink passes is written nowhere.

---

## Find One Order's Records

Save the files from [Full Files](#full-files) in one folder and run the commands there: `desflow run streaming_out.yaml`, `python streaming_out.py` or `python streaming_out_low_level.py`. Every event carries three names. `key` names the entity, here the order, and the arrival names the orders `orders-1`, `orders-2`, and so on. `task` names the task, and `instance` names this run of it. The terminal shows the three events of `orders-12`, each with the same three names:

```text
[EVT] {'sim_ts': 26.97, 'timestamp': '...', 'key': 'orders-12', 'task': 'pack_order', 'instance': 11, 'value': {'path_id': 'Shop.service.packing', 'status': 'queued'}}
[EVT] {'sim_ts': 28.202, 'timestamp': '...', 'key': 'orders-12', 'task': 'pack_order', 'instance': 11, 'value': {'path_id': 'Shop.service.packing', 'status': 'started'}}
[EVT] {'sim_ts': 29.702, 'timestamp': '...', 'key': 'orders-12', 'task': 'pack_order', 'instance': 11, 'value': {'event_type': 'order_packed'}}
```

The file sink receives only the last of the three, the `order_packed` event. Without a router it writes one flat row per event, with the payload's fields beside the record's own:

```text
{"stream_type":"event","sim_ts":29.702,"timestamp":"...","key":"orders-12","task":"pack_order","instance":11,"event_type":"order_packed"}
```

To find one order's records among the 4,329 of the hour, print the records as JSON lines with `--stdout` and keep the lines with its key. `--stdout` sends the records to the terminal in place of the sinks, so this run writes no files:

```bash
desflow run streaming_out.yaml --stdout | grep '"key":"orders-12"'
```

It prints the three events of `orders-12` above, each as the whole record. In the builder and the low-level API, `ConsoleEgress(format="json")` as the sink prints the same lines.

A reader that joins the files with another source uses `key`, and one that follows a task through its states uses `task` and `instance`. [Records and Telemetry](../reference/records-and-telemetry.md#record-shape) lists every field, and [What Each Sink Writes](../reference/records-and-telemetry.md#what-each-sink-writes) shows how each sink lays them out.

---

## Read the Files

The file sink writes one file per batch, named after `default_path` with 8 random hexadecimal digits, such as `packed/orders_1a2b3c4d.jsonl`. A file, once written, is never changed:

```python title="docs/snippets/tutorials/streaming_out.py:read"
def read_files(folder: Path) -> dict:
    """The rows of each file the sink wrote to folder, by file name."""
    return {
        path.name: [json.loads(line) for line in path.read_text().splitlines()]
        for path in sorted(folder.glob("*.jsonl"))
    }
```

The names are random, so the order of the files is not the order of the records. Sort by `sim_ts` when the order matters. A second run in the same folder adds new files beside the old ones, so the scripts empty `packed/` before they run.

---

## Know When the Files Are Complete

The model and the sinks finish at different times. The model stops at `until`. A sink receives records in batches. At `factor: 0`, a batch is handed over only when it holds 500 records, and the records left over are handed over at **teardown**, after the model has stopped. Teardown hands over what each sink still holds, then waits until every sink has written it:

=== "YAML"

    `desflow run` tears the run down when it reaches `run.until`, and returns once the sinks have finished.

=== "Builder"

    `app.run(until="1 h")` tears the run down when it reaches the end, and returns once the sinks have finished.

=== "Low-level"

    ```python title="docs/snippets/tutorials/streaming_out_low_level.py:run"
    try:
        env.run(until=3600)  # the model stops at 1 hour
    finally:
        env.teardown()  # the sinks receive the rest and finish writing
    ```

    `env.run` runs SimPy's event loop and stops at `until`, as [SimPy and desflow](../des/simpy-and-desflow.md#environment-and-the-event-list) describes. `env.teardown()` is desflow's step that hands over the rest and waits for the sinks.

A job that reads the folder while the run is going therefore sees only the full batches. The terminal can show the same effect. On a fast machine the model can finish before the console sink prints its first line, and then every line appears after the log says `Tearing down egress connectors`. Teardown waits for the sinks to finish writing, and gives up only when they have made no progress for `drain_stall_seconds`, as [Egress](../user-guide/egress.md#how-it-works) describes. Records not written by then are lost, with a warning.

---

## Result

The rows the file sink writes over the hour, counted by the time each order was packed:

![The packed orders, counted by the time each was packed, rise steadily over the hour to 1,432. File 1 holds the 500 orders packed in the first 20.5 minutes, file 2 the 500 packed from 20.5 to 40.6 minutes, and file 3 the 432 packed from 40.6 minutes to the end of the hour](../assets/charts/streaming-rows.png)

Which records reach the team's files?

- The terminal receives every record: **4,299** events (**1,434** orders queued, **1,433** started and **1,432** packed) and **30** telemetry records, five statistics read every 10 minutes.
- The file sink receives only the **1,432** packed orders, one row each, in **3** files of **500**, **500** and **432** rows. The two files of 500 were handed over while the model ran. The file of 432 was handed over at teardown, after the model stopped.

The run returns only when teardown has finished, so the team's job should start only after the run has returned. One order's records share its `key`: `orders-12` was queued at **26.97** seconds, started at **28.202** and packed at **29.702**.

The Builder and Low-level scripts print:

```text
3 files, with [432, 500, 500] rows
orders-12: {'stream_type': 'event', 'sim_ts': 29.702, 'key': 'orders-12', 'task': 'pack_order', 'instance': 11, 'event_type': 'order_packed'}
```

## Why It Works

A record goes from `publish_event` to each sink's `when` test, then into that sink's buffer, then to a queue that a separate thread writes from. The model only fills buffers, so it can finish while the threads are still writing. Teardown is the step that waits for them. Writing records on background threads is one of the things desflow adds to SimPy, as [What desflow Changes](../des/coming-from-simpy.md#what-desflow-changes) sets out. [Egress, Batching and Delivery](../user-guide/egress.md#how-it-works) shows each stage of that path.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/streaming_out.yaml"
        # The Streaming Out tutorial: the shop's records, to the terminal and to a file.
        # Run it with: desflow run streaming_out.yaml
        # Every record is printed; the finished orders are written to packed/.
        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42

        resources:
          packers: {current_cap: 1, max_cap: 1}

        services:
          packing: {dist: constant, value: 1.5 s}

        arrivals:
          orders: {dist: exponential, rate: 0.4, spawn: pack_order}

        tasks:
          pack_order:
            service: packing
            resource: packers
            payload: {event_type: order_packed}

        # The packers' and the packing task's readings every 10 minutes, as telemetry
        stats: {level: minimal, interval: 10 min}

        egress:
          # Every event and every telemetry record
          - type: Console
          # The finished orders only, one flat row each
          - type: Jsonl
            config: {default_path: packed/orders.jsonl}
            when: !python streaming_out_logic.packed_only

        run:
          until: 1 h
        ```

        ```python title="docs/snippets/yaml/streaming_out_logic.py"
        """Python for the Streaming Out blueprint beside it: the file sink's predicate."""


        def packed_only(record: dict) -> bool:
            """Keeps the finished orders: events whose value says order_packed."""
            return (
                record["stream_type"] == "event"
                and record["value"].get("event_type") == "order_packed"
            )
        ```

    === "Builder"

        ```python title="docs/snippets/tutorials/streaming_out.py"
        """The Streaming Out tutorial: the shop's records, to the terminal and to a file, builder.

        One packer takes 1.5 seconds to pack an order, and an order arrives every 2.5 seconds
        on average (rate 0.4), for an hour. Every record is printed to the terminal, and the
        finished orders are written to packed/. The script then prints the files the sink
        wrote and one order's row. To see only those lines, run:
        python streaming_out.py 2>/dev/null
        """

        import json
        import logging
        import shutil
        from pathlib import Path

        from desflow import ConsoleEgress, JsonlStorageEgress, SimulationContext

        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
        )


        def packed_only(record: dict) -> bool:
            """Keeps the finished orders: events whose value says order_packed."""
            return (
                record["stream_type"] == "event"
                and record["value"].get("event_type") == "order_packed"
            )


        def shop(out: Path) -> SimulationContext:
            """The shop of Changing Demand, without the rush, writing under out."""
            app = (
                SimulationContext(sim_id="Shop", factor=0, random_seed=42)
                .add_resource("packers", current_cap=1, max_cap=1)
                .add_service("packing", dist="constant", value=1.5)
                .add_arrival("orders", dist="exponential", rate=0.4)
                # The packers' and the packing task's readings every 10 minutes, as telemetry
                .with_stats("minimal", interval=600)
                # Every event and every telemetry record
                .add_egress(ConsoleEgress())
                # The finished orders only, one flat row each
                .add_egress(
                    JsonlStorageEgress(default_path=f"{out}/packed/orders.jsonl"),
                    when=packed_only,
                )
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


        def read_files(folder: Path) -> dict:
            """The rows of each file the sink wrote to folder, by file name."""
            return {
                path.name: [json.loads(line) for line in path.read_text().splitlines()]
                for path in sorted(folder.glob("*.jsonl"))
            }


        def outcomes(out: Path) -> dict:
            """Runs the hour; returns the rows in each file and the row of orders-12."""
            shutil.rmtree(out / "packed", ignore_errors=True)  # a run adds files; start empty
            shop(out).run(until="1 h")
            files = read_files(out / "packed")
            rows = [row for file_rows in files.values() for row in file_rows]
            return {
                "file_rows": sorted(len(file_rows) for file_rows in files.values()),
                "row": next(row for row in rows if row["key"] == "orders-12"),
            }


        if __name__ == "__main__":
            r = outcomes(Path("streaming_out_output"))
            print(f"{len(r['file_rows'])} files, with {r['file_rows']} rows")
            row = {k: v for k, v in r["row"].items() if k != "timestamp"}
            print(f"orders-12: {row}")
        ```

    === "Low-level"

        ```python title="docs/snippets/tutorials/streaming_out_low_level.py"
        """The Streaming Out tutorial: the shop's records, to the terminal and to a file, low-level.

        One packer takes 1.5 seconds to pack an order, and an order arrives every 2.5 seconds
        on average (rate 0.4), for an hour. Every record is printed to the terminal, and the
        finished orders are written to packed/. The script then prints the files the sink
        wrote and one order's row. To see only those lines, run:
        python streaming_out_low_level.py 2>/dev/null
        """

        import itertools
        import json
        import logging
        import shutil
        from pathlib import Path

        from desflow import (
            CapacityConfig,
            ConsoleEgress,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            JsonlStorageEgress,
            Sampler,
            SimParameter,
        )

        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
        )

        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
            service={"packing": DistributionConfig(dist="constant", value=1.5)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
        )


        def packed_only(record: dict) -> bool:
            """Keeps the finished orders: events whose value says order_packed."""
            return (
                record["stream_type"] == "event"
                and record["value"].get("event_type") == "order_packed"
            )


        def pack_order(env, sampler, packers, key, task_id):
            path_id = "Shop.service.packing"

            def publish(value):
                # The key names the order; task and instance name this run of the task
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


        def order_generator(env, sampler, packers):
            arrival = env.registry.get_config("Shop.arrival.orders")
            for task_id in itertools.count():
                yield env.timeout(sampler.sample(arrival))
                key = f"orders-{task_id + 1}"
                env.process(pack_order(env, sampler, packers, key, task_id))


        def run(out: Path):
            env = DynamicRealtimeEnvironment(factor=0)
            env.registry.register_sim_parameter(PARAMS)
            env.setup_egress(
                [
                    # Every event and every telemetry record
                    ConsoleEgress(),
                    # The finished orders only, one flat row each
                    JsonlStorageEgress(default_path=f"{out}/packed/orders.jsonl"),
                ],
                predicates=[None, packed_only],
            )
            # The packers' and the packing task's readings every 10 minutes, as telemetry
            env.setup_stats("Shop", "minimal", interval=600)
            packers = DynamicResource(env, "Shop", "packers")
            env.process(order_generator(env, Sampler(seed=42), packers))
            try:
                env.run(until=3600)  # the model stops at 1 hour
            finally:
                env.teardown()  # the sinks receive the rest and finish writing
            return env


        def read_files(folder: Path) -> dict:
            """The rows of each file the sink wrote to folder, by file name."""
            return {
                path.name: [json.loads(line) for line in path.read_text().splitlines()]
                for path in sorted(folder.glob("*.jsonl"))
            }


        def outcomes(out: Path) -> dict:
            """Runs the hour; returns the rows in each file and the row of orders-12."""
            shutil.rmtree(out / "packed", ignore_errors=True)  # a run adds files; start empty
            run(out)
            files = read_files(out / "packed")
            rows = [row for file_rows in files.values() for row in file_rows]
            return {
                "file_rows": sorted(len(file_rows) for file_rows in files.values()),
                "row": next(row for row in rows if row["key"] == "orders-12"),
            }


        if __name__ == "__main__":
            r = outcomes(Path("streaming_out_output"))
            print(f"{len(r['file_rows'])} files, with {r['file_rows']} rows")
            row = {k: v for k, v in r["row"].items() if k != "timestamp"}
            print(f"orders-12: {row}")
        ```

[Experiments](../user-guide/experiments/index.md) describes every setting of an experiment, its files and its report, and the rest of the [User Guide](../user-guide/ways-to-write.md) describes each part of desflow in full. The [Kafka Digital Twin](../examples/kafka.md) example takes live changes from Kafka and sends its records to Kafka.

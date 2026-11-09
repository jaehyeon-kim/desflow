# Data at Scale

Generate a dataset of the size you name, such as a month of orders for 1,000 customers, as one Parquet file for each day, and check that it is complete. The run needs no wall-clock time: it takes as long as the machine needs to compute the records.

For the speed of one run, see [Tune Performance](../tools/performance-tuning.md). This page is about the size of the data and how to split a job that is too large for one run.

---

## Before You Start

- [Write to Cloud Storage](../storage-and-tables/cloud-storage.md#write-a-file-for-each-day) explains `__path__` and how the sinks name their files.
- [Time and Pacing](../../user-guide/time-and-pacing/index.md) explains `factor: 0` and `logical_start_time`.
- Install the extra: `pip install "desflow[parquet]"`.
- The code runs in a folder of your choice. The files go to `scale_output/`, or to the folder in `OUT`. `CUSTOMERS` and `DAYS` set the size, and a small value for each is the quickest way to try the page, such as `CUSTOMERS=20 DAYS=3`.

---

## Steps

### Set the Span

A run covers the period you want when it starts at the first day, runs without waiting, and stops at the last. `factor: 0` removes the wait, `logical_start_time` and `timezone` fix the calendar the file names are built from, and `run.until` is the length of the period:

=== "YAML"

    ```yaml title="docs/snippets/yaml/data_at_scale.yaml:span"
    simulation:
      sim_id: Shop
      factor: 0
      random_seed: ${SEED:-42}
      timezone: UTC
      logical_start_time: "2026-01-01T00:00:00+00:00"

    run: {until: "${DAYS:-30} days"}
    ```

=== "Builder"

    ```python title="docs/snippets/guides/data_at_scale.py:span"
    def new_shop() -> SimulationContext:
        """A shop that runs without waiting and starts on 1 January 2026, UTC."""
        return SimulationContext(
            sim_id="Shop",
            factor=0,
            random_seed=int(os.getenv("SEED", "42")),
            timezone="UTC",
            logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/data_at_scale_low_level.py:span"
    def new_env() -> DynamicRealtimeEnvironment:
        """An environment that runs without waiting and starts on 1 January 2026, UTC."""
        return DynamicRealtimeEnvironment(
            factor=0,
            logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            timezone="UTC",
        )
    ```

The period is 30 days unless `DAYS` says otherwise.

### Set the Volume

The number of records comes from how many entities there are and how often each one acts. This shop has 1,000 customers, and each orders after a gap of 12 to 36 hours, so a customer places about one order a day. `count` makes the customers, `key` gives each its name, and the gap is drawn again after every order. The month then holds about 30,000 orders. For a different volume, change `CUSTOMERS`, or the gap:

=== "YAML"

    ```yaml title="docs/snippets/yaml/data_at_scale.yaml:volume"
    arrivals:
      # Each customer orders about once a day: a gap of 12 to 36 hours, drawn again after
      # each order
      customers:
        dist: uniform
        low: 43200
        high: 129600
        spawn: place_order
        count: ${CUSTOMERS:-1000}
        key: {gen: sequence, start: "${FIRST:-1}", format: "customer-{}"}
    ```

=== "Builder"

    ```python title="docs/snippets/guides/data_at_scale.py:volume"
    def add_customers(app: SimulationContext) -> None:
        """One arrival for each customer, ordering about once a day (12 to 36 hours apart).
        A blueprint's `count` is one arrival here, each with its own key."""
        for number in range(FIRST, FIRST + CUSTOMERS):
            app.add_arrival(
                f"customer-{number}",
                "uniform",
                low=43200,
                high=129600,
                spawn="place_order",
                key=lambda n, number=number: f"customer-{number}",
            )
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/data_at_scale_low_level.py:volume"
    def add_customers(env: DynamicRealtimeEnvironment) -> None:
        """One process for each customer, ordering about once a day (12 to 36 hours apart)."""
        sampler = Sampler(seed=int(os.getenv("SEED", "42")))
        gap = DistributionConfig(dist="uniform", low=43200, high=129600)
        amounts = sampler.generator("tasks.place_order.payload.amount")
        sent = {"orders": 0}

        def customer(key):
            while True:
                yield env.timeout(sampler.sample(gap))
                amount = round(float(amounts.normal(40, 10)), 2)
                order = order_row(env, key, sent["orders"], amount)
                env.publish_event(key, order, task="place_order", instance=sent["orders"])
                sent["orders"] += 1

        for number in range(FIRST, FIRST + CUSTOMERS):
            env.process(customer(f"customer-{number}"))
    ```

`keys` takes a list of names instead of `count` and `key`, for entities with names you choose.

### Write a File for Each Day

`__path__` in the order sends it to the file of its day, as [Write to Cloud Storage](../storage-and-tables/cloud-storage.md#write-a-file-for-each-day) explains. A Parquet sink writes one file for each batch, so a `batch_size` of 10,000 makes a file of up to 10,000 rows instead of one for every 500 records, the default. A file name is the path with an eight-digit hexadecimal suffix, such as `2026-01-01_a28fed01.parquet`:

=== "YAML"

    ```yaml title="docs/snippets/yaml/data_at_scale.yaml:files"
    tasks:
      place_order:
        payload:
          # Numbered within the run, so the first customer of the run is part of the id
          order_id: {gen: sequence, format: "${FIRST:-1}-{}"}
          customer: {gen: entity}
          amount: {gen: normal, mean: 40, std: 10, decimals: 2}
          # The file of the order: the day it was placed, in the simulated calendar
          __path__: {gen: format_time, format: "${OUT:-scale_output}/%Y-%m-%d.parquet"}

    egress:
      - type: Parquet
        # One file for each batch, so a large batch gives few, large files
        batch_size: 10000
        config:
          default_path: ${OUT:-scale_output}/other.parquet
    ```

=== "Builder"

    ```python title="docs/snippets/guides/data_at_scale.py:files"
    def add_files(app: SimulationContext) -> None:
        """A Parquet sink with a large batch, and an order that names the file of its day."""
        # One file for each batch, so a large batch gives few, large files
        app.add_egress(
            ParquetStorageEgress(default_path=f"{OUT}/other.parquet"), batch_size=10000
        )
        order = Template(
            {
                # Numbered within the run, so the first customer of the run is part of the id
                "order_id": {"gen": "sequence", "format": f"{FIRST}-{{}}"},
                "customer": {"gen": "entity"},
                "amount": {"gen": "normal", "mean": 40, "std": 10, "decimals": 2},
                # The file of the order: the day it was placed, in the simulated calendar
                "__path__": {"gen": "format_time", "format": f"{OUT}/%Y-%m-%d.parquet"},
            },
            name="tasks.place_order",
        )

        @app.task(name="place_order")
        def place_order(task_id):
            return order
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/data_at_scale_low_level.py:files"
    def setup_files(env: DynamicRealtimeEnvironment) -> None:
        """A Parquet sink with a large batch. One file for each batch, so a large batch
        gives few, large files."""
        env.setup_egress(
            [ParquetStorageEgress(default_path=f"{OUT}/other.parquet")], batch_size=10000
        )


    def order_row(env: DynamicRealtimeEnvironment, key: str, number: int, amount: float):
        """An order that names the file of its day."""
        # The file of the order: the day it was placed, in the simulated calendar
        day = env.start_datetime + timedelta(seconds=env.now)
        return {
            # Numbered within the run, so the first customer of the run is part of the id
            "order_id": f"{FIRST}-{number}",
            "customer": key,
            "amount": amount,
            "__path__": day.strftime(f"{OUT}/%Y-%m-%d.parquet"),
        }
    ```

### Split a Large Job

One run keeps its entities and its records in one process. When a month for 1,000 customers is too much for one run, or too slow, run several shards, each as a separate process with its own part of the customers. Give each shard its own `FIRST`, `CUSTOMERS` and `SEED`, so that no two shards share a customer or a random sequence. They can write to the same folder, because every file name has its own suffix:

=== "YAML"

    ```bash
    desflow run data_at_scale.yaml --var FIRST=1 --var CUSTOMERS=500 --seed 1 &
    desflow run data_at_scale.yaml --var FIRST=501 --var CUSTOMERS=500 --seed 2 &
    wait
    ```

=== "Builder"

    ```bash
    FIRST=1 CUSTOMERS=500 SEED=1 python data_at_scale.py &
    FIRST=501 CUSTOMERS=500 SEED=2 python data_at_scale.py &
    wait
    ```

=== "Low-level"

    ```bash
    FIRST=1 CUSTOMERS=500 SEED=1 python data_at_scale_low_level.py &
    FIRST=501 CUSTOMERS=500 SEED=2 python data_at_scale_low_level.py &
    wait
    ```

Each shard writes its own file for each day, so a day has one file for each shard. The order ids start with the number of the shard's first customer, so they stay unique across shards. To split by period instead, give each shard its own `logical_start_time` and `run.until`.

`desflow run --replications` does not do this. A replication runs the model with no ingress and no egress, so it writes no files. It is for the spread of a result, not for data.

---

## Check the Result

Count the rows and the files of each day, and compare the total with the volume the model should give. Each customer orders about once a day, but the first order comes 12 to 36 hours in, so the total is about half a day short of customers times days. Save `data_at_scale_check.py` from [Full Files](#full-files) and run it from the folder of the files:

```python title="docs/snippets/guides/data_at_scale_check.py:check"
import sys
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq


def check(folder: str, customers: int, days: int) -> int:
    """Prints the files and rows of each day and returns the total rows."""
    rows: Counter = Counter()
    files: Counter = Counter()
    for path in sorted(Path(folder).glob("*.parquet")):
        day = path.name[:10]  # the day in the file name, such as 2026-01-01
        files[day] += 1
        rows[day] += pq.ParquetFile(path).metadata.num_rows
    for day in sorted(rows):
        print(f"{day}  {files[day]} file  {rows[day]:>7} rows")
    # Each customer orders once a day on average. Its first order comes 12 to 36 hours
    # in, so the total is about half a day short of customers * days.
    expected = customers * (days - 0.46)
    total = sum(rows.values())
    print(f"total {total} rows, expected about {expected:.0f}")
    assert len(rows) == days, f"{len(rows)} days have a file, expected {days}"
    assert abs(total - expected) < 0.1 * expected, (
        "the total is far from the expected volume"
    )
    return total


if __name__ == "__main__":
    args = sys.argv[1:]
    check(
        *(args[:1] or ["scale_output"]), *(int(a) for a in (args[1:] or ["1000", "30"]))
    )
```

```bash
python data_at_scale_check.py scale_output 20 3
```

```text
2026-01-01  1 file        9 rows
2026-01-02  1 file       20 rows
2026-01-03  1 file       18 rows
total 47 rows, expected about 51
```

The check stops with an error when a day has no file, or when the total is more than 10 percent from the expected volume. After a sharded run, it counts the files of every shard in the folder, so the days show one file for each shard.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/data_at_scale.yaml"
        # The Data at Scale guide: a month of orders for 1,000 customers, written as one Parquet
        # file for each day. CUSTOMERS, DAYS and OUT change the size and the folder, FIRST names
        # the number of the first customer and SEED the random seed, so that separate runs can
        # each cover some of the customers. Run it with: desflow run data_at_scale.yaml
        simulation:
          sim_id: Shop
          factor: 0
          random_seed: ${SEED:-42}
          timezone: UTC
          logical_start_time: "2026-01-01T00:00:00+00:00"

        run: {until: "${DAYS:-30} days"}

        arrivals:
          # Each customer orders about once a day: a gap of 12 to 36 hours, drawn again after
          # each order
          customers:
            dist: uniform
            low: 43200
            high: 129600
            spawn: place_order
            count: ${CUSTOMERS:-1000}
            key: {gen: sequence, start: "${FIRST:-1}", format: "customer-{}"}

        tasks:
          place_order:
            payload:
              # Numbered within the run, so the first customer of the run is part of the id
              order_id: {gen: sequence, format: "${FIRST:-1}-{}"}
              customer: {gen: entity}
              amount: {gen: normal, mean: 40, std: 10, decimals: 2}
              # The file of the order: the day it was placed, in the simulated calendar
              __path__: {gen: format_time, format: "${OUT:-scale_output}/%Y-%m-%d.parquet"}

        egress:
          - type: Parquet
            # One file for each batch, so a large batch gives few, large files
            batch_size: 10000
            config:
              default_path: ${OUT:-scale_output}/other.parquet
        ```

    === "Builder"

        ```python title="docs/snippets/guides/data_at_scale.py"
        """The Data at Scale guide, built with the builder API: a month of orders for 1,000
        customers, written as Parquet files, one for each day.

        CUSTOMERS, DAYS, FIRST (the number of the first customer), SEED and OUT change the size,
        the customers, the random seed and the folder. Run it with `python data_at_scale.py`.
        """

        import logging
        import os
        from datetime import datetime, timezone

        from desflow import ParquetStorageEgress, SimulationContext
        from desflow.values import Template

        CUSTOMERS = int(os.getenv("CUSTOMERS", "1000"))
        DAYS = int(os.getenv("DAYS", "30"))
        FIRST = int(os.getenv("FIRST", "1"))
        OUT = os.getenv("OUT", "scale_output")


        def new_shop() -> SimulationContext:
            """A shop that runs without waiting and starts on 1 January 2026, UTC."""
            return SimulationContext(
                sim_id="Shop",
                factor=0,
                random_seed=int(os.getenv("SEED", "42")),
                timezone="UTC",
                logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )


        def add_customers(app: SimulationContext) -> None:
            """One arrival for each customer, ordering about once a day (12 to 36 hours apart).
            A blueprint's `count` is one arrival here, each with its own key."""
            for number in range(FIRST, FIRST + CUSTOMERS):
                app.add_arrival(
                    f"customer-{number}",
                    "uniform",
                    low=43200,
                    high=129600,
                    spawn="place_order",
                    key=lambda n, number=number: f"customer-{number}",
                )


        def add_files(app: SimulationContext) -> None:
            """A Parquet sink with a large batch, and an order that names the file of its day."""
            # One file for each batch, so a large batch gives few, large files
            app.add_egress(
                ParquetStorageEgress(default_path=f"{OUT}/other.parquet"), batch_size=10000
            )
            order = Template(
                {
                    # Numbered within the run, so the first customer of the run is part of the id
                    "order_id": {"gen": "sequence", "format": f"{FIRST}-{{}}"},
                    "customer": {"gen": "entity"},
                    "amount": {"gen": "normal", "mean": 40, "std": 10, "decimals": 2},
                    # The file of the order: the day it was placed, in the simulated calendar
                    "__path__": {"gen": "format_time", "format": f"{OUT}/%Y-%m-%d.parquet"},
                },
                name="tasks.place_order",
            )

            @app.task(name="place_order")
            def place_order(task_id):
                return order


        def build_shop() -> SimulationContext:
            app = new_shop()
            add_customers(app)
            add_files(app)
            return app


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            build_shop().run(until=f"{DAYS} days")
        ```

    === "Low-level"

        ```python title="docs/snippets/guides/data_at_scale_low_level.py"
        """The Data at Scale guide, low-level API: a month of orders for 1,000 customers, written
        as Parquet files, one for each day.

        CUSTOMERS, DAYS, FIRST (the number of the first customer), SEED and OUT change the size,
        the customers, the random seed and the folder. Run it with `python data_at_scale_low_level.py`.
        """

        import logging
        import os
        from datetime import datetime, timedelta, timezone

        from desflow import (
            DistributionConfig,
            DynamicRealtimeEnvironment,
            ParquetStorageEgress,
            Sampler,
        )

        CUSTOMERS = int(os.getenv("CUSTOMERS", "1000"))
        DAYS = int(os.getenv("DAYS", "30"))
        FIRST = int(os.getenv("FIRST", "1"))
        OUT = os.getenv("OUT", "scale_output")


        def new_env() -> DynamicRealtimeEnvironment:
            """An environment that runs without waiting and starts on 1 January 2026, UTC."""
            return DynamicRealtimeEnvironment(
                factor=0,
                logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                timezone="UTC",
            )


        def setup_files(env: DynamicRealtimeEnvironment) -> None:
            """A Parquet sink with a large batch. One file for each batch, so a large batch
            gives few, large files."""
            env.setup_egress(
                [ParquetStorageEgress(default_path=f"{OUT}/other.parquet")], batch_size=10000
            )


        def order_row(env: DynamicRealtimeEnvironment, key: str, number: int, amount: float):
            """An order that names the file of its day."""
            # The file of the order: the day it was placed, in the simulated calendar
            day = env.start_datetime + timedelta(seconds=env.now)
            return {
                # Numbered within the run, so the first customer of the run is part of the id
                "order_id": f"{FIRST}-{number}",
                "customer": key,
                "amount": amount,
                "__path__": day.strftime(f"{OUT}/%Y-%m-%d.parquet"),
            }


        def add_customers(env: DynamicRealtimeEnvironment) -> None:
            """One process for each customer, ordering about once a day (12 to 36 hours apart)."""
            sampler = Sampler(seed=int(os.getenv("SEED", "42")))
            gap = DistributionConfig(dist="uniform", low=43200, high=129600)
            amounts = sampler.generator("tasks.place_order.payload.amount")
            sent = {"orders": 0}

            def customer(key):
                while True:
                    yield env.timeout(sampler.sample(gap))
                    amount = round(float(amounts.normal(40, 10)), 2)
                    order = order_row(env, key, sent["orders"], amount)
                    env.publish_event(key, order, task="place_order", instance=sent["orders"])
                    sent["orders"] += 1

            for number in range(FIRST, FIRST + CUSTOMERS):
                env.process(customer(f"customer-{number}"))


        def shop() -> DynamicRealtimeEnvironment:
            env = new_env()
            setup_files(env)
            add_customers(env)
            return env


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            env = shop()
            try:
                env.run(until=DAYS * 24 * 3600)
            finally:
                env.teardown()
        ```

    ```python title="docs/snippets/guides/data_at_scale_check.py"
    """The check of the Data at Scale page: count the rows and the files of each day.

    Run it from the folder of the files with `python data_at_scale_check.py [folder]
    [customers] [days]`. It reads every Parquet file under the folder, prints the rows
    and the files of each day, and stops with an error when a day has no file or the
    total is far from the volume the model should give.
    """

    import sys
    from collections import Counter
    from pathlib import Path

    import pyarrow.parquet as pq


    def check(folder: str, customers: int, days: int) -> int:
        """Prints the files and rows of each day and returns the total rows."""
        rows: Counter = Counter()
        files: Counter = Counter()
        for path in sorted(Path(folder).glob("*.parquet")):
            day = path.name[:10]  # the day in the file name, such as 2026-01-01
            files[day] += 1
            rows[day] += pq.ParquetFile(path).metadata.num_rows
        for day in sorted(rows):
            print(f"{day}  {files[day]} file  {rows[day]:>7} rows")
        # Each customer orders once a day on average. Its first order comes 12 to 36 hours
        # in, so the total is about half a day short of customers * days.
        expected = customers * (days - 0.46)
        total = sum(rows.values())
        print(f"total {total} rows, expected about {expected:.0f}")
        assert len(rows) == days, f"{len(rows)} days have a file, expected {days}"
        assert abs(total - expected) < 0.1 * expected, (
            "the total is far from the expected volume"
        )
        return total


    if __name__ == "__main__":
        args = sys.argv[1:]
        check(
            *(args[:1] or ["scale_output"]), *(int(a) for a in (args[1:] or ["1000", "30"]))
        )
    ```

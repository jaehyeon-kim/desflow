# Parquet History, Kafka Live

One run writes ten minutes of backdated history to Parquet, then publishes to Kafka in real time. Each tab shows the example written one way, and the three publish the same records. The sections after the tabs explain why one run is better than two, how the settings fit together and what to get right.

=== "YAML"

    This example builds the simulation of the Builder tab from a YAML blueprint, with no Python. Until `go_live_at` the run is unpaced and writes backdated history to Parquet. From `go_live_at` it is paced in real time and publishes to Kafka.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[kafka,parquet]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get backfill-live --api yaml
        cd backfill-live/yaml

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it
        desflow run backfill_live.yaml

        # 5. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[kafka,parquet]" "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get backfill-live --api yaml
        cd backfill-live/yaml

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it
        desflow run backfill_live.yaml

        # 5. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    **What It Does**

    Ten minutes of history are written to `data/backfill/` within the first second, then the run publishes to `sim-events` and `sim-telemetry` in real time for 60 seconds and ends. The history folder is created on the first write, and `KafkaEgress` creates the two topics when it starts.

    **Full Source Code**

    `logical_start_time: -10m` and `go_live_at: now` are read against one moment, when the file is loaded, so they are exactly ten minutes apart, and `until: 11m` adds one minute of live tail. `when: history` sends the records stamped before `go_live_at` to Parquet, and `when: live` sends the rest to Kafka.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/backfill_live.yaml"
    # Backfill then go live in one run, in YAML.
    #
    # The YAML version of examples/builder/backfill_live_example.py. Until
    # go_live_at the clock is detached from the wall clock, so ten minutes of backdated
    # shop orders are written to Parquet as fast as the machine allows. From go_live_at
    # the run is paced at one simulated second per real second, and the same events go
    # to Kafka for one minute.
    #
    # Needs a broker: odctl up kafka-lite.
    # Run it with: desflow run examples/yaml/backfill_live.yaml

    simulation:
      sim_id: Shop
      # Unpaced to begin with, so the history costs no real time.
      factor: 0.0
      random_seed: 42
      logical_start_time: -10m
      # From here the same run is paced at one simulated second per real second.
      go_live_at: now

    egress:
      - type: Parquet
        config:
          default_path: data/backfill/events.parquet
        # Records stamped before go_live_at.
        when: history
      - type: Kafka
        config:
          event_topic: sim-events
          telemetry_topic: sim-telemetry
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
        # Records stamped at or after go_live_at.
        when: live

    # Only batch_size governs the history. The interval flush starts at go-live, so the
    # live tail also flushes every 10 seconds.
    batching:
      batch_size: 2000
      flush_interval: 10.0

    resources:
      packers: {current_cap: 4, max_cap: 10}

    services:
      packing: {dist: normal, mean: 2.0, std: 0.2}

    arrivals:
      orders: {dist: exponential, rate: 0.5, spawn: pack_order}

    tasks:
      pack_order:
        service: packing
        resource: packers
        payload: {path_id: Shop.service.packing, status: finished}

    # The packers' readings every 30 simulation seconds.
    stats: {level: minimal, interval: 30}

    run:
      # Ten minutes of history, then one minute of live tail.
      until: 11m
    ```

=== "Builder"

    [`examples/builder/backfill_live_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/backfill_live_example.py) backdates the clock by ten minutes, writes those ten minutes to Parquet in well under a second, then publishes to Kafka in real time for sixty seconds. `HISTORY_MINUTES` and `LIVE_SECONDS` set the two halves; a day of history (about 130,000 records) still generates in about two seconds.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[kafka,parquet]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get backfill-live --api builder
        cd backfill-live/builder

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it
        uv run --no-project --with "desflow[kafka,parquet]" backfill_live_example.py

        # 5. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[kafka,parquet]" "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get backfill-live --api builder
        cd backfill-live/builder

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it
        python backfill_live_example.py

        # 5. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    **Full Source Code**

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/builder/backfill_live_example.py"
    """
    Backfill-then-live Example.

    One run produces both halves of a tiered dataset. Until `go_live_at` the clock is
    detached from the wall clock, so ten minutes of backdated shop orders are written to
    Parquet as fast as the machine allows. From `go_live_at` the same run is paced at one
    simulated second per real second, and the same events are published to Kafka as they
    happen. Doing this in two processes would mean repeating the seed and the start
    instant in both, and keeping them in step by hand.
    """

    import logging
    import os
    import time
    from datetime import datetime, timedelta

    from desflow import (
        KafkaAdminConnector,
        KafkaEgress,
        ParquetStorageEgress,
        SimulationContext,
    )

    # Logging is configured here rather than in a wrapper, because this script is run
    # directly. Without it the run produces no output at all.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    logger = logging.getLogger(__name__)

    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    EVENT_TOPIC = "sim-events"
    TELEMETRY_TOPIC = "sim-telemetry"

    # How much history to generate, and how long to keep tailing once live. The live half
    # costs real time, second for second, so it is short by default.
    HISTORY = timedelta(minutes=float(os.getenv("HISTORY_MINUTES", "10")))
    LIVE_SECONDS = float(os.getenv("LIVE_SECONDS", "60"))

    base_path = os.getenv("DEST_PATH", "data/backfill")

    # The go-live instant is now, so everything before it is history and everything after
    # it is the live tail. These sit at module scope because the builder below needs them,
    # and the builder has to stay at module scope for the decorators to attach to it.
    GO_LIVE_AT = datetime.now()
    LOGICAL_START_TIME = GO_LIVE_AT - HISTORY

    # Records carry their logical time as an ISO string, so the predicates that split the
    # two sinks compare strings. That works because every timestamp comes from the same
    # formatter: identical layout, so ordering by text is ordering by time.
    GO_LIVE_ISO = GO_LIVE_AT.isoformat(timespec="milliseconds")


    def is_history(record: dict) -> bool:
        """True for records stamped before the go-live instant."""
        return record["timestamp"] < GO_LIVE_ISO


    def is_live(record: dict) -> bool:
        """True for records stamped at or after the go-live instant."""
        return record["timestamp"] >= GO_LIVE_ISO


    def history_router(data: dict) -> str | None:
        """Drops telemetry and flattens the event payload, as Parquet needs flat rows."""
        if data.get("stream_type") != "event":
            return None

        if isinstance(data.get("value"), dict):
            data.update(data.pop("value"))

        return f"{base_path}/events.parquet"


    # ==========================================
    # 1. Infrastructure Builder
    # ==========================================
    app = (
        SimulationContext(
            sim_id="Shop",
            # Unpaced to begin with, so the history costs no real time.
            factor=0.0,
            random_seed=42,
            logical_start_time=LOGICAL_START_TIME,
            # From here the same run is paced at one simulated second per real second.
            go_live_at=GO_LIVE_AT,
        )
        .add_egress(ParquetStorageEgress(path_router=history_router), when=is_history)
        .add_egress(
            KafkaEgress(
                event_topic=EVENT_TOPIC,
                telemetry_topic=TELEMETRY_TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
            ),
            when=is_live,
        )
        # Only batch_size governs the history. The interval flush is a simulation process,
        # started only when factor is non-zero as the egress is set up, and this run starts
        # at 0.0. It starts at go-live, so the live tail also flushes every 10 seconds.
        .with_batching(batch_size=2000, flush_interval=10.0)
        .add_resource("packers", current_cap=4, max_cap=10)
        .add_service("packing", dist="normal", mean=2.0, std=0.2)
        .add_arrival("orders", dist="exponential", rate=0.5)
        # The packers' readings every 30 simulation seconds
        .with_stats("minimal", interval=30.0)
    )


    # ==========================================
    # 2. Simulation Logic
    # ==========================================
    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, context):
        """Returns the flat payload that both sinks receive for a finished task."""
        return {"path_id": "Shop.service.packing", "status": "finished"}


    @app.arrival_loop("orders")
    def arrival_generator(context):
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, context, entity=entity))
            task_id += 1


    # ==========================================
    # 3. Execution
    # ==========================================
    def run():
        """Generates the history instantly, then tails live for LIVE_SECONDS."""
        os.makedirs(base_path, exist_ok=True)

        try:
            admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
            admin.create_topics(
                topics_config=[
                    {"name": EVENT_TOPIC, "partitions": 1},
                    {"name": TELEMETRY_TOPIC, "partitions": 1},
                ]
            )
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Could not explicitly create topics: {e}")

        logger.info(
            "Backfilling from %s to %s into '%s/', then tailing live to Kafka for %.0fs.",
            LOGICAL_START_TIME.strftime("%Y-%m-%d %H:%M:%S"),
            GO_LIVE_AT.strftime("%Y-%m-%d %H:%M:%S"),
            base_path,
            LIVE_SECONDS,
        )

        app.run(until=HISTORY.total_seconds() + LIVE_SECONDS)

        logger.info("Run complete. History is in '%s/', the tail is in Kafka.", base_path)


    if __name__ == "__main__":
        run()
    ```

=== "Low-level"

    [`examples/low-level/backfill_live_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/backfill_live_example.py) wires the same run by hand on `DynamicRealtimeEnvironment`: it backdates the clock by ten minutes, writes those ten minutes to Parquet in well under a second, then publishes to Kafka in real time for sixty seconds. `HISTORY_MINUTES` and `LIVE_SECONDS` set the two halves.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[kafka,parquet]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get backfill-live --api low-level
        cd backfill-live/low-level

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it
        uv run --no-project --with "desflow[kafka,parquet]" backfill_live_example.py

        # 5. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[kafka,parquet]" "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get backfill-live --api low-level
        cd backfill-live/low-level

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it
        python backfill_live_example.py

        # 5. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    **Full Source Code**

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/low-level/backfill_live_example.py"
    """
    Backfill-then-live Example, low-level API.

    The low-level twin of `builder/backfill_live_example.py`. One run produces both
    halves of a tiered dataset. Until `go_live_at` the clock is detached from the wall
    clock, so ten minutes of backdated shop orders are written to Parquet as fast as the
    machine allows. From `go_live_at` the same run is paced at one simulated second per
    real second, and the same events are published to Kafka as they happen.
    """

    import logging
    import os
    import time
    from datetime import datetime, timedelta

    import numpy as np

    from desflow import (
        CapacityConfig,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        KafkaAdminConnector,
        KafkaEgress,
        ParquetStorageEgress,
        Sampler,
        SimParameter,
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    EVENT_TOPIC = "sim-events"
    TELEMETRY_TOPIC = "sim-telemetry"

    HISTORY = timedelta(minutes=float(os.getenv("HISTORY_MINUTES", "10")))
    LIVE_SECONDS = float(os.getenv("LIVE_SECONDS", "60"))
    base_path = os.getenv("DEST_PATH", "data/backfill")

    GO_LIVE_AT = datetime.now()
    LOGICAL_START_TIME = GO_LIVE_AT - HISTORY
    GO_LIVE_ISO = GO_LIVE_AT.isoformat(timespec="milliseconds")

    PARAMS = SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
        service={"packing": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
        resources={"packers": CapacityConfig(current_cap=4, max_cap=10)},
    )


    def is_history(record: dict) -> bool:
        """True for records stamped before the go-live instant."""
        return record["timestamp"] < GO_LIVE_ISO


    def is_live(record: dict) -> bool:
        """True for records stamped at or after the go-live instant."""
        return record["timestamp"] >= GO_LIVE_ISO


    def history_router(data: dict) -> str | None:
        """Drops telemetry and flattens the event payload, as Parquet needs flat rows."""
        if data.get("stream_type") != "event":
            return None
        if isinstance(data.get("value"), dict):
            data.update(data.pop("value"))
        return f"{base_path}/events.parquet"


    def pack_order(env, sampler, packers, key, task_id):
        """What @app.task does: queued, started, the service time, then finished."""
        path_id = "Shop.service.packing"
        env.publish_event(
            key,
            {"path_id": path_id, "status": "queued"},
            task="pack_order",
            instance=task_id,
        )
        env.stats.task_queued("pack_order", task_id)  # counted as in progress
        with packers.request() as request:
            yield request
            env.publish_event(
                key,
                {"path_id": path_id, "status": "started"},
                task="pack_order",
                instance=task_id,
            )
            env.stats.task_started("pack_order", task_id)
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            env.publish_event(
                key,
                {"path_id": path_id, "status": "finished"},
                task="pack_order",
                instance=task_id,
            )
            env.stats.task_finished("pack_order", task_id)


    def arrival_generator(env, sampler, packers):
        gap = env.registry.get_config("Shop.arrival.orders")
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(gap))
            env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
            task_id += 1


    def run():
        """Generates the history instantly, then tails live for LIVE_SECONDS."""
        os.makedirs(base_path, exist_ok=True)
        try:
            admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
            admin.create_topics(
                topics_config=[
                    {"name": EVENT_TOPIC, "partitions": 1},
                    {"name": TELEMETRY_TOPIC, "partitions": 1},
                ]
            )
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Could not explicitly create topics: {e}")

        env = DynamicRealtimeEnvironment(
            factor=0.0,  # unpaced, so the history costs no real time
            logical_start_time=LOGICAL_START_TIME,
            go_live_at=GO_LIVE_AT,  # from here, one simulated second per real second
        )
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress(
            [
                ParquetStorageEgress(path_router=history_router),
                KafkaEgress(
                    event_topic=EVENT_TOPIC,
                    telemetry_topic=TELEMETRY_TOPIC,
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                ),
            ],
            batch_size=2000,
            flush_interval=10.0,
            predicates=[is_history, is_live],  # matched to the providers by position
        )
        # The packers' readings every 30 simulation seconds
        env.setup_stats("Shop", "minimal", interval=30.0)
        packers = DynamicResource(env, "Shop", "packers")
        sampler = Sampler(rng=np.random.default_rng(42))
        env.process(arrival_generator(env, sampler, packers))

        logger.info(
            "Backfilling from %s to %s into '%s/', then tailing live to Kafka for %.0fs.",
            LOGICAL_START_TIME.strftime("%Y-%m-%d %H:%M:%S"),
            GO_LIVE_AT.strftime("%Y-%m-%d %H:%M:%S"),
            base_path,
            LIVE_SECONDS,
        )
        try:
            env.run(until=HISTORY.total_seconds() + LIVE_SECONDS)
        finally:
            env.teardown()
        logger.info("Run complete. History is in '%s/', the tail is in Kafka.", base_path)


    if __name__ == "__main__":
        run()
    ```

---

## Why One Run Rather Than Two

A tiered demo usually needs two datasets: a backdated history sitting in a lake, and a live stream arriving now. They have to be the same shop, with the same packers, the same distributions and the same task ids, or the join between them is meaningless.

Producing them takes two settings that contradict each other. History wants `factor=0.0`, so a week of events is generated as fast as the machine allows. A live tail wants `factor=1.0`, so events arrive one simulated second per real second.

Without `go_live_at`, the only way to get both is to start two processes with the same `random_seed` and the same `logical_start_time`, one fast-forwarding into Parquet and one pacing into Kafka. That costs:

* A second full execution of the same simulation, so the history is generated twice.
* Seed and start instant duplicated in two places, which have to be edited together. Change one and the two halves silently stop being the same shop.
* No guarantee about the seam. Each process decides on its own where its half ends, so the join can overlap or leave a gap.

`go_live_at` replaces both processes with one. The run generates history unpaced up to that logical instant, then switches to real-time pacing and keeps going. There is one seed, one start instant and one seam.

---

## How the Three Settings Fit Together

Three separate settings make up the feature, and each answers a different question.

| Setting | Question it answers |
| --- | --- |
| `logical_start_time` | Where does the logical clock start? Set it in the past to backdate the history. |
| `go_live_at` | At which logical instant does pacing stop being `factor` and become real time? |
| `when` on `add_egress` | Which sink receives each record? |

They are independent. `go_live_at` changes pacing only; it never routes a record. `when` routes records only; it never changes pacing. Using them together is what produces a tiered dataset, and the two instants have to agree: pass the same instant to `go_live_at` and to the predicates, or the seam in the data will not match the seam in the pacing.

![From logical_start_time to go_live_at the run writes the backdated history to Parquet without waiting; from go_live_at to the end it streams records to Kafka in real time](../assets/diagrams/backfill-timeline.png)

In the Builder version, the three settings make the run:

```python title="examples/builder/backfill_live_example.py:builder"
app = (
    SimulationContext(
        sim_id="Shop",
        # Unpaced to begin with, so the history costs no real time.
        factor=0.0,
        random_seed=42,
        logical_start_time=LOGICAL_START_TIME,
        # From here the same run is paced at one simulated second per real second.
        go_live_at=GO_LIVE_AT,
    )
    .add_egress(ParquetStorageEgress(path_router=history_router), when=is_history)
    .add_egress(
        KafkaEgress(
            event_topic=EVENT_TOPIC,
            telemetry_topic=TELEMETRY_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
        ),
        when=is_live,
    )
    # Only batch_size governs the history. The interval flush is a simulation process,
    # started only when factor is non-zero as the egress is set up, and this run starts
    # at 0.0. It starts at go-live, so the live tail also flushes every 10 seconds.
    .with_batching(batch_size=2000, flush_interval=10.0)
    .add_resource("packers", current_cap=4, max_cap=10)
    .add_service("packing", dist="normal", mean=2.0, std=0.2)
    .add_arrival("orders", dist="exponential", rate=0.5)
    # The packers' readings every 30 simulation seconds
    .with_stats("minimal", interval=30.0)
)
```

[Time and Pacing](../user-guide/time-and-pacing.md#how-it-works) describes how the switch is applied, and the cases where `go_live_at` is before the start or after the end of the run.

---

## Two Details to Get Right

**Predicates compare strings, not datetimes.** A record carries its logical time as an ISO string, so `record["timestamp"] >= GO_LIVE_AT` raises a `TypeError`. Format the instant once with `isoformat(timespec="milliseconds")` and compare against that. Every timestamp is produced by the same formatter, so ordering by text is ordering by time.

```python title="examples/builder/backfill_live_example.py:clock"
GO_LIVE_AT = datetime.now()
LOGICAL_START_TIME = GO_LIVE_AT - HISTORY

# Records carry their logical time as an ISO string, so the predicates that split the
# two sinks compare strings. That works because every timestamp comes from the same
# formatter: identical layout, so ordering by text is ordering by time.
GO_LIVE_ISO = GO_LIVE_AT.isoformat(timespec="milliseconds")
```

**`flush_interval` has no effect before `go_live_at`, so `batch_size` decides the history.** The interval flush is a simulation process, and it is only started if `factor` is non-zero when the egress is set up. This run starts at `factor=0.0`, so until go-live records leave the buffer only when it fills to `batch_size`. That sets the number of Parquet part files. From go-live the interval flush starts, so the live tail also flushes every `flush_interval`, and that decides how often Kafka sees anything. Give each sink its own `batch_size` and `flush_interval` to separate those two jobs:

```python title="docs/snippets/pages/backfill_live.py:per-sink"
app.add_egress(parquet, when=is_history, batch_size=200_000)
app.add_egress(kafka, when=is_live, batch_size=500, flush_interval=1.0)
```

---

## What the Run Logs

The two halves look different enough that the second can be mistaken for a hang. History finishes within the same second the run starts, then nothing is logged at all until teardown, because from `go_live_at` the run waits on the wall clock exactly as a live twin does. `Logical clock reached go-live` is the line that confirms the switch.

---

## Low-level API

`go_live_at` behaves identically on `DynamicRealtimeEnvironment`, alongside `logical_start_time`:

```python title="docs/snippets/pages/backfill_live.py:low-level"
go_live_at = datetime.now()

env = DynamicRealtimeEnvironment(
    factor=0.0,
    logical_start_time=go_live_at - timedelta(days=7),
    go_live_at=go_live_at,
)
```

`env.factor` reports `0.0` until the logical clock reaches `go_live_at`, and `1.0` afterwards.

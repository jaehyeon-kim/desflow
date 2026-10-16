# Fast-Forward to Iceberg

A day of an online shop's orders, generated at `factor=0.0` and appended to an Apache Iceberg table through an Iceberg REST catalog. Each tab shows the example written one way: as a YAML blueprint, with the builder API, or with the low-level API. [Ways to write a simulation](../user-guide/ways-to-write.md) compares the three.

`ParquetStorageEgress` writes files and leaves registering them as a table to you. `IcebergStorageEgress` appends into an Iceberg table instead, so one run ends with a table another engine can query.

Each flush of the buffer is one Iceberg commit. Every commit writes a manifest, a manifest list and a new `metadata.json`, so query planning slows down as snapshots accumulate. That is why this example gives the provider a large `batch_size` of its own: a day of events lands in four snapshots.

=== "YAML"

    This example builds the simulation of the builder example in the Builder tab from a YAML blueprint, with no Python. A day of shop orders is generated at `factor: 0.0`, and each flush of the buffer becomes one Iceberg commit.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[iceberg]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get iceberg --api yaml
        cd iceberg/yaml

        # 3. Start its service
        odctl up catalog

        # 4. Run it
        desflow run iceberg.yaml

        # 5. Stop its service when finished
        odctl down catalog --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[iceberg]" "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get iceberg --api yaml
        cd iceberg/yaml

        # 3. Start its service
        odctl up catalog

        # 4. Run it
        desflow run iceberg.yaml

        # 5. Stop its service when finished
        odctl down catalog --volumes
        ```

    **What It Does**

    The run appends a day of lifecycle events to the `simulation.events` table and ends on its own. `batch_size: 200000` on the egress makes each flush one commit, so the day lands in a few snapshots. Without a router, each event is written to `default_table` as one flat row, and telemetry is left out.

    The catalog is contacted on the first write, not when the blueprint is loaded, so a blueprint with a wrong catalog address still loads and fails when the first batch is written.

    `ICEBERG_URI`, `ICEBERG_WAREHOUSE`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_REGION` override the catalog and credentials.

    **Full Source Code**

    `catalog` is a mapping of PyIceberg catalog properties, passed to PyIceberg's `load_catalog`. `schemas` names a type for each column, so `timestamp` is created as a timestamp rather than inferred as a string, and the ISO time each record carries is converted before the write.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/iceberg.yaml"
    # Lakehouse data generation in YAML.
    #
    # The YAML version of examples/builder/iceberg_example.py. A day of shop
    # orders is generated at factor 0, and each flush of the buffer becomes one Iceberg
    # commit. Without a router, each event is written to default_table as one flat row
    # and telemetry is left out. The catalog is contacted on the first write.
    #
    # Needs the odctl catalog profile: odctl up catalog. ICEBERG_URI, ICEBERG_WAREHOUSE,
    # S3_ENDPOINT, S3_ACCESS_KEY and S3_SECRET_KEY override the catalog and credentials.
    # Run it with: desflow run examples/yaml/iceberg.yaml

    simulation:
      sim_id: Shop
      factor: 0.0
      random_seed: 42
      # A day before now, so the records are stamped as history.
      logical_start_time: -1d

    egress:
      - type: Iceberg
        config:
          # PyIceberg catalog properties, passed to load_catalog.
          catalog:
            name: odctl
            type: rest
            uri: ${ICEBERG_URI:-http://localhost:8181}
            warehouse: ${ICEBERG_WAREHOUSE:-s3://warehouse/}
            s3.endpoint: ${S3_ENDPOINT:-http://localhost:8333}
            s3.access-key-id: ${S3_ACCESS_KEY:-user}
            s3.secret-access-key: ${S3_SECRET_KEY:-password}
            s3.region: ${S3_REGION:-us-east-1}
          default_table: simulation.events
          # Pinned rather than inferred, so timestamp is a timestamp, not a string.
          schemas:
            simulation.events:
              sim_ts: double
              timestamp: timestamp
              key: string
              path_id: string
              status: string
        # One commit per flush, sized so a day of events lands in a few snapshots.
        batch_size: 200000

    resources:
      packers: {current_cap: 4, max_cap: 10}

    services:
      packing: {dist: normal, mean: 2.0, std: 0.2}

    arrivals:
      orders: {dist: exponential, rate: 2.0, spawn: pack_order}

    tasks:
      pack_order:
        service: packing
        resource: packers
        payload: {path_id: Shop.service.packing, status: finished}

    run:
      until: 1 day
    ```

=== "Builder"

    This example demonstrates how to run a simulation in **fast-forward mode** using the **builder API (`SimulationContext`)** and commit the output straight into an Apache Iceberg table using the `IcebergStorageEgress` connector.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[iceberg]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get iceberg --api builder
        cd iceberg/builder

        # 3. Start its service
        odctl up catalog

        # 4. Run it
        uv run --no-project --with "desflow[iceberg]" iceberg_example.py

        # 5. Stop its service when finished
        odctl down catalog --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[iceberg]" "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get iceberg --api builder
        cd iceberg/builder

        # 3. Start its service
        odctl up catalog

        # 4. Run it
        python iceberg_example.py

        # 5. Stop its service when finished
        odctl down catalog --volumes
        ```

    `ICEBERG_URI`, `ICEBERG_WAREHOUSE`, `ICEBERG_NAMESPACE`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_REGION` override the catalog and credentials.

    Running it twice appends to the same table rather than replacing it, which is what an Iceberg table is for. Use `odctl down catalog --volumes` to start from nothing.

    **Full Source Code**

    This script simulates the shop over a 1-day period. It routes lifecycle events into one Iceberg table and pins the schema so the event time is a real timestamp rather than the string inference would give.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/builder/iceberg_example.py"
    """
    Lakehouse Data Generation Example.

    The Iceberg twin of `builder/parquet_example.py`. The same fast-forward engine,
    but each flush of the buffer becomes one Iceberg commit rather than one Parquet file,
    so the run ends with a table another engine can query instead of a folder someone
    still has to register.

    Every commit writes a manifest, a manifest list and a new `metadata.json`, so the
    buffer is sized to produce a handful of snapshots rather than hundreds. That is what
    `batch_size` on `add_egress` is for.

    Requires the odctl `catalog` profile: `odctl up catalog`.
    """

    import logging
    import os
    from datetime import datetime, timedelta

    import pyarrow as pa

    from desflow import IcebergStorageEgress, SimulationContext

    # Logging is configured here rather than in a wrapper, because this script is run
    # directly. Without it the run produces no output at all.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    logger = logging.getLogger(__name__)

    NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "simulation")
    EVENTS_TABLE = f"{NAMESPACE}.events"

    # Pinned rather than inferred. Inference reads the ISO timestamp the environment
    # writes as a string, which is not what a consumer of an event table expects.
    EVENTS_SCHEMA = pa.schema(
        [
            ("sim_ts", pa.float64()),
            ("timestamp", pa.timestamp("us")),
            ("key", pa.string()),
            ("path_id", pa.string()),
            ("status", pa.string()),
        ]
    )


    def create_table_router(events_table: str):
        """
        Router Factory: Generates a router returning `namespace.table`, and reshapes
        each event into the pinned schema.
        """

        def table_router(data: dict) -> str | None:
            if data.get("path_id") == "system.simulation.lag_seconds":
                return None

            if data.get("stream_type") != "event":
                return None

            # FLATTEN EVENT INTO THE PINNED COLUMNS
            nested_value = data.pop("value", None)
            if isinstance(nested_value, dict):
                data.update(nested_value)

            # A pinned timestamp column takes a datetime. PyArrow rejects the ISO string
            # the environment writes, so the conversion belongs here.
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])

            return events_table

        return table_router


    # ==========================================
    # 1. CATALOG CONFIGURATION
    # ==========================================
    # RestCatalog fetches the catalog configuration as it is constructed, so building
    # the client below contacts the catalog when this module is imported. Creating the
    # namespace waits longer: the connector does that on its first batch, inside the run.
    def build_catalog():
        """Connects to the Iceberg REST catalog from the odctl `catalog` profile."""
        from pyiceberg.catalog.rest import RestCatalog

        return RestCatalog(
            "odctl",
            **{
                "uri": os.getenv("ICEBERG_URI", "http://localhost:8181"),
                "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
                "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:8333"),
                "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "user"),
                "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "password"),
                "s3.region": os.getenv("S3_REGION", "us-east-1"),
            },
        )


    router = create_table_router(EVENTS_TABLE)

    # The day of backdating this example exists to demonstrate. It sits at module scope
    # because the builder below needs it, and the builder has to stay at module scope for
    # the decorators further down to attach to it.
    LOGICAL_START_TIME = datetime.now() - timedelta(days=1)

    # ==========================================
    # 2. Infrastructure Builder
    # ==========================================
    app = (
        SimulationContext(
            sim_id="Shop",
            factor=0.0,
            random_seed=42,
            logical_start_time=LOGICAL_START_TIME,
        )
        .add_egress(
            IcebergStorageEgress(
                catalog=build_catalog(),
                table_router=router,
                schemas={EVENTS_TABLE: EVENTS_SCHEMA},
            ),
            # One commit per flush. Sized so a day of events lands in a few snapshots.
            batch_size=200_000,
        )
        .add_resource("packers", current_cap=4, max_cap=10)
        .add_service("packing", dist="normal", mean=2.0, std=0.2)
        .add_arrival("orders", dist="exponential", rate=2.0)
    )


    # ==========================================
    # 3. Simulation Logic
    # ==========================================
    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, context):
        """
        Returns the exact flat dictionary expected by the table router
        to represent the 'finished' state of the lifecycle.
        """
        return {"path_id": "Shop.service.packing", "status": "finished"}


    @app.arrival_loop("orders")
    def arrival_generator(context):
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, context, entity=entity))
            task_id += 1


    # ==========================================
    # 4. Execution
    # ==========================================
    def run():
        """Generates 1 day of shop orders instantly, into an Iceberg table."""
        logger.info(
            "Generating historical data mimicking start from "
            f"{LOGICAL_START_TIME.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info("Fast-forwarding (factor=0.0)...")

        # The clock detaches and executes 1 day of operations instantaneously
        app.run(until="1 day")

        table = build_catalog().load_table(EVENTS_TABLE)
        logger.info(
            f"Wrote {table.scan().to_arrow().num_rows} rows to '{EVENTS_TABLE}' "
            f"in {len(table.metadata.snapshots)} snapshots at {table.location()}"
        )


    if __name__ == "__main__":
        run()
    ```

=== "Low-level"

    This example demonstrates how to run a simulation in **fast-forward mode** using the low-level **Low-level API (`DynamicRealtimeEnvironment`)** and commit the output straight into an Apache Iceberg table using the `IcebergStorageEgress` connector.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[iceberg]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get iceberg --api low-level
        cd iceberg/low-level

        # 3. Start its service
        odctl up catalog

        # 4. Run it
        uv run --no-project --with "desflow[iceberg]" iceberg_example.py

        # 5. Stop its service when finished
        odctl down catalog --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[iceberg]" "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get iceberg --api low-level
        cd iceberg/low-level

        # 3. Start its service
        odctl up catalog

        # 4. Run it
        python iceberg_example.py

        # 5. Stop its service when finished
        odctl down catalog --volumes
        ```

    `ICEBERG_URI`, `ICEBERG_WAREHOUSE`, `ICEBERG_NAMESPACE`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_REGION` override the catalog and credentials.

    Running it twice appends to the same table rather than replacing it, which is what an Iceberg table is for. Use `odctl down catalog --volumes` to start from nothing.

    **Full Source Code**

    This script simulates the shop over a 1-day period. It routes lifecycle events into one Iceberg table and pins the schema so the event time is a real timestamp rather than the string inference would give.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/low-level/iceberg_example.py"
    """Lakehouse data generation, low-level API.

    The low-level twin of `builder/iceberg_example.py`. It wires
    `DynamicRealtimeEnvironment`, the registry and the connectors by hand rather than
    through the builder, which shows what `SimulationContext` does for you.

    `factor=0.0` detaches the clock from real time, so a day of shop orders is generated
    as fast as the machine allows and committed into an Iceberg table. Each flush of the
    buffer is one commit, so `batch_sizes` is what keeps the snapshot count down.

    Requires the odctl `catalog` profile: `odctl up catalog`.
    """

    import logging
    import os
    from datetime import datetime, timedelta

    import numpy as np
    import pyarrow as pa

    from desflow import (
        CapacityConfig,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        IcebergStorageEgress,
        Sampler,
        SimParameter,
    )
    from desflow.utils import time_to_seconds

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    logger = logging.getLogger("iceberg_example")

    NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "simulation")
    EVENTS_TABLE = f"{NAMESPACE}.events"

    # Pinned rather than inferred. Inference reads the ISO timestamp the environment
    # writes as a string, which is not what a consumer of an event table expects.
    EVENTS_SCHEMA = pa.schema(
        [
            ("sim_ts", pa.float64()),
            ("timestamp", pa.timestamp("us")),
            ("key", pa.string()),
            ("path_id", pa.string()),
            ("status", pa.string()),
        ]
    )


    def build_catalog():
        """Connects to the Iceberg REST catalog from the odctl `catalog` profile."""
        from pyiceberg.catalog.rest import RestCatalog

        return RestCatalog(
            "odctl",
            **{
                "uri": os.getenv("ICEBERG_URI", "http://localhost:8181"),
                "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/"),
                "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:8333"),
                "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "user"),
                "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "password"),
                "s3.region": os.getenv("S3_REGION", "us-east-1"),
            },
        )


    def create_table_router(events_table: str):
        """
        Router Factory: Generates a router returning `namespace.table`, and reshapes
        each event into the pinned schema.
        """

        def table_router(data: dict) -> str | None:
            if data.get("path_id") == "system.simulation.lag_seconds":
                return None

            if data.get("stream_type") != "event":
                return None

            # FLATTEN EVENT INTO THE PINNED COLUMNS
            nested_value = data.pop("value", None)
            if isinstance(nested_value, dict):
                data.update(nested_value)

            # A pinned timestamp column takes a datetime. PyArrow rejects the ISO string
            # the environment writes, so the conversion belongs here.
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])

            return events_table

        return table_router


    # The shop starts with 4 packers and has room for 10.
    PARAMS = SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="exponential", rate=2.0)},
        service={"packing": DistributionConfig(dist="normal", mean=2.0, std=0.2)},
        resources={"packers": CapacityConfig(current_cap=4, max_cap=10)},
    )


    def pack_order(env, sampler, packers, key, task_id):
        """What @app.task does: queued, started, the service time, then finished."""
        path_id = "Shop.service.packing"
        env.publish_event(
            key,
            {"path_id": path_id, "status": "queued"},
            task="pack_order",
            instance=task_id,
        )
        with packers.request() as req:
            yield req
            env.publish_event(
                key,
                {"path_id": path_id, "status": "started"},
                task="pack_order",
                instance=task_id,
            )
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            env.publish_event(
                key,
                {"path_id": path_id, "status": "finished"},
                task="pack_order",
                instance=task_id,
            )


    def arrival_generator(env, sampler, packers):
        gap = env.registry.get_config("Shop.arrival.orders")
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(gap))
            env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
            task_id += 1


    def run():
        # ---------------------------------------------------------
        # 1. SIMULATION SETUP
        # ---------------------------------------------------------
        start_time = datetime.now() - timedelta(days=1)
        env = DynamicRealtimeEnvironment(factor=0.0, logical_start_time=start_time)
        env.registry.register_sim_parameter(PARAMS)

        egress = IcebergStorageEgress(
            catalog=build_catalog(),
            table_router=create_table_router(EVENTS_TABLE),
            schemas={EVENTS_TABLE: EVENTS_SCHEMA},
        )

        # One commit per flush, so a day of events lands in a handful of snapshots.
        env.setup_egress([egress], batch_sizes=[200_000], flush_interval=86400)

        packers = DynamicResource(env, "Shop", "packers")
        sampler = Sampler(rng=np.random.default_rng(42))

        env.process(arrival_generator(env, sampler, packers))

        run_duration_str = "1 day"
        run_duration_sec = time_to_seconds(run_duration_str)

        logger.info(
            f"Generating historical data from {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info("Fast-forwarding (factor=0.0)...")

        try:
            env.run(until=run_duration_sec)
        finally:
            env.teardown()

        table = build_catalog().load_table(EVENTS_TABLE)
        logger.info(
            f"Wrote {table.scan().to_arrow().num_rows} rows to '{EVENTS_TABLE}' "
            f"in {len(table.metadata.snapshots)} snapshots at {table.location()}"
        )


    if __name__ == "__main__":
        run()
    ```

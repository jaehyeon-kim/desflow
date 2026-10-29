# Kafka Digital Twin

This example models a shop packing orders. The simulation reads parameter updates from a Kafka topic and publishes events and telemetry to Kafka topics. Each tab shows the example written one way: as a YAML blueprint, with the builder API, or with the low-level API. [Ways to write a simulation](../../user-guide/writing-a-model/index.md) compares the three.

With `KafkaIngress` and `KafkaEgress` in place of the local connectors, the simulation runs as a separate service. Look for the lifecycle events on `sim-events` and the packers' readings on `sim-telemetry`, published every 2 simulation seconds. A JSON parameter update sent to `sim-config` changes the running simulation, and the readings show the change.

---

## Before You Start

The example needs Docker and the odctl profile `kafka-lite`, which each Quick Start below starts and stops. [Services for the Examples](index.md#services-for-the-examples) shows how to install odctl.

---

## Steps

### Run It

=== "YAML"

    This example builds the simulation of the builder example in the Builder tab from a YAML blueprint, with no Python. The connectors, the parameters, the task and the telemetry are all declared in the file.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[kafka]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get kafka --api yaml
        cd kafka/yaml

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it (Ctrl + C to stop)
        desflow run kafka.yaml

        # 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
        uv run --no-project --with "desflow[kafka]" --with nicegui ../kafka_dashboard.py

        # 6. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[kafka]" "odctl>=1.0,<2" nicegui

        # 2. Download the example
        desflow examples get kafka --api yaml
        cd kafka/yaml

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it (Ctrl + C to stop)
        desflow run kafka.yaml

        # 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
        python ../kafka_dashboard.py

        # 6. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    **What It Does**

    The run keeps going until you stop it. When it starts, `KafkaEgress` creates `sim-events` and `sim-telemetry` if they do not exist. The run then publishes task lifecycle events to `sim-events`, and the packers' four readings (`capacity`, `in_use`, `queue_length` and `utilization`) and the task's `in_progress` to `sim-telemetry`. It applies parameter updates sent to `sim-config`.

    The dashboard in the Quick Start works with this run unchanged, because it reads the same topics and the same four packer readings.

    The blueprint reads the broker address with `${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}`, so the environment variable `KAFKA_BOOTSTRAP_SERVERS` overrides it.

=== "Builder"

    This version builds the simulation with the builder API (`SimulationContext`).

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[kafka]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get kafka --api builder
        cd kafka/builder

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it (Ctrl + C to stop)
        uv run --no-project --with "desflow[kafka]" kafka_example.py

        # 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
        uv run --no-project --with "desflow[kafka]" --with nicegui ../kafka_dashboard.py

        # 6. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[kafka]" "odctl>=1.0,<2" nicegui

        # 2. Download the example
        desflow examples get kafka --api builder
        cd kafka/builder

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it (Ctrl + C to stop)
        python kafka_example.py

        # 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
        python ../kafka_dashboard.py

        # 6. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    The run keeps going until you stop it. It logs one line per order when a packer starts on it. It publishes task lifecycle events to `sim-events`, and the packers' four readings (`capacity`, `in_use`, `queue_length` and `utilization`) and the task's `in_progress` to `sim-telemetry`.

=== "Low-level"

    This version wires the simulation by hand on `DynamicRealtimeEnvironment`, the low-level API.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[kafka]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get kafka --api low-level
        cd kafka/low-level

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it (Ctrl + C to stop)
        uv run --no-project --with "desflow[kafka]" kafka_example.py

        # 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
        uv run --no-project --with "desflow[kafka]" --with nicegui ../kafka_dashboard.py

        # 6. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[kafka]" "odctl>=1.0,<2" nicegui

        # 2. Download the example
        desflow examples get kafka --api low-level
        cd kafka/low-level

        # 3. Start its service
        odctl up kafka-lite

        # 4. Run it (Ctrl + C to stop)
        python kafka_example.py

        # 5. In a second terminal, in the same folder, watch and steer it at http://localhost:8080
        python ../kafka_dashboard.py

        # 6. Stop its service when finished
        odctl down kafka-lite --volumes
        ```

    The run keeps going until you stop it. It logs one line per order when a packer starts on it. It publishes task lifecycle events to `sim-events`, and the packers' four readings (`capacity`, `in_use`, `queue_length` and `utilization`) and the task's `in_progress` to `sim-telemetry`.

### Change a Parameter Live

Step 5 of each Quick Start starts a dashboard at http://localhost:8080. It reads `sim-telemetry`, and its capacity control sends a new `current_cap` for the packers to `sim-config`. `KafkaIngress` applies the update while the run goes, and the packers' `capacity` reading on `sim-telemetry` shows the change. Any producer can send the same update: a JSON message such as `{"path": "Shop.resources.packers.current_cap", "value": 3}` on `sim-config`.

---

## Check the Result

The three tabs publish the same records, which [a test](https://github.com/jaehyeon-kim/desflow/blob/main/tests/docs/test_example_twins.py) checks by running each version unpaced. A real run sends the events to `sim-events` and the telemetry to `sim-telemetry`. `desflow run kafka.yaml --sample 5 --stdout` prints the first five events, and the telemetry published between them, as JSON lines instead, unpaced and with no broker:

<!-- records: kafka-sample -->
```text
{"stream_type":"event","sim_ts":1.276,"timestamp":"...","key":"orders-1","task":"pack_order","instance":0,"value":{"path_id":"Shop.service.packing","status":"queued"}}
{"stream_type":"event","sim_ts":1.276,"timestamp":"...","key":"orders-1","task":"pack_order","instance":0,"value":{"path_id":"Shop.service.packing","status":"started"}}
{"stream_type":"event","sim_ts":1.348,"timestamp":"...","key":"orders-2","task":"pack_order","instance":1,"value":{"path_id":"Shop.service.packing","status":"queued"}}
{"stream_type":"event","sim_ts":1.869,"timestamp":"...","key":"orders-3","task":"pack_order","instance":2,"value":{"path_id":"Shop.service.packing","status":"queued"}}
{"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":1}
{"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
{"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":2}
{"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":100.0}
{"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.tasks.pack_order.in_progress","value":3}
{"stream_type":"event","sim_ts":2.01,"timestamp":"...","key":"orders-4","task":"pack_order","instance":3,"value":{"path_id":"Shop.service.packing","status":"queued"}}
```

With one packer, `orders-1` starts at once and the next orders wait. At 2 simulation seconds the readings show 1 packer in use, 2 orders in the queue and 3 tasks in progress.

---

## Full Files

??? example "Full files"

    === "YAML"

        The blueprint wires both Kafka connectors and declares the packers, the packing service, the order arrival and the task. The finished event of each task is the fixed `payload`. `stats: minimal` publishes the packers' four built-in readings and the task's `in_progress` every 2 simulation seconds. `with_stats("minimal", interval=2.0)` does the same in the builder version, and `env.setup_stats` in the low-level version.

        Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

        ```yaml title="examples/yaml/kafka.yaml"
        # Kafka digital twin in YAML.
        #
        # The YAML version of kafka_example.py, which --api builder downloads: a shop packing
        # orders. KafkaIngress reads parameter updates from sim-config, and KafkaEgress
        # publishes lifecycle events to sim-events and packer metrics to sim-telemetry.
        # KafkaEgress creates its two topics when the run starts.
        #
        # Needs a broker: odctl up kafka-lite. Runs until interrupted with Ctrl + C.
        # KAFKA_BOOTSTRAP_SERVERS overrides the broker address.
        # Run it from its folder with: desflow run kafka.yaml

        simulation:
          sim_id: Shop
          factor: 1.0
          random_seed: 42

        ingress:
          - type: Kafka
            config:
              topic: sim-config
              bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}

        egress:
          - type: Kafka
            config:
              event_topic: sim-events
              telemetry_topic: sim-telemetry
              bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}

        resources:
          packers: {current_cap: 1, max_cap: 10}

        services:
          packing: {dist: normal, mean: 3.0, std: 0.5}

        arrivals:
          orders: {dist: exponential, rate: 1.0, spawn: pack_order}

        tasks:
          pack_order:
            service: packing
            resource: packers
            payload: {path_id: Shop.service.packing, status: finished}

        # The packers' readings every 2 seconds, which kafka_dashboard.py shows.
        stats: {level: minimal, interval: 2}
        ```

    === "Builder"

        This script connects the simulation to Kafka topics and uses Pydantic models to fix the fields and types of the events.

        Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

        ```python title="examples/builder/kafka_example.py"
        """Kafka Digital Twin, builder API.

        Models a shop packing orders with `SimulationContext` and connects the simulation
        to Kafka for input and output. `KafkaIngress` reads parameter updates from `sim-config`,
        so the running simulation can be steered without restarting it. `KafkaEgress`
        publishes lifecycle events to `sim-events` and metrics to `sim-telemetry`, with
        Pydantic models that fix the fields and types of the events.

        Needs a broker: `odctl up kafka-lite`. Runs until interrupted with Ctrl + C.
        """

        import logging
        import os
        import time

        from pydantic import BaseModel

        from desflow import (
            KafkaAdminConnector,
            KafkaEgress,
            KafkaIngress,
            SimulationContext,
        )

        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
        )
        logger = logging.getLogger("kafka_example")


        # ==========================================
        # 1. Event Payload Model
        # ==========================================
        class TaskEvent(BaseModel):
            """The fields and types of the finished event.

            Pydantic checks them when the model is built, before the event is sent to Kafka.
            """

            path_id: str
            status: str


        BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

        # ==========================================
        # 2. Infrastructure Builder
        # ==========================================
        app = (
            SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
            .add_ingress(KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS))
            .add_egress(
                KafkaEgress(
                    event_topic="sim-events",
                    telemetry_topic="sim-telemetry",
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                )
            )
            .add_resource("packers", current_cap=1, max_cap=10)
            .add_service("packing", dist="normal", mean=3.0, std=0.5)
            .add_arrival("orders", dist="exponential", rate=1.0)
            # The packers' readings every 2 seconds, which kafka_dashboard.py shows
            .with_stats("minimal", interval=2.0)
        )


        # ==========================================
        # 3. Simulation Logic
        # ==========================================
        @app.task(service_id="packing", resource_id="packers")
        def pack_order(task_id: int, context):
            """
            The @task decorator requests the resource and publishes the 'queued'
            and 'started' events. The function adds its own logic and returns the
            finished payload.
            """
            logger.info(f"Task {task_id} started at sim time: {context.env.now:.2f}s")

            # The payload of the finished event, from the Pydantic model
            return TaskEvent(path_id="Shop.service.packing", status="finished").model_dump(
                mode="json"
            )


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
            TOPICS_CONFIG = [
                {"name": "sim-config", "partitions": 1},
                {"name": "sim-telemetry", "partitions": 1},
                {"name": "sim-events", "partitions": 1},
            ]

            logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
            try:
                admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
                admin.create_topics(topics_config=TOPICS_CONFIG)
                time.sleep(2)
            except Exception as e:
                logger.warning(f"Could not explicitly create topics: {e}")

            print("Simulation started.")
            print("  - Listen to 'sim-telemetry' for system vitals.")
            print("  - Listen to 'sim-events' for task lifecycles.")
            print("  - Send to 'sim-config' to update parameters.")

            app.run()  # Starts the clock and orchestrates all connectors


        if __name__ == "__main__":
            run()
        ```

    === "Low-level"

        This script connects the simulation to Kafka topics and uses Pydantic models to fix the fields and types of the events.

        Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

        ```python title="examples/low-level/kafka_example.py"
        """Kafka Digital Twin, low-level API.

        The low-level version of `builder/kafka_example.py`, a shop packing orders, with
        the environment, registry and connectors wired by hand. Like the builder version, it
        creates its topics first with `KafkaAdminConnector`.

        Needs a broker: `odctl up kafka-lite`. Runs until interrupted with Ctrl + C.
        """

        import logging
        import os
        import time

        import numpy as np
        from pydantic import BaseModel

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            KafkaAdminConnector,
            KafkaEgress,
            KafkaIngress,
            Sampler,
            SimParameter,
        )

        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
        )
        logger = logging.getLogger("kafka_example")

        BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")


        # ==========================================
        # 1. Event Payload Model
        # ==========================================
        class TaskEvent(BaseModel):
            """The fields and types of each lifecycle event.

            env.publish_event() takes this Pydantic model in place of a dict, and turns it
            into a dict. KafkaEgress serializes that dict, to JSON or to Avro.
            """

            path_id: str
            status: str


        # ==========================================
        # 2. Initial System State
        # ==========================================
        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},  # 1 a second
            service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=10)},
        )


        # ==========================================
        # 3. Simulation Logic
        # ==========================================
        def pack_order(env, sampler, packers, key, task_id):
            """Publishes a Pydantic model instead of a dict for each lifecycle event."""
            path_id = "Shop.service.packing"
            env.publish_event(
                key,
                TaskEvent(path_id=path_id, status="queued"),
                task="pack_order",
                instance=task_id,
            )
            env.stats.task_queued("pack_order", task_id)  # counted as in progress

            with packers.request() as req:
                yield req

                logger.info(f"Task {task_id} started at sim time: {env.now:.2f}s")
                env.publish_event(
                    key,
                    TaskEvent(path_id=path_id, status="started"),
                    task="pack_order",
                    instance=task_id,
                )
                env.stats.task_started("pack_order", task_id)

                # Use the latest service config from the registry
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))

                env.publish_event(
                    key,
                    TaskEvent(path_id=path_id, status="finished"),
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


        # ==========================================
        # 4. Execution
        # ==========================================
        def run():
            TOPICS_CONFIG = [
                {"name": "sim-config", "partitions": 1},
                {"name": "sim-telemetry", "partitions": 1},
                {"name": "sim-events", "partitions": 1},
            ]

            logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
            admin_connector = KafkaAdminConnector(
                bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100
            )
            admin_connector.create_topics(topics_config=TOPICS_CONFIG)
            time.sleep(2)

            # A secured cluster needs more keyword arguments, such as security_protocol="SASL_SSL"
            ingress = KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS)

            # KafkaEgress uses JsonSerializer by default. To write Avro instead:
            # from desflow.connectors.egress.kafka import ConfluentAvroSerializer
            # avro_serializer = ConfluentAvroSerializer(
            #     registry_url="http://127.0.0.1:8081", schema_str=AVRO_SCHEMA
            # )
            # Then pass: topic_serializers={"sim-events": avro_serializer}
            # Generate AVRO_SCHEMA from AvroBaseModel rather than a plain Pydantic model;
            # see Use Avro and Pydantic. TaskEvent above has no .avro_schema().
            egress = KafkaEgress(
                telemetry_topic="sim-telemetry",
                event_topic="sim-events",
                bootstrap_servers=BOOTSTRAP_SERVERS,
            )

            env = DynamicRealtimeEnvironment(factor=1.0, strict=False)
            env.registry.register_sim_parameter(PARAMS)
            env.setup_ingress([ingress])
            env.setup_egress([egress])
            # The packers' readings every 2 seconds, which kafka_dashboard.py shows
            env.setup_stats("Shop", "minimal", interval=2.0)

            packers = DynamicResource(env, "Shop", "packers")
            sampler = Sampler(rng=np.random.default_rng(42))

            env.process(arrival_generator(env, sampler, packers))

            print("Simulation started.")
            print("  - Listen to 'sim-telemetry' for system vitals.")
            print("  - Listen to 'sim-events' for task lifecycles.")
            print("  - Send to 'sim-config' to update parameters.")

            try:
                env.run()
            except KeyboardInterrupt:
                logger.info("Simulation interrupted by user.")
            finally:
                env.teardown()


        if __name__ == "__main__":
            run()
        ```

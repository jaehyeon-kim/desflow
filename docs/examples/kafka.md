# Kafka Digital Twin

This example models a shop packing orders. The simulation reads parameter updates from a Kafka topic and publishes events and telemetry to Kafka topics. Each tab shows the example written one way: as a YAML blueprint, with the builder API, or with the low-level API. [Ways to write a simulation](../user-guide/ways-to-write.md) compares the three.

With `KafkaIngress` and `KafkaEgress` in place of the local connectors, the simulation runs as a separate service. It reads JSON parameter updates from a topic, and publishes telemetry and typed Pydantic events to other topics.

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

    The run keeps going until you stop it. When it starts, `KafkaEgress` creates `sim-events` and `sim-telemetry` if they do not exist. The run then publishes task lifecycle events to `sim-events` and packer metrics to `sim-telemetry`, and applies parameter updates sent to `sim-config`.

    The dashboard in the Quick Start works with this run unchanged, because it reads the same topics and the same four packer metrics.

    `KAFKA_BOOTSTRAP_SERVERS` overrides the broker address, because the blueprint reads it with `${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}`.

    **Full Source Code**

    The blueprint wires both Kafka connectors and declares the packers, the packing service, the order arrival and the task. The finished event of each task is the fixed `payload`, and `stats: minimal` publishes the packers' four built-in readings and the task's `in_progress` every 2 simulation seconds, as `with_stats("minimal", interval=2.0)` and `env.setup_stats` do in the builder and low-level versions.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/kafka.yaml"
    # Kafka digital twin in YAML.
    #
    # The YAML version of examples/builder/kafka_example.py, a shop packing
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
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

    egress:
      - type: Kafka
        config:
          event_topic: sim-events
          telemetry_topic: sim-telemetry
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

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

    This example demonstrates how to integrate `desflow` into a full event-driven architecture using the **builder API (`SimulationContext`)**.

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

    The run keeps going until you stop it. It logs one line per order as a packer starts on it, publishes task lifecycle events to `sim-events` and resource metrics to `sim-telemetry`.

    **Full Source Code**

    This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/builder/kafka_example.py"
    """Kafka Digital Twin, builder API.

    Models a shop packing orders with `SimulationContext` and connects the simulation
    to Kafka for input and output. `KafkaIngress` reads parameter updates from `sim-config`,
    so the running simulation can be steered without restarting it. `KafkaEgress`
    publishes lifecycle events to `sim-events` and metrics to `sim-telemetry`, with
    Pydantic models giving the events a declared shape.

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
    # 1. Define Strongly-Typed Event Payloads
    # ==========================================
    class TaskEvent(BaseModel):
        """
        Strongly typed event payload to guarantee schema consistency
        when shipping data over the wire to Kafka.
        """

        path_id: str
        status: str


    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

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
        The @task decorator automatically locks the resource and emits the
        'queued' and 'started' events. We just execute our custom logic
        and return the final payload.
        """
        logger.info(f"Task {task_id} started at sim time: {context._env.now:.2f}s")

        # Return the strongly-typed Pydantic model for the 'finished' state
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

    This example demonstrates how to integrate `desflow` into a full event-driven architecture using the low-level **Low-level API**.

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

    The run keeps going until you stop it. It logs one line per order as a packer starts on it, publishes task lifecycle events to `sim-events` and resource metrics to `sim-telemetry`.

    **Full Source Code**

    This script connects the simulation to Kafka topics and utilizes Pydantic models for structured event logging.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/low-level/kafka_example.py"
    """Kafka Digital Twin, low-level API.

    The low-level twin of `builder/kafka_example.py`, a shop packing orders, with
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

    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


    # ==========================================
    # 1. Define Strongly-Typed Event Payloads
    # ==========================================
    class TaskEvent(BaseModel):
        """
        Thanks to desflow's duck-typing, we can pass this Pydantic model
        directly into env.publish_event(). KafkaEgress converts it to a dict and
        serializes it, to JSON or to Avro.
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

        # (Optional: Pass **kwargs like `security_protocol="SASL_SSL"` for enterprise clusters)
        ingress = KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS)

        # By default, this uses JsonSerializer. To use Avro for enterprise environments:
        # from desflow.connectors.egress.kafka import ConfluentAvroSerializer
        # avro_serializer = ConfluentAvroSerializer(
        #     registry_url="http://127.0.0.1:8081", schema_str=AVRO_SCHEMA
        # )
        # Then pass: topic_serializers={"sim-events": avro_serializer}
        # Generate AVRO_SCHEMA from AvroBaseModel rather than a plain Pydantic model;
        # see the Avro and Pydantic guide. TaskEvent above has no .avro_schema().
        egress = KafkaEgress(
            telemetry_topic="sim-telemetry",
            event_topic="sim-events",
            bootstrap_servers=BOOTSTRAP_SERVERS,
        )

        env = DynamicRealtimeEnvironment(factor=1.0)
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

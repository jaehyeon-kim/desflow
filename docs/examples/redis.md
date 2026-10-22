# Redis

This example models a shop receiving orders. The simulation writes each order to a Redis Stream as it is placed and reads parameter updates from a Redis Pub/Sub channel. Each tab shows the example written one way: as a YAML blueprint, with the builder API, or with the low-level API. [Ways to write a simulation](../user-guide/ways-to-write.md) compares the three.

=== "YAML"

    This example builds the simulation of the builder example in the Builder tab from a YAML blueprint, with no Python. `RedisEgress` writes order records to the `order_events` stream, which each record's `__stream__` key names, and `RedisIngress` subscribes to the `simulation_params` channel.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[redis]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get redis --api yaml
        cd redis/yaml

        # 3. Start its service
        odctl up valkey

        # 4. Run it (Ctrl + C to stop)
        desflow run redis.yaml

        # 5. Stop its service when finished
        odctl down valkey --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[redis]" "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get redis --api yaml
        cd redis/yaml

        # 3. Start its service
        odctl up valkey

        # 4. Run it (Ctrl + C to stop)
        desflow run redis.yaml

        # 5. Stop its service when finished
        odctl down valkey --volumes
        ```

    **What It Does**

    The run writes order records to the `order_events` stream, and the lag telemetry to `events`, until you stop it. In a second terminal, raise the arrival rate while it runs:

    ```bash
    docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"path": "Shop.arrival.orders.rate", "value": 10.0}'
    ```

    `RedisIngress` does not log the message it receives, so the sign that the update landed is that `XLEN order_events` climbs about five times faster.

    **Full Source Code**

    `record_order` has no service or resource, so it publishes its fixed `payload` as soon as an arrival spawns it, with the task id added as `order_id`. A mapping payload is a constant, so every order here comes from the `web` channel; the builder and low-level versions publish the same records.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/redis.yaml"
    # Redis Streams output with live parameter updates, in YAML.
    #
    # The YAML version of examples/builder/redis_example.py: a simulation that records
    # each order as it is placed. Each order spawns record_order, a task with no service
    # or resource, which publishes its payload at once. RedisEgress writes it to the
    # order_events stream that the __stream__ key names, while RedisIngress subscribes to
    # the simulation_params channel.
    #
    # Needs Valkey: odctl up valkey. Runs until interrupted with Ctrl + C.
    # Run it from its folder with: desflow run redis.yaml

    simulation:
      sim_id: Shop
      factor: 1.0
      random_seed: 42

    ingress:
      - type: Redis
        config:
          # The odctl valkey profile disables the unauthenticated default user, so the
          # URL carries credentials.
          url: redis://user:password@localhost:6379/0
          channel_name: simulation_params

    egress:
      - type: Redis
        config:
          url: redis://user:password@localhost:6379/0
          stream_name: events

    arrivals:
      orders: {dist: exponential, rate: 2.0, spawn: record_order}

    tasks:
      record_order:
        # id_field adds the task id as order_id.
        payload: {__stream__: order_events, channel: web, status: placed}
        id_field: order_id
    ```

=== "Builder"

    This example demonstrates how to integrate `desflow` with Redis using the **builder API (`SimulationContext`)**.

    With `RedisIngress` and `RedisEgress`, the simulation reads parameter updates through Pub/Sub and writes events and telemetry to Redis Streams.

    **Streaming to Redis**

    `RedisEgress` writes each record to a Redis Stream with `XADD`. Its default stream is the one named in the constructor, `events` in this example, and a `__stream__` key in a record picks another.

    ```python title="examples/builder/redis_example.py:app"
    app = (
        SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
        .add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
        .add_egress(RedisEgress(REDIS_URL, stream_name="events"))
        .add_arrival("orders", dist="exponential", rate=2.0)
    )
    ```

    `record_order` has no service or resource, so its record is published as soon as the arrival loop spawns it, one per order:

    ```python title="examples/builder/redis_example.py:records"
    @app.task()
    def record_order(task_id: int, context):
        """A task with no service or resource: its payload is published at once."""
        return {
            "__stream__": "order_events",
            "order_id": task_id,
            "channel": "web",
            "status": "placed",
        }


    @app.arrival_loop("orders")
    def order_generator(context):
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(record_order(task_id, context, entity=entity))
            task_id += 1
    ```

    Each entry holds one field, `payload`, containing the JSON of the published record. `RedisEgress` reads `__stream__` from inside `value`, which is where both `context.publish` and `publish_event` put the dictionary you pass them, so order records go to `order_events` and everything else to the `events` stream named in the constructor. The key itself is removed before writing, so it does not appear in the stored payload.

    **Dynamic Parameter Updates (Ingress)**

    The `RedisIngress` in the builder above listens to the Pub/Sub channel `simulation_params`. While the simulation runs, publishing a JSON message with `path` and `value` to that channel changes the parameter, such as `{"path": "Shop.arrival.orders.rate", "value": 10.0}` to raise the arrival rate. The example logs a `valkey-cli` command that sends it.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[redis]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get redis --api builder
        cd redis/builder

        # 3. Start its service
        odctl up valkey

        # 4. Run it (Ctrl + C to stop)
        uv run --no-project --with "desflow[redis]" redis_example.py

        # 5. Stop its service when finished
        odctl down valkey --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[redis]" "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get redis --api builder
        cd redis/builder

        # 3. Start its service
        odctl up valkey

        # 4. Run it (Ctrl + C to stop)
        python redis_example.py

        # 5. Stop its service when finished
        odctl down valkey --volumes
        ```

    **In a second terminal, execute the dynamic parameter update:**
    ```bash
    # Connect to the Valkey container and publish the parameter update
    docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"path": "Shop.arrival.orders.rate", "value": 10.0}'
    ```
    `RedisIngress` does not log the message it receives, so the sign that the update landed is the throughput. The arrival rate goes from 2.0 to 10.0 per second, and `XLEN order_events` climbs about five times faster than before.

    ```bash
    # Clean up the infrastructure when finished
    odctl down valkey --volumes
    ```

    **Full Source Code**

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/builder/redis_example.py"
    """Redis Streams output with live parameter updates, builder API.

    The simulation records each order as it is placed. `RedisEgress` writes the
    records to the `order_events` Redis Stream, named by each record's `__stream__` key,
    while `RedisIngress` subscribes to the `simulation_params` channel, so publishing a
    message to that channel changes the arrival rate of a running simulation.

    Needs Valkey: `odctl up valkey`. Runs until interrupted with Ctrl + C.
    """

    import logging

    from desflow import RedisEgress, RedisIngress, SimulationContext

    logger = logging.getLogger(__name__)

    # The odctl `valkey` profile disables the unauthenticated default user, so the
    # URL carries credentials. A plain Redis without auth takes redis://localhost:6379/0.
    REDIS_URL = "redis://user:password@localhost:6379/0"

    app = (
        SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
        .add_ingress(RedisIngress(REDIS_URL, channel_name="simulation_params"))
        .add_egress(RedisEgress(REDIS_URL, stream_name="events"))
        .add_arrival("orders", dist="exponential", rate=2.0)
    )


    @app.task()
    def record_order(task_id: int, context):
        """A task with no service or resource: its payload is published at once."""
        return {
            "__stream__": "order_events",
            "order_id": task_id,
            "channel": "web",
            "status": "placed",
        }


    @app.arrival_loop("orders")
    def order_generator(context):
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(record_order(task_id, context, entity=entity))
            task_id += 1


    def run():
        logger.info("Starting Builder Redis Demo. Press Ctrl+C to stop...")
        logger.info(
            "Test Ingress by running: docker exec -it valkey valkey-cli --user user "
            "--pass password PUBLISH simulation_params "
            '\'{"path": "Shop.arrival.orders.rate", "value": 10.0}\''
        )
        try:
            app.run()
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user.")


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        run()
    ```

=== "Low-level"

    This example demonstrates how to integrate `desflow` with Redis using the **low-level API (`DynamicRealtimeEnvironment`)**.

    This is useful if you are migrating existing SimPy generators and prefer to handle `env.process()` and component registration manually rather than through the builder.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install "desflow[redis]" --with-executables-from "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get redis --api low-level
        cd redis/low-level

        # 3. Start its service
        odctl up valkey

        # 4. Run it (Ctrl + C to stop)
        uv run --no-project --with "desflow[redis]" redis_example.py

        # 5. Stop its service when finished
        odctl down valkey --volumes
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install "desflow[redis]" "odctl>=1.0,<2"

        # 2. Download the example
        desflow examples get redis --api low-level
        cd redis/low-level

        # 3. Start its service
        odctl up valkey

        # 4. Run it (Ctrl + C to stop)
        python redis_example.py

        # 5. Stop its service when finished
        odctl down valkey --volumes
        ```

    **In a second terminal, execute the dynamic parameter update:**
    ```bash
    # Connect to the Valkey container and publish the parameter update
    docker exec -it valkey valkey-cli --user user --pass password PUBLISH simulation_params '{"path": "Shop.arrival.orders.rate", "value": 10.0}'
    ```
    `RedisIngress` does not log the message it receives, so the sign that the update landed is the throughput. The arrival rate goes from 2.0 to 10.0 per second, and `XLEN order_events` climbs about five times faster than before.

    ```bash
    # Clean up the infrastructure when finished
    odctl down valkey --volumes
    ```

    **Full Source Code**

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/desflow/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/low-level/redis_example.py"
    """Redis Streams output with live parameter updates, low-level API.

    The low-level twin of `builder/redis_example.py`. The simulation records each
    order as it is placed. `RedisEgress` writes the records to the `order_events` stream,
    named by each record's `__stream__` key, and `RedisIngress` subscribes to the
    `simulation_params` channel.

    Needs Valkey: `odctl up valkey`. Runs until interrupted with Ctrl + C.
    """

    import logging

    import numpy as np

    from desflow import (
        DistributionConfig,
        DynamicRealtimeEnvironment,
        RedisEgress,
        RedisIngress,
        Sampler,
        SimParameter,
    )

    logger = logging.getLogger("redis_example")

    # The odctl `valkey` profile disables the unauthenticated default user, so the
    # URL carries credentials. A plain Redis without auth takes redis://localhost:6379/0.
    REDIS_URL = "redis://user:password@localhost:6379/0"

    PARAMS = SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="exponential", rate=2.0)},
    )


    def order_generator(env, sampler):
        """Publishes one record per order, as the builder's task with no service does."""
        gap = env.registry.get_config("Shop.arrival.orders")
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(gap))
            order_event = {
                "__stream__": "order_events",
                "order_id": task_id,
                "channel": "web",
                "status": "placed",
            }
            # The order key, and the task name and instance id the builder adds
            env.publish_event(
                f"orders-{task_id + 1}", order_event, task="record_order", instance=task_id
            )
            task_id += 1


    def run():
        env = DynamicRealtimeEnvironment(factor=1.0, strict=False)
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress([RedisEgress(REDIS_URL, stream_name="events")])
        env.setup_ingress([RedisIngress(REDIS_URL, channel_name="simulation_params")])

        sampler = Sampler(rng=np.random.default_rng(42))
        env.process(order_generator(env, sampler))

        logger.info("Starting Low-level Redis Demo. Press Ctrl+C to stop...")
        logger.info(
            "Test Ingress by running: docker exec -it valkey valkey-cli --user user "
            "--pass password PUBLISH simulation_params "
            '\'{"path": "Shop.arrival.orders.rate", "value": 10.0}\''
        )

        try:
            env.run()
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user.")
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        run()
    ```

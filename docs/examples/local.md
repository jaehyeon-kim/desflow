# Local Simulation

An online shop that packs orders and prints its events and telemetry to the terminal. It needs no container, so it is the place to start. Each tab shows the example written one way: with the builder API, with the low-level API, or as a YAML blueprint. [Ways to write a simulation](../architecture/overview.md) compares the three.

Local connectors need no Docker, Kafka or other external store, so this example is useful for testing and benchmarking.

=== "YAML"

    This example builds the same simulation as the builder example in the Builder tab, from a YAML blueprint and nothing else. No Python is written: the arrival loop, the task and the telemetry loop are all declared in the file.

    It writes to `ConsoleEgress` and needs no container, so it is the place to start.

    **Quick Start**

    `ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install dynamic-des

        # 2. Download the example
        ddes examples get local --api yaml
        cd local/yaml

        # 3. Run it
        ddes run local.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install dynamic-des

        # 2. Download the example
        ddes examples get local --api yaml
        cd local/yaml

        # 3. Run it
        ddes run local.yaml
        ```

    **What It Does**

    The run prints every record to the terminal and stops after 30 simulation seconds, which at `factor: 1.0` is 30 real seconds. Telemetry lines carry `[TEL]` and events carry `[EVT]`:

    ```text
    [TEL] {'sim_ts': 0.0, 'timestamp': '...', 'path_id': 'Shop.packers.capacity', 'value': 1}
    [EVT] {'sim_ts': 2.404, 'timestamp': '...', 'key': 'task-0', 'value': {'path_id': 'Shop.service.packing', 'status': 'queued'}}
    [EVT] {'sim_ts': 5.779, 'timestamp': '...', 'key': 'task-0', 'value': {'event_type': 'order_packed', 'shipping': 'standard', 'order_id': 0}}
    [TEL] {'sim_ts': 12.0, 'timestamp': '...', 'path_id': 'Shop.packers.capacity', 'value': 3}
    ```

    Each order produces a `queued`, a `started` and a finished event. The finished event is the task's `payload`, with the task id added as `order_id` because of `id_field`. The capacity telemetry shows the scenario: one packer, three from 10 seconds, then two from 20 seconds. Add `--until 10` to stop after 10 simulation seconds instead.

    **Full Source Code**

    The blueprint declares one resource, one service and one arrival. `spawn` names the task each arrival starts, the `telemetry` entry publishes four statistics of the packers every 2 simulation seconds, and the `scenario` changes the number of packers at 10 and 20 seconds.

    Files live in the [`examples/yaml/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples/yaml) of the repository, and the label on each block below is its path there.

    ```yaml title="examples/yaml/local.yaml"
    # Local simulation in YAML, with no Python and no containers.
    #
    # The twin of examples/builder/local_example.py. An online shop packs orders, and
    # Shop writes to ConsoleEgress, so events and telemetry are printed to the
    # terminal. A scenario adds packers at 10 seconds and removes one at 20 seconds, and
    # the run ends on its own after 30 simulation seconds.
    #
    # Run it with: ddes run examples/yaml/local.yaml

    simulation:
      sim_id: Shop
      factor: 1.0
      random_seed: 42

    egress:
      - type: Console

    resources:
      packers: {current_cap: 1, max_cap: 5}

    services:
      packing: {dist: normal, mean: 3.0, std: 0.5}

    arrivals:
      # Each order spawns one pack_order task.
      orders: {dist: exponential, rate: 1.0, spawn: pack_order}

    tasks:
      pack_order:
        service: packing
        resource: packers
        # The value of the task's finished event. id_field adds the task id as order_id.
        payload: {event_type: order_packed, shipping: standard}
        id_field: order_id

    telemetry:
      # Samples the packers every 2 simulation seconds.
      - interval: 2.0
        publish:
          packers.capacity: packers.capacity
          packers.in_use: packers.in_use
          packers.queue_length: packers.queue_length
          packers.utilization: packers.utilization

    scenario:
      # Three packers from 10 seconds, then two from 20 seconds.
      - {at: 10, path: Shop.resources.packers.current_cap, value: 3}
      - {at: 20, path: Shop.resources.packers.current_cap, value: 2}

    run:
      until: 30
    ```

=== "Builder"

    This example demonstrates how to build a dynamic simulation using the **builder API (`SimulationContext`)** and **Local Connectors**.

    This example adds `ConsoleEgress` and no ingress. `add_process` starts `capacity_changes`, which waits on the simulation clock and changes the number of packers at 10 and 20 seconds, as the YAML `scenario` does.

    **Quick Start**

    `ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install dynamic-des

        # 2. Download the example
        ddes examples get local --api builder
        cd local/builder

        # 3. Run it
        uv run --no-project --with dynamic-des local_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install dynamic-des

        # 2. Download the example
        ddes examples get local --api builder
        cd local/builder

        # 3. Run it
        python local_example.py
        ```

    **Full Source Code**

    This script builds the shop, runs it for 30 simulation seconds, and streams events and telemetry directly to your terminal.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/builder/local_example.py"
    """Local simulation, builder API, no containers.

    The smallest complete example. An online shop packs orders: `Shop` is built with
    `SimulationContext` and writes to `ConsoleEgress`, so events and telemetry are printed
    to the terminal and nothing external is involved. A scenario adds packers at 10
    seconds and removes one at 20 seconds, and the telemetry shows the effect.

    Start here. It needs no broker, no database and no object store, and it ends on its own
    after 30 simulation seconds.
    """

    import logging

    from dynamic_des import ConsoleEgress, SimulationContext

    # Logging is configured here rather than in a wrapper, because this script is run
    # directly. Without it the run produces no output at all.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    logger = logging.getLogger(__name__)

    # ==========================================
    # 1. Infrastructure Builder
    # ==========================================
    app = (
        SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
        .add_egress(ConsoleEgress())
        .add_resource("packers", current_cap=1, max_cap=5)
        .add_service("packing", dist="normal", mean=3.0, std=0.5)
        .add_arrival("orders", dist="exponential", rate=1.0)
    )


    # ==========================================
    # 2. Simulation Logic (Decorators)
    # ==========================================
    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, context):
        """Packs one order and returns the payload of its finished event."""
        return {"event_type": "order_packed", "shipping": "standard", "order_id": task_id}


    @app.arrival_loop("orders")
    def arrival_generator(context):
        """Spawns a pack_order task for every order the 'orders' distribution draws."""
        task_id = 0
        while True:
            yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, context))
            task_id += 1


    @app.telemetry_loop(interval=2.0)
    def telemetry_generator(context):
        """Samples the packers every 2 simulation seconds."""
        res = context.get_resource("packers")
        util = (res.in_use / res.capacity) * 100 if res.capacity > 0 else 0

        context.publish("packers.capacity", res.capacity)
        context.publish("packers.in_use", res.in_use)
        context.publish("packers.queue_length", len(res.queue.items))
        context.publish("packers.utilization", util)


    def capacity_changes(context):
        """Three packers from 10 seconds, then two from 20 seconds, on simulation time."""
        path = "Shop.resources.packers.current_cap"
        yield context.env.timeout(10)
        context.env.registry.update(path, 3)
        yield context.env.timeout(10)
        context.env.registry.update(path, 2)


    app.add_process(capacity_changes)


    # ==========================================
    # 3. Execution
    # ==========================================
    def run():
        """Starts the local simulation for a fixed duration."""
        logger.info("Starting Builder Local Example. Running for 30 seconds...")
        app.run(until=30)


    if __name__ == "__main__":
        run()
    ```

=== "Low-level"

    This example demonstrates how to build a dynamic simulation using the low-level **Low-level API** and **Local Connectors**.

    **Quick Start**

    `ddes examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install dynamic-des

        # 2. Download the example
        ddes examples get local --api low-level
        cd local/low-level

        # 3. Run it
        uv run --no-project --with dynamic-des local_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install dynamic-des

        # 2. Download the example
        ddes examples get local --api low-level
        cd local/low-level

        # 3. Run it
        python local_example.py
        ```

    **Full Source Code**

    This script builds the shop and runs it for 30 simulation seconds. The `capacity_changes` process changes the number of packers from 1 to 3 at 10 seconds, then down to 2 at 20 seconds. Events and telemetry stream directly to your terminal, driven by plain SimPy generators.

    Scripts live in the [`examples/` folder](https://github.com/jaehyeon-kim/dynamic-des/tree/main/examples) of the repository, and the label on the block below is this one's path there.

    ```python title="examples/low-level/local_example.py"
    """Local simulation, low-level API, no containers.

    The low-level twin of `builder/local_example.py`, built on `DynamicRealtimeEnvironment`
    directly. An online shop packs orders and writes to `ConsoleEgress`. A scenario
    process adds packers at 10 seconds and removes one at 20 seconds, and the telemetry
    shows the effect.

    Needs no containers. Ends on its own after 30 simulation seconds.
    """

    import logging

    import numpy as np

    from dynamic_des import (
        CapacityConfig,
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        Sampler,
        SimParameter,
    )

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    logger = logging.getLogger("local_example")

    # The shop starts with 1 packer and has room for 5.
    PARAMS = SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
        service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
        resources={"packers": CapacityConfig(current_cap=1, max_cap=5)},
    )


    def pack_order(env, sampler, packers, task_id):
        """Models the task lifecycle: queued, started, then finished with its payload."""
        key, path_id = f"task-{task_id}", "Shop.service.packing"
        env.publish_event(key, {"path_id": path_id, "status": "queued"})

        with packers.request() as req:
            yield req
            env.publish_event(key, {"path_id": path_id, "status": "started"})
            # Late binding: the service time is read when the work starts
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            env.publish_event(
                key,
                {"event_type": "order_packed", "shipping": "standard", "order_id": task_id},
            )


    def arrival_generator(env, sampler, packers):
        """Spawns a pack_order task for every order the arrival distribution draws."""
        # A reference: the config follows the registry when the rate changes
        gap = env.registry.get_config("Shop.arrival.orders")
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(gap))
            env.process(pack_order(env, sampler, packers, task_id))
            task_id += 1


    def telemetry_generator(env, packers):
        """Samples the packers every 2 simulation seconds."""
        while True:
            util = (packers.in_use / packers.capacity) * 100 if packers.capacity > 0 else 0
            env.publish_telemetry("Shop.packers.capacity", packers.capacity)
            env.publish_telemetry("Shop.packers.in_use", packers.in_use)
            env.publish_telemetry("Shop.packers.queue_length", len(packers.queue.items))
            env.publish_telemetry("Shop.packers.utilization", util)
            yield env.timeout(2.0)


    def capacity_changes(env):
        """Three packers from 10 seconds, then two from 20 seconds, on simulation time."""
        path = "Shop.resources.packers.current_cap"
        yield env.timeout(10)
        env.registry.update(path, 3)
        yield env.timeout(10)
        env.registry.update(path, 2)


    def run():
        env = DynamicRealtimeEnvironment(factor=1.0)
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress([ConsoleEgress()])

        packers = DynamicResource(env, "Shop", "packers")
        sampler = Sampler(rng=np.random.default_rng(42))

        env.process(arrival_generator(env, sampler, packers))
        env.process(telemetry_generator(env, packers))
        env.process(capacity_changes(env))

        logger.info("Simulation started. Watch the capacity change at 10 s and 20 s...")
        try:
            env.run(until=30)
        finally:
            env.teardown()


    if __name__ == "__main__":
        run()
    ```

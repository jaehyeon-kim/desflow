# Processes with Queues

Nine processes in which entities wait for a resource or move through steps: orders that wait for busy packers, a machine that slows as it wears, a capacity that drops while the queue grows, callers who hang up, urgent tickets served first, shoppers who pay or abandon, visitors who drop out at each sign-up step, bets that are settled, and audits that need a follow-up. Each entry is a complete file in three forms.

---

## Before You Start

The commands run from the root of a clone of the repository, where the files are in `examples/gallery/`:

```bash
git clone https://github.com/jaehyeon-kim/desflow.git
cd desflow
```

An entry that needs a service or an extra says so. [Services for the Examples](../index.md#services-for-the-examples) shows how to start the services. A file with a fixed `random_seed` and `factor: 0` prints the same records on every run, and each output block below holds the first events of the YAML file. The Builder and Low-level files publish the same records.

---

## Entries

### Orders wait in a queue when all packers are busy

A shop has 3 packers. Orders arrive about once a minute, and packing takes a normal time with a mean of 240 s and a standard deviation of 60 s. The run lasts 2 hours. When all three packers are busy an order waits, and the minimal statistics publish the queue length and the units in use.

=== "YAML"

    Run it with the sample option, which stops after the first events and prints them:

    ```bash
    desflow run examples/gallery/orders_wait_for_busy_packers.yaml --sample 5
    ```

    ```yaml title="examples/gallery/orders_wait_for_busy_packers.yaml"
    # Orders wait in a queue when all packers are busy.
    #
    # A shop has 3 packers. Orders arrive about once a minute, and packing takes a normal
    # time with a mean of 240 s and a standard deviation of 60 s. The run lasts 2 hours.
    # When all three packers are busy an order waits, and the minimal statistics publish
    # the queue length and the units in use.
    #
    # Needs: nothing
    # Run it with: desflow run examples/gallery/orders_wait_for_busy_packers.yaml

    simulation:
      sim_id: Shop
      factor: 0
      random_seed: 42
      logical_start_time: 2026-01-01T00:00:00

    egress:
      - {type: Console}

    stats: {level: minimal}

    resources:
      packers: {current_cap: 3}

    services:
      pack: {dist: normal, mean: 240, std: 60}

    arrivals:
      orders: {dist: exponential, rate: 0.015, spawn: order}

    tasks:
      order:
        resource: packers
        service: pack
        id_field: order_id
        payload: {event_type: packed}

    run: {until: 2 hours}
    ```

    <!-- records: gallery-orders_wait_for_busy_packers -->
    ```text
    {"stream_type":"telemetry","sim_ts":10.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":10.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":0}
    {"stream_type":"telemetry","sim_ts":10.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":10.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":0.0}
    {"stream_type":"telemetry","sim_ts":20.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":20.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":0}
    {"stream_type":"telemetry","sim_ts":20.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":20.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":0.0}
    {"stream_type":"event","sim_ts":20.943711043131454,"timestamp":"...","key":"orders-1","task":"order","instance":0,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"queued"}}
    {"stream_type":"event","sim_ts":20.943711043131454,"timestamp":"...","key":"orders-1","task":"order","instance":0,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"started"}}
    {"stream_type":"telemetry","sim_ts":30.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":30.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":30.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":30.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":30.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":40.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":40.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":40.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":40.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":40.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":50.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":50.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":50.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":50.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":50.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":60.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":60.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":60.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":60.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":60.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":70.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":70.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":70.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":70.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":70.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":80.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":80.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":80.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":80.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":80.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":90.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":90.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":90.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":90.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":90.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":100.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":100.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":100.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":100.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":100.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":110.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":110.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":110.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":110.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":110.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":120.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":120.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":120.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":120.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":120.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":130.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":130.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":130.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":130.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":130.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":140.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":140.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":140.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":140.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":140.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":150.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":150.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":150.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":150.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":150.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":160.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":160.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":160.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":160.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":160.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"event","sim_ts":164.4779633739713,"timestamp":"...","key":"orders-2","task":"order","instance":1,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"queued"}}
    {"stream_type":"event","sim_ts":164.4779633739713,"timestamp":"...","key":"orders-2","task":"order","instance":1,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"started"}}
    {"stream_type":"event","sim_ts":167.48162062711958,"timestamp":"...","key":"orders-3","task":"order","instance":2,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"queued"}}
    ```

=== "Builder"

    The Builder file builds the same model with the builder API and prints every record of its run:

    ```bash
    python examples/gallery/orders_wait_for_busy_packers_builder.py
    ```

    ```python title="examples/gallery/orders_wait_for_busy_packers_builder.py"
    """Orders wait in a queue when all packers are busy, builder API.

    The builder version of `orders_wait_for_busy_packers.yaml`: a shop with 3 packers,
    orders about once a minute, and a packing time that is normal with a mean of 240 s.

    Needs: nothing
    Run it with: python examples/gallery/orders_wait_for_busy_packers_builder.py
    """

    import logging
    from datetime import datetime, timezone

    from desflow import ConsoleEgress, SimulationContext

    UNTIL = 7200


    def build() -> SimulationContext:
        """Builds the shop."""
        app = SimulationContext(
            sim_id="Shop",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        app.add_egress(ConsoleEgress())
        app.with_stats(level="minimal")
        app.add_resource("packers", current_cap=3)
        app.add_service("pack", dist="normal", mean=240, std=60)
        app.add_arrival("orders", "exponential", rate=0.015, spawn="order")

        @app.task(name="order", resource_id="packers", service_id="pack")
        def order(task_id):
            """Packs one order."""
            return {"event_type": "packed", "order_id": task_id}

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build().run(until=UNTIL)
    ```

=== "Low-level"

    The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

    ```bash
    python examples/gallery/orders_wait_for_busy_packers_low_level.py
    ```

    The gaps and the service times come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.orders` and `service.pack`, the sequences the blueprint uses for its arrivals and services.

    ```python title="examples/gallery/orders_wait_for_busy_packers_low_level.py"
    """Orders wait in a queue when all packers are busy, low-level API.

    The low-level version of `orders_wait_for_busy_packers.yaml`: plain SimPy on a
    `DynamicRealtimeEnvironment`, which publishes each record with `publish_event` and each
    reading with `env.stats`. Each order is one process that queues for a packer, packs and
    publishes.

    Needs: nothing
    Run it with: python examples/gallery/orders_wait_for_busy_packers_low_level.py
    """

    import itertools
    import logging
    from datetime import datetime, timezone

    from desflow import (
        CapacityConfig,
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        Sampler,
        SimParameter,
    )

    UNTIL = 7200
    START = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
    PARAMS = SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="exponential", rate=0.015)},
        service={"pack": DistributionConfig(dist="normal", mean=240, std=60)},
        resources={"packers": CapacityConfig(current_cap=3)},
    )
    STATS = "minimal"


    def build(env: DynamicRealtimeEnvironment) -> None:
        """Starts the arrivals and the orders on `env`."""
        sampler = Sampler(seed=42)
        packers = DynamicResource(env, "Shop", "packers")
        gap = env.registry.get_config("Shop.arrival.orders")
        gap.stream = "arrival.orders"  # the random sequence the blueprint's draw uses
        service = env.registry.get_config("Shop.service.pack")
        service.stream = "service.pack"

        def order(key, task_id):
            path_id = "Shop.service.pack"
            env.stats.flow_started("orders")
            env.publish_event(
                key,
                {
                    "path_id": path_id,
                    "sim_id": "Shop",
                    "service": "pack",
                    "status": "queued",
                },
                task="order",
                instance=task_id,
            )
            env.stats.task_queued("order", task_id)
            with packers.request() as request:
                yield request
                env.publish_event(
                    key,
                    {
                        "path_id": path_id,
                        "sim_id": "Shop",
                        "service": "pack",
                        "status": "started",
                    },
                    task="order",
                    instance=task_id,
                )
                env.stats.task_started("order", task_id)
                yield env.timeout(sampler.sample(service))
                env.publish_event(
                    key,
                    {"event_type": "packed", "order_id": task_id},
                    task="order",
                    instance=task_id,
                )
                env.stats.task_finished("order", task_id)
            env.stats.flow_finished("orders")

        def source():
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(order(f"orders-{number}", number - 1))

        env.process(source())


    def run() -> None:
        """Runs the model unpaced into the console."""
        env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress([ConsoleEgress()])
        env.setup_stats("Shop", "minimal")
        build(env)
        try:
            env.run(until=UNTIL)
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        run()
    ```

### A machine slows down as it wears, then is repaired

A press stamps one blank at a time. Stamping takes a normal time with a mean of 10 s. Wear raises the mean to 12, 14 and 16 s at 1, 2 and 3 hours, and a repair at 4 hours sets it back to 10 s. The wear is written as steps of a scenario.

=== "YAML"

    Run it with the sample option, which stops after the first events and prints them:

    ```bash
    desflow run examples/gallery/machine_slows_as_it_wears.yaml --sample 5
    ```

    ```yaml title="examples/gallery/machine_slows_as_it_wears.yaml"
    # A machine slows down as it wears, then is repaired.
    #
    # A press stamps one blank at a time. Stamping takes a normal time with a mean of 10 s.
    # Wear raises the mean to 12, 14 and 16 s at 1, 2 and 3 hours, and a repair at 4 hours
    # sets it back to 10 s. The wear is written as steps of a scenario.
    #
    # Needs: nothing
    # Run it with: desflow run examples/gallery/machine_slows_as_it_wears.yaml

    simulation:
      sim_id: Press
      factor: 0
      random_seed: 42
      logical_start_time: 2026-01-01T00:00:00

    egress:
      - {type: Console}

    resources:
      station: {current_cap: 1}

    services:
      stamp: {dist: normal, mean: 10, std: 1}

    arrivals:
      blanks: {dist: exponential, rate: 0.05, spawn: blank}

    tasks:
      blank:
        resource: station
        service: stamp
        payload: {event_type: stamped}

    scenario:
      - {at: 1 hour, path: Press.service.stamp.mean, value: 12}
      - {at: 2 hours, path: Press.service.stamp.mean, value: 14}
      - {at: 3 hours, path: Press.service.stamp.mean, value: 16}
      - {at: 4 hours, path: Press.service.stamp.mean, value: 10}

    run: {until: 5 hours}
    ```

    <!-- records: gallery-machine_slows_as_it_wears -->
    ```text
    {"stream_type":"event","sim_ts":21.79090305473576,"timestamp":"...","key":"blanks-1","task":"blank","instance":0,"value":{"path_id":"Press.service.stamp","sim_id":"Press","service":"stamp","status":"queued"}}
    {"stream_type":"event","sim_ts":21.79090305473576,"timestamp":"...","key":"blanks-1","task":"blank","instance":0,"value":{"path_id":"Press.service.stamp","sim_id":"Press","service":"stamp","status":"started"}}
    {"stream_type":"event","sim_ts":31.148971588708257,"timestamp":"...","key":"blanks-1","task":"blank","instance":0,"value":{"event_type":"stamped"}}
    {"stream_type":"event","sim_ts":41.95644863411036,"timestamp":"...","key":"blanks-2","task":"blank","instance":1,"value":{"path_id":"Press.service.stamp","sim_id":"Press","service":"stamp","status":"queued"}}
    {"stream_type":"event","sim_ts":41.95644863411036,"timestamp":"...","key":"blanks-2","task":"blank","instance":1,"value":{"path_id":"Press.service.stamp","sim_id":"Press","service":"stamp","status":"started"}}
    ```

=== "Builder"

    The Builder file builds the same model with the builder API and prints every record of its run:

    ```bash
    python examples/gallery/machine_slows_as_it_wears_builder.py
    ```

    ```python title="examples/gallery/machine_slows_as_it_wears_builder.py"
    """A machine slows down as it wears, then is repaired, builder API.

    The builder version of `machine_slows_as_it_wears.yaml`: a press with one station,
    whose mean stamping time rises in steps at 1, 2 and 3 hours and returns to 10 s at 4.

    Needs: nothing
    Run it with: python examples/gallery/machine_slows_as_it_wears_builder.py
    """

    import logging
    from datetime import datetime, timezone

    from desflow import ConsoleEgress, SimulationContext

    UNTIL = 18000


    def build() -> SimulationContext:
        """Builds the press, with the wear and the repair as scenario steps."""
        app = SimulationContext(
            sim_id="Press",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        app.add_egress(ConsoleEgress())
        app.add_resource("station", current_cap=1)
        app.add_service("stamp", dist="normal", mean=10, std=1)
        app.add_arrival("blanks", "exponential", rate=0.05, spawn="blank")
        app.add_scenario(
            [
                {"at": "1 hour", "path": "Press.service.stamp.mean", "value": 12},
                {"at": "2 hours", "path": "Press.service.stamp.mean", "value": 14},
                {"at": "3 hours", "path": "Press.service.stamp.mean", "value": 16},
                {"at": "4 hours", "path": "Press.service.stamp.mean", "value": 10},
            ]
        )

        @app.task(name="blank", resource_id="station", service_id="stamp")
        def blank(task_id):
            """Stamps one blank."""
            return {"event_type": "stamped"}

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build().run(until=UNTIL)
    ```

=== "Low-level"

    The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

    ```bash
    python examples/gallery/machine_slows_as_it_wears_low_level.py
    ```

    The gaps and the service times come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.blanks` and `service.stamp`, the sequences the blueprint uses for its arrivals and services.

    ```python title="examples/gallery/machine_slows_as_it_wears_low_level.py"
    """A machine slows down as it wears, then is repaired, low-level API.

    The low-level version of `machine_slows_as_it_wears.yaml`: plain SimPy on a
    `DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. The wear
    is a process that sets the mean at the times of the steps.

    Needs: nothing
    Run it with: python examples/gallery/machine_slows_as_it_wears_low_level.py
    """

    import itertools
    import logging
    from datetime import datetime, timezone

    from desflow import (
        CapacityConfig,
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        Sampler,
        SimParameter,
        run_scenario,
    )

    UNTIL = 18000
    START = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
    PARAMS = SimParameter(
        sim_id="Press",
        arrival={"blanks": DistributionConfig(dist="exponential", rate=0.05)},
        service={"stamp": DistributionConfig(dist="normal", mean=10, std=1)},
        resources={"station": CapacityConfig(current_cap=1)},
    )
    SCHEDULE = [
        {"at": 3600, "path": "Press.service.stamp.mean", "value": 12},
        {"at": 7200, "path": "Press.service.stamp.mean", "value": 14},
        {"at": 10800, "path": "Press.service.stamp.mean", "value": 16},
        {"at": 14400, "path": "Press.service.stamp.mean", "value": 10},
    ]


    def build(env: DynamicRealtimeEnvironment) -> None:
        """Starts the arrivals and the scenario on `env`."""
        sampler = Sampler(seed=42)
        station = DynamicResource(env, "Press", "station")
        gap = env.registry.get_config("Press.arrival.blanks")
        gap.stream = "arrival.blanks"  # the random sequence the blueprint's draw uses
        service = env.registry.get_config("Press.service.stamp")
        service.stream = "service.stamp"

        def blank(key, task_id):
            path_id = "Press.service.stamp"
            env.publish_event(
                key,
                {
                    "path_id": path_id,
                    "sim_id": "Press",
                    "service": "stamp",
                    "status": "queued",
                },
                task="blank",
                instance=task_id,
            )
            with station.request() as request:
                yield request
                env.publish_event(
                    key,
                    {
                        "path_id": path_id,
                        "sim_id": "Press",
                        "service": "stamp",
                        "status": "started",
                    },
                    task="blank",
                    instance=task_id,
                )
                yield env.timeout(sampler.sample(service))
                env.publish_event(
                    key,
                    {"event_type": "stamped"},
                    task="blank",
                    instance=task_id,
                )

        def source():
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(blank(f"blanks-{number}", number - 1))

        env.process(source())
        env.process(run_scenario(env, SCHEDULE, until=UNTIL))


    def run() -> None:
        """Runs the model unpaced into the console."""
        env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress([ConsoleEgress()])
        build(env)
        try:
            env.run(until=UNTIL)
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        run()
    ```

### Capacity drops to one at 20 seconds, and the queue grows

A shop has 3 packers. An order arrives about every second, and packing takes a normal time with a mean of 2.5 s. A scenario step at 20 s sets the capacity to 1, so the orders queue. The statistics publish the capacity and the queue length every second.

=== "YAML"

    Run it with the sample option, which stops after the first events and prints them:

    ```bash
    desflow run examples/gallery/capacity_drops_and_queue_grows.yaml --sample 5
    ```

    ```yaml title="examples/gallery/capacity_drops_and_queue_grows.yaml"
    # Capacity drops to one at 20 seconds, and the queue grows.
    #
    # A shop has 3 packers. An order arrives about every second, and packing takes a
    # normal time with a mean of 2.5 s. A scenario step at 20 s sets the capacity to 1, so
    # the orders queue. The statistics publish the capacity and the queue length every
    # second.
    #
    # Needs: nothing
    # Run it with: desflow run examples/gallery/capacity_drops_and_queue_grows.yaml

    simulation:
      sim_id: Shop
      factor: 0
      random_seed: 42
      logical_start_time: 2026-01-01T00:00:00

    egress:
      - {type: Console}

    stats: {level: minimal, interval: 1}

    resources:
      packers: {current_cap: 3}

    services:
      pack: {dist: normal, mean: 2.5, std: 0.3}

    arrivals:
      orders: {dist: exponential, rate: 1, spawn: order}

    tasks:
      order:
        resource: packers
        service: pack
        payload: {event_type: packed}

    scenario:
      - {at: 20, path: Shop.resources.packers.current_cap, value: 1}

    run: {until: 60}
    ```

    <!-- records: gallery-capacity_drops_and_queue_grows -->
    ```text
    {"stream_type":"event","sim_ts":0.3141556656469718,"timestamp":"...","key":"orders-1","task":"order","instance":0,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"queued"}}
    {"stream_type":"event","sim_ts":0.3141556656469718,"timestamp":"...","key":"orders-1","task":"order","instance":0,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"started"}}
    {"stream_type":"telemetry","sim_ts":1.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":1.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":1.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":1.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":1.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.capacity","value":3}
    {"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.in_use","value":1}
    {"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
    {"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.utilization","value":33.33333333333333}
    {"stream_type":"telemetry","sim_ts":2.0,"timestamp":"...","path_id":"Shop.stats.tasks.order.in_progress","value":1}
    {"stream_type":"event","sim_ts":2.4671694506095694,"timestamp":"...","key":"orders-2","task":"order","instance":1,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"queued"}}
    {"stream_type":"event","sim_ts":2.4671694506095694,"timestamp":"...","key":"orders-2","task":"order","instance":1,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"started"}}
    {"stream_type":"event","sim_ts":2.5122243094067938,"timestamp":"...","key":"orders-3","task":"order","instance":2,"value":{"path_id":"Shop.service.pack","sim_id":"Shop","service":"pack","status":"queued"}}
    ```

=== "Builder"

    The Builder file builds the same model with the builder API and prints every record of its run:

    ```bash
    python examples/gallery/capacity_drops_and_queue_grows_builder.py
    ```

    ```python title="examples/gallery/capacity_drops_and_queue_grows_builder.py"
    """Capacity drops to one at 20 seconds, and the queue grows, builder API.

    The builder version of `capacity_drops_and_queue_grows.yaml`: 3 packers, an order a
    second, and a scenario step at 20 s that sets the capacity to 1.

    Needs: nothing
    Run it with: python examples/gallery/capacity_drops_and_queue_grows_builder.py
    """

    import logging
    from datetime import datetime, timezone

    from desflow import ConsoleEgress, SimulationContext

    UNTIL = 60


    def build() -> SimulationContext:
        """Builds the shop."""
        app = SimulationContext(
            sim_id="Shop",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        app.add_egress(ConsoleEgress())
        app.with_stats(level="minimal", interval=1)
        app.add_resource("packers", current_cap=3)
        app.add_service("pack", dist="normal", mean=2.5, std=0.3)
        app.add_arrival("orders", "exponential", rate=1, spawn="order")
        app.add_scenario(
            [{"at": 20, "path": "Shop.resources.packers.current_cap", "value": 1}]
        )

        @app.task(name="order", resource_id="packers", service_id="pack")
        def order(task_id):
            """Packs one order."""
            return {"event_type": "packed"}

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build().run(until=UNTIL)
    ```

=== "Low-level"

    The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

    ```bash
    python examples/gallery/capacity_drops_and_queue_grows_low_level.py
    ```

    The gaps and the service times come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.orders` and `service.pack`, the sequences the blueprint uses for its arrivals and services.

    ```python title="examples/gallery/capacity_drops_and_queue_grows_low_level.py"
    """Capacity drops to one at 20 seconds, and the queue grows, low-level API.

    The low-level version of `capacity_drops_and_queue_grows.yaml`: plain SimPy on a
    `DynamicRealtimeEnvironment`, which publishes each record with `publish_event` and each
    reading with `env.stats`. A process sets the capacity to 1 at 20 s.

    Needs: nothing
    Run it with: python examples/gallery/capacity_drops_and_queue_grows_low_level.py
    """

    import itertools
    import logging
    from datetime import datetime, timezone

    from desflow import (
        CapacityConfig,
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        Sampler,
        SimParameter,
        run_scenario,
    )

    UNTIL = 60
    START = datetime(2026, 1, 1, tzinfo=timezone.utc)
    ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
    PARAMS = SimParameter(
        sim_id="Shop",
        arrival={"orders": DistributionConfig(dist="exponential", rate=1)},
        service={"pack": DistributionConfig(dist="normal", mean=2.5, std=0.3)},
        resources={"packers": CapacityConfig(current_cap=3)},
    )
    STATS = ("minimal", 1)  # the level and the interval of the readings
    SCHEDULE = [{"at": 20, "path": "Shop.resources.packers.current_cap", "value": 1}]


    def build(env: DynamicRealtimeEnvironment) -> None:
        """Starts the arrivals and the scenario on `env`."""
        sampler = Sampler(seed=42)
        packers = DynamicResource(env, "Shop", "packers")
        gap = env.registry.get_config("Shop.arrival.orders")
        gap.stream = "arrival.orders"  # the random sequence the blueprint's draw uses
        service = env.registry.get_config("Shop.service.pack")
        service.stream = "service.pack"

        def order(key, task_id):
            path_id = "Shop.service.pack"
            env.stats.flow_started("orders")
            env.publish_event(
                key,
                {
                    "path_id": path_id,
                    "sim_id": "Shop",
                    "service": "pack",
                    "status": "queued",
                },
                task="order",
                instance=task_id,
            )
            env.stats.task_queued("order", task_id)
            with packers.request() as request:
                yield request
                env.publish_event(
                    key,
                    {
                        "path_id": path_id,
                        "sim_id": "Shop",
                        "service": "pack",
                        "status": "started",
                    },
                    task="order",
                    instance=task_id,
                )
                env.stats.task_started("order", task_id)
                yield env.timeout(sampler.sample(service))
                env.publish_event(
                    key,
                    {"event_type": "packed"},
                    task="order",
                    instance=task_id,
                )
                env.stats.task_finished("order", task_id)
            env.stats.flow_finished("orders")

        def source():
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(order(f"orders-{number}", number - 1))

        env.process(source())
        env.process(run_scenario(env, SCHEDULE, until=UNTIL))


    def run() -> None:
        """Runs the model unpaced into the console."""
        env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress([ConsoleEgress()])
        env.setup_stats("Shop", "minimal", 1)
        build(env)
        try:
            env.run(until=UNTIL)
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        run()
    ```

### Callers hang up when they wait for an agent for longer than they will tolerate

Calls arrive at 54 an hour and two agents answer about 30 an hour, so a queue builds. A caller who waits longer than their patience, 1 to 5 minutes, hangs up. A scenario step adds two agents after half an hour, so calls wait less and fewer callers hang up.

=== "YAML"

    Run it with the sample option, which stops after the first events and prints them:

    ```bash
    desflow run examples/gallery/callers_hang_up_after_waiting_too_long.yaml --sample 5
    ```

    ```yaml title="examples/gallery/callers_hang_up_after_waiting_too_long.yaml"
    # Callers hang up when they wait for an agent for longer than they will tolerate.
    #
    # Calls arrive at 54 an hour and two agents answer about 30 an hour, so a queue
    # builds. A caller who waits longer than their patience, 1 to 5 minutes, hangs up. A
    # scenario step adds two agents after half an hour, so calls wait less and fewer
    # callers hang up.
    #
    # Needs: nothing
    # Run it with: desflow run examples/gallery/callers_hang_up_after_waiting_too_long.yaml

    simulation:
      sim_id: Centre
      factor: 0
      random_seed: 42
      logical_start_time: 2026-01-01T00:00:00

    egress:
      - {type: Console}

    resources:
      agents: {current_cap: 2, max_cap: 4}

    services:
      talk: {dist: normal, mean: 4 min, std: 1 min}

    arrivals:
      calls: {dist: exponential, rate: 0.015, spawn: answer_call}

    tasks:
      answer_call:
        service: talk
        resource: agents
        patience: {gen: uniform, low: 60, high: 300}
        on_renege: hang_up
        id_field: call_id
        payload: {event_type: answered}
      hang_up:
        payload: {event_type: hung_up}

    scenario:
      - {at: 30 min, path: Centre.resources.agents.current_cap, value: 4}

    run: {until: 1 hour}
    ```

    <!-- records: gallery-callers_hang_up_after_waiting_too_long -->
    ```text
    {"stream_type":"event","sim_ts":15.924314308737758,"timestamp":"...","key":"calls-1","task":"answer_call","instance":0,"value":{"path_id":"Centre.service.talk","sim_id":"Centre","service":"talk","status":"queued"}}
    {"stream_type":"event","sim_ts":15.924314308737758,"timestamp":"...","key":"calls-1","task":"answer_call","instance":0,"value":{"path_id":"Centre.service.talk","sim_id":"Centre","service":"talk","status":"started"}}
    {"stream_type":"event","sim_ts":250.72846807640823,"timestamp":"...","key":"calls-2","task":"answer_call","instance":1,"value":{"path_id":"Centre.service.talk","sim_id":"Centre","service":"talk","status":"queued"}}
    {"stream_type":"event","sim_ts":250.72846807640823,"timestamp":"...","key":"calls-2","task":"answer_call","instance":1,"value":{"path_id":"Centre.service.talk","sim_id":"Centre","service":"talk","status":"started"}}
    {"stream_type":"event","sim_ts":312.58292688735503,"timestamp":"...","key":"calls-3","task":"answer_call","instance":2,"value":{"path_id":"Centre.service.talk","sim_id":"Centre","service":"talk","status":"queued"}}
    ```

=== "Builder"

    The Builder file builds the same model with the builder API and prints every record of its run:

    ```bash
    python examples/gallery/callers_hang_up_after_waiting_too_long_builder.py
    ```

    ```python title="examples/gallery/callers_hang_up_after_waiting_too_long_builder.py"
    """Callers hang up when they wait for an agent for longer than they will tolerate.

    The Builder form of callers_hang_up_after_waiting_too_long.yaml, with the same records.
    Calls arrive at 54 an hour and two agents answer about 30 an hour, so a queue builds.
    A caller who waits longer than their patience, 1 to 5 minutes, hangs up. A scenario step
    adds two agents after half an hour, so calls wait less and fewer callers hang up.

    Needs: nothing
    Run it with: python examples/gallery/callers_hang_up_after_waiting_too_long_builder.py
    """

    import logging
    from datetime import datetime

    from desflow import ConsoleEgress, SimulationContext
    from desflow.values import generator

    UNTIL = 3600


    def build() -> SimulationContext:
        """Builds the model."""
        app = (
            SimulationContext(
                sim_id="Centre",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1),
            )
            .add_egress(ConsoleEgress())
            .add_resource("agents", current_cap=2, max_cap=4)
            .add_service("talk", dist="normal", mean="4 min", std="1 min")
            .add_arrival("calls", "exponential", rate=0.015, spawn="answer_call")
            .add_scenario(
                [
                    {
                        "at": "30 min",
                        "path": "Centre.resources.agents.current_cap",
                        "value": 4,
                    }
                ]
            )
        )

        @app.task(
            name="answer_call",
            service_id="talk",
            resource_id="agents",
            patience=generator({"gen": "uniform", "low": 60, "high": 300}),
            on_renege="hang_up",
        )
        def answer_call(task_id):
            return {"event_type": "answered", "call_id": task_id}

        @app.task(name="hang_up")
        def hang_up(task_id):
            return {"event_type": "hung_up"}

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build().run(until=UNTIL)
    ```

=== "Low-level"

    The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

    ```bash
    python examples/gallery/callers_hang_up_after_waiting_too_long_low_level.py
    ```

    The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `tasks.answer_call.patience`. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's.

    ```python title="examples/gallery/callers_hang_up_after_waiting_too_long_low_level.py"
    """Callers hang up when they wait for an agent for longer than they will tolerate.

    The Low-level form of callers_hang_up_after_waiting_too_long.yaml, with the same
    records, built on `DynamicRealtimeEnvironment`. Calls arrive at 54 an hour and two
    agents answer about 30 an hour, so a queue builds. A caller who waits longer than their
    patience, 1 to 5 minutes, hangs up. A scenario step adds two agents after half an hour,
    so calls wait less and fewer callers hang up. A process adds them at 30 minutes,
    through the registry.

    Needs: nothing
    Run it with: python examples/gallery/callers_hang_up_after_waiting_too_long_low_level.py
    """

    import itertools
    import logging
    from datetime import datetime

    from desflow import (
        CapacityConfig,
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        Sampler,
        SimParameter,
    )

    SEED = 42
    UNTIL = 3600
    PARAMS = SimParameter(
        sim_id="Centre",
        arrival={"calls": DistributionConfig(dist="exponential", rate=0.015)},
        # DistributionConfig takes seconds: 4 min (std 1 min) for a call
        service={"talk": DistributionConfig(dist="normal", mean=240, std=60)},
        resources={"agents": CapacityConfig(current_cap=2, max_cap=4)},
    )


    def build(env):
        """Starts the call source and the shift change on `env`."""
        env.registry.register_sim_parameter(PARAMS)
        sampler = Sampler(seed=SEED)
        patience = sampler.generator("tasks.answer_call.patience")
        agents = DynamicResource(env, "Centre", "agents")
        path_id = "Centre.service.talk"
        instances = {"answer_call": itertools.count(), "hang_up": itertools.count()}

        def call(key):
            instance = next(instances["answer_call"])

            def publish(value):
                env.publish_event(key, value, task="answer_call", instance=instance)

            waits = 60 + (300 - 60) * patience.random()
            publish(
                {
                    "path_id": path_id,
                    "sim_id": "Centre",
                    "service": "talk",
                    "status": "queued",
                }
            )
            with agents.request() as request:
                got = yield request | env.timeout(waits)
                if request not in got:
                    publish(
                        {
                            "path_id": path_id,
                            "sim_id": "Centre",
                            "service": "talk",
                            "status": "reneged",
                        }
                    )
                    env.publish_event(
                        key,
                        {"event_type": "hung_up"},
                        task="hang_up",
                        instance=next(instances["hang_up"]),
                    )
                    return
                publish(
                    {
                        "path_id": path_id,
                        "sim_id": "Centre",
                        "service": "talk",
                        "status": "started",
                    }
                )
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                publish({"event_type": "answered", "call_id": instance})

        def calls():
            gap = env.registry.get_config("Centre.arrival.calls")
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(call(f"calls-{number}"))

        def shift_change():
            yield env.timeout(1800)
            env.registry.update("Centre.resources.agents.current_cap", 4)

        env.process(calls())
        env.process(shift_change())


    def run():
        """Runs the model for one simulation hour."""
        env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
        env.setup_egress([ConsoleEgress()])
        build(env)
        try:
            env.run(until=UNTIL)
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        run()
    ```

### Urgent support tickets are served before older tickets of lower priority

One agent solves a ticket in about 50 seconds, and a ticket arrives about every 55 seconds, so tickets wait. The agent's queue is ordered by priority, so a ticket of priority 1 (10 percent of them) waits less than one of priority 3 (60 percent).

=== "YAML"

    Run it with the sample option, which stops after the first events and prints them:

    ```bash
    desflow run examples/gallery/urgent_tickets_are_served_first.yaml --sample 5
    ```

    ```yaml title="examples/gallery/urgent_tickets_are_served_first.yaml"
    # Urgent support tickets are served before older tickets of lower priority.
    #
    # One agent solves a ticket in about 50 seconds, and a ticket arrives about every 55
    # seconds, so tickets wait. The agent's queue is ordered by priority, so a ticket of
    # priority 1 (10 percent of them) waits less than one of priority 3 (60 percent).
    #
    # Needs: nothing
    # Run it with: desflow run examples/gallery/urgent_tickets_are_served_first.yaml

    simulation:
      sim_id: Support
      factor: 0
      random_seed: 42
      logical_start_time: 2026-01-01T00:00:00

    egress:
      - {type: Console}

    resources:
      agents: {current_cap: 1, max_cap: 1, queue: priority}

    services:
      solve: {dist: normal, mean: 50, std: 10}

    arrivals:
      tickets:
        dist: exponential
        rate: 0.018
        attributes:
          priority:
            gen: weighted
            options: {1: 10, 2: 30, 3: 60}
        spawn: solve_ticket

    tasks:
      solve_ticket:
        service: solve
        resource: agents
        priority: attr.priority
        payload:
          event_type: solved
          priority: {gen: attr, name: priority}

    run: {until: 4 hours}
    ```

    <!-- records: gallery-urgent_tickets_are_served_first -->
    ```text
    {"stream_type":"event","sim_ts":15.04613672339676,"timestamp":"...","key":"tickets-1","task":"solve_ticket","instance":0,"value":{"path_id":"Support.service.solve","sim_id":"Support","service":"solve","status":"queued"}}
    {"stream_type":"event","sim_ts":15.04613672339676,"timestamp":"...","key":"tickets-1","task":"solve_ticket","instance":0,"value":{"path_id":"Support.service.solve","sim_id":"Support","service":"solve","status":"started"}}
    {"stream_type":"event","sim_ts":78.19013668146923,"timestamp":"...","key":"tickets-1","task":"solve_ticket","instance":0,"value":{"event_type":"solved","priority":3}}
    {"stream_type":"event","sim_ts":87.67926599645057,"timestamp":"...","key":"tickets-2","task":"solve_ticket","instance":1,"value":{"path_id":"Support.service.solve","sim_id":"Support","service":"solve","status":"queued"}}
    {"stream_type":"event","sim_ts":87.67926599645057,"timestamp":"...","key":"tickets-2","task":"solve_ticket","instance":1,"value":{"path_id":"Support.service.solve","sim_id":"Support","service":"solve","status":"started"}}
    ```

=== "Builder"

    The Builder file builds the same model with the builder API and prints every record of its run:

    ```bash
    python examples/gallery/urgent_tickets_are_served_first_builder.py
    ```

    ```python title="examples/gallery/urgent_tickets_are_served_first_builder.py"
    """Urgent support tickets are served before older tickets of lower priority.

    The Builder form of urgent_tickets_are_served_first.yaml, with the same records. One
    agent solves a ticket in about 50 seconds, and the agent's queue is ordered by the
    priority each ticket arrives with, so a ticket of priority 1 waits less than one of
    priority 3.

    Needs: nothing
    Run it with: python examples/gallery/urgent_tickets_are_served_first_builder.py
    """

    import logging
    from datetime import datetime

    from desflow import ConsoleEgress, SimulationContext
    from desflow.values import Template, generator

    UNTIL = 14400


    def build() -> SimulationContext:
        """Builds the model."""
        app = (
            SimulationContext(
                sim_id="Support",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1),
            )
            .add_egress(ConsoleEgress())
            .add_resource("agents", current_cap=1, max_cap=1, queue="priority")
            .add_service("solve", dist="normal", mean=50, std=10)
            .add_arrival(
                "tickets",
                "exponential",
                rate=0.018,
                attributes={
                    "priority": generator(
                        {"gen": "weighted", "options": {1: 10, 2: 30, 3: 60}}
                    )
                },
                spawn="solve_ticket",
            )
        )

        solved_payload = Template(
            {"event_type": "solved", "priority": {"gen": "attr", "name": "priority"}},
            name="tasks.solve_ticket",
        )

        @app.task(
            name="solve_ticket",
            service_id="solve",
            resource_id="agents",
            priority="attr.priority",
        )
        def solve_ticket(task_id):
            return solved_payload

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build().run(until=UNTIL)
    ```

=== "Low-level"

    The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

    ```bash
    python examples/gallery/urgent_tickets_are_served_first_low_level.py
    ```

    The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `arrivals.tickets.attributes.priority`. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's.

    ```python title="examples/gallery/urgent_tickets_are_served_first_low_level.py"
    """Urgent support tickets are served before older tickets of lower priority.

    The Low-level form of urgent_tickets_are_served_first.yaml, with the same records,
    built on `DynamicRealtimeEnvironment`. The agent is a `DynamicResource` whose queue
    order is `priority`, and each ticket asks for it with its own priority. The priority is
    drawn with `Sampler`, from the random sequence the blueprint's generator uses.

    Needs: nothing
    Run it with: python examples/gallery/urgent_tickets_are_served_first_low_level.py
    """

    import itertools
    import logging
    from datetime import datetime

    from desflow import (
        CapacityConfig,
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        Sampler,
        SimParameter,
    )

    SEED = 42
    UNTIL = 14400
    PARAMS = SimParameter(
        sim_id="Support",
        arrival={"tickets": DistributionConfig(dist="exponential", rate=0.018)},
        service={"solve": DistributionConfig(dist="normal", mean=50, std=10)},
        resources={"agents": CapacityConfig(current_cap=1, max_cap=1)},
    )


    def weighted(rng, options):
        """One key of `options`, chosen by its weight, as the `weighted` generator does."""
        point = rng.random() * sum(options.values())
        running = 0.0
        for value, weight in options.items():
            running += weight
            if point < running:
                return value
        return value


    def build(env):
        """Starts the ticket source on `env`."""
        env.registry.register_sim_parameter(PARAMS)
        sampler = Sampler(seed=SEED)
        pick = sampler.generator("arrivals.tickets.attributes.priority")
        agents = DynamicResource(env, "Support", "agents", queue="priority")
        path_id = "Support.service.solve"

        def ticket(key, priority, instance):
            def publish(value):
                env.publish_event(key, value, task="solve_ticket", instance=instance)

            publish(
                {
                    "path_id": path_id,
                    "sim_id": "Support",
                    "service": "solve",
                    "status": "queued",
                }
            )
            # The lowest priority number is served first
            with agents.request(priority=priority) as request:
                yield request
                publish(
                    {
                        "path_id": path_id,
                        "sim_id": "Support",
                        "service": "solve",
                        "status": "started",
                    }
                )
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                publish({"event_type": "solved", "priority": priority})

        def tickets():
            gap = env.registry.get_config("Support.arrival.tickets")
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                priority = weighted(pick, {1: 10, 2: 30, 3: 60})
                env.process(ticket(f"tickets-{number}", priority, number - 1))

        env.process(tickets())


    def run():
        """Runs the model for four simulation hours."""
        env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
        env.setup_egress([ConsoleEgress()])
        build(env)
        try:
            env.run(until=UNTIL)
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        run()
    ```

### A shopper keeps adding items to a cart, then pays or abandons it

A shopper adds an item every 5 to 30 seconds, and after each item 60 percent add another, 25 percent pay and 15 percent leave. Two tills serve the payments, each for about 25 seconds, so shoppers queue to pay when both are busy.

=== "YAML"

    Run it with the sample option, which stops after the first events and prints them:

    ```bash
    desflow run examples/gallery/shoppers_add_items_then_pay_or_abandon.yaml --sample 5
    ```

    ```yaml title="examples/gallery/shoppers_add_items_then_pay_or_abandon.yaml"
    # A shopper keeps adding items to a cart, then pays or abandons it.
    #
    # A shopper adds an item every 5 to 30 seconds, and after each item 60 percent add
    # another, 25 percent pay and 15 percent leave. Two tills serve the payments, each for
    # about 25 seconds, so shoppers queue to pay when both are busy.
    #
    # Needs: nothing
    # Run it with: desflow run examples/gallery/shoppers_add_items_then_pay_or_abandon.yaml

    simulation:
      sim_id: Shop
      factor: 0
      random_seed: 42
      logical_start_time: 2026-01-01T00:00:00

    egress:
      - {type: Console}

    resources:
      tills: {current_cap: 2, max_cap: 2}

    services:
      checkout: {dist: normal, mean: 25, std: 5}

    arrivals:
      shoppers:
        dist: exponential
        rate: 0.1
        attributes: {items: 0}
        spawn: add_item

    tasks:
      add_item:
        service_time: {gen: uniform, low: 5, high: 30}
        set:
          items: {gen: expr, expr: attr.items + 1}
        payload:
          event_type: item_added
          items: {gen: expr, expr: attr.items + 1}
        next:
          weights: {add_item: 60, pay: 25, abandon: 15}
      pay:
        service: checkout
        resource: tills
        payload:
          event_type: paid
          items: {gen: attr, name: items}
      abandon:
        payload:
          event_type: abandoned
          items: {gen: attr, name: items}

    run: {until: 1 hour}
    ```

    <!-- records: gallery-shoppers_add_items_then_pay_or_abandon -->
    ```text
    {"stream_type":"event","sim_ts":10.467584677592853,"timestamp":"...","key":"shoppers-1","task":"add_item","instance":0,"value":{"status":"started"}}
    {"stream_type":"event","sim_ts":11.299271630799119,"timestamp":"...","key":"shoppers-2","task":"add_item","instance":1,"value":{"status":"started"}}
    {"stream_type":"event","sim_ts":11.629322773869537,"timestamp":"...","key":"shoppers-3","task":"add_item","instance":2,"value":{"status":"started"}}
    {"stream_type":"event","sim_ts":20.121210214963746,"timestamp":"...","key":"shoppers-4","task":"add_item","instance":3,"value":{"status":"started"}}
    {"stream_type":"event","sim_ts":22.33567323143638,"timestamp":"...","key":"shoppers-5","task":"add_item","instance":4,"value":{"status":"started"}}
    ```

=== "Builder"

    The Builder file builds the same model with the builder API and prints every record of its run:

    ```bash
    python examples/gallery/shoppers_add_items_then_pay_or_abandon_builder.py
    ```

    ```python title="examples/gallery/shoppers_add_items_then_pay_or_abandon_builder.py"
    """A shopper keeps adding items to a cart, then pays or abandons it.

    The Builder form of shoppers_add_items_then_pay_or_abandon.yaml, with the same records.
    A shopper adds an item every 5 to 30 seconds, and after each item 60 percent add
    another, 25 percent pay and 15 percent leave. Two tills serve the payments, each for
    about 25 seconds.

    Needs: nothing
    Run it with: python examples/gallery/shoppers_add_items_then_pay_or_abandon_builder.py
    """

    import logging
    from datetime import datetime

    from desflow import ConsoleEgress, SimulationContext
    from desflow.values import Template, generator

    UNTIL = 3600


    def build() -> SimulationContext:
        """Builds the model."""
        app = (
            SimulationContext(
                sim_id="Shop",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1),
            )
            .add_egress(ConsoleEgress())
            .add_resource("tills", current_cap=2, max_cap=2)
            .add_service("checkout", dist="normal", mean=25, std=5)
            .add_arrival(
                "shoppers",
                "exponential",
                rate=0.1,
                attributes={"items": 0},
                spawn="add_item",
            )
        )

        item_added_payload = Template(
            {
                "event_type": "item_added",
                "items": {"gen": "expr", "expr": "attr.items + 1"},
            },
            name="tasks.add_item",
        )
        paid_payload = Template(
            {"event_type": "paid", "items": {"gen": "attr", "name": "items"}},
            name="tasks.pay",
        )
        abandoned_payload = Template(
            {"event_type": "abandoned", "items": {"gen": "attr", "name": "items"}},
            name="tasks.abandon",
        )

        @app.task(
            name="add_item",
            service_time=generator({"gen": "uniform", "low": 5, "high": 30}),
            set={"items": generator({"gen": "expr", "expr": "attr.items + 1"})},
            next={"weights": {"add_item": 60, "pay": 25, "abandon": 15}},
        )
        def add_item(task_id):
            return item_added_payload

        @app.task(name="pay", resource_id="tills", service_id="checkout")
        def pay(task_id):
            return paid_payload

        @app.task(name="abandon")
        def abandon(task_id):
            return abandoned_payload

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build().run(until=UNTIL)
    ```

=== "Low-level"

    The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

    ```bash
    python examples/gallery/shoppers_add_items_then_pay_or_abandon_low_level.py
    ```

    The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `tasks.add_item.service_time`. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's. The branch of each step is chosen by a `Router` whose random sequence is `Sampler.generator("route.add_item")`, the sequence of the blueprint's `next`.

    ```python title="examples/gallery/shoppers_add_items_then_pay_or_abandon_low_level.py"
    """A shopper keeps adding items to a cart, then pays or abandons it.

    The Low-level form of shoppers_add_items_then_pay_or_abandon.yaml, with the same
    records, built on `DynamicRealtimeEnvironment`. A shopper adds an item every 5 to 30
    seconds, and after each item 60 percent add another, 25 percent pay and 15 percent
    leave. Two tills serve the payments, each for about 25 seconds.

    Needs: nothing
    Run it with: python examples/gallery/shoppers_add_items_then_pay_or_abandon_low_level.py
    """

    import itertools
    import logging
    from datetime import datetime

    from desflow import (
        CapacityConfig,
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        Router,
        Sampler,
        SimParameter,
    )

    SEED = 42
    UNTIL = 3600
    PARAMS = SimParameter(
        sim_id="Shop",
        arrival={"shoppers": DistributionConfig(dist="exponential", rate=0.1)},
        service={"checkout": DistributionConfig(dist="normal", mean=25, std=5)},
        resources={"tills": CapacityConfig(current_cap=2, max_cap=2)},
    )


    def build(env):
        """Starts the shopper source on `env`."""
        env.registry.register_sim_parameter(PARAMS)
        sampler = Sampler(seed=SEED)
        browse = sampler.generator("tasks.add_item.service_time")
        router = Router(
            {"weights": {"add_item": 60, "pay": 25, "abandon": 15}},
            rng=sampler.generator("route.add_item"),
        )
        tills = DynamicResource(env, "Shop", "tills")
        path_id = "Shop.service.checkout"
        instances = {task: itertools.count() for task in ("add_item", "pay", "abandon")}

        def shopper(key):
            cart = {"items": 0}
            while True:
                instance = next(instances["add_item"])
                env.publish_event(
                    key, {"status": "started"}, task="add_item", instance=instance
                )
                yield env.timeout(5 + (30 - 5) * browse.random())
                env.publish_event(
                    key,
                    {"event_type": "item_added", "items": cart["items"] + 1},
                    task="add_item",
                    instance=instance,
                )
                cart["items"] += 1  # the blueprint's `set`, applied after the record
                task = router.choose(None, env=env)
                if task != "add_item":
                    break
            instance = next(instances[task])
            if task == "pay":
                env.publish_event(
                    key,
                    {
                        "path_id": path_id,
                        "sim_id": "Shop",
                        "service": "checkout",
                        "status": "queued",
                    },
                    task="pay",
                    instance=instance,
                )
                with tills.request() as request:
                    yield request
                    env.publish_event(
                        key,
                        {
                            "path_id": path_id,
                            "sim_id": "Shop",
                            "service": "checkout",
                            "status": "started",
                        },
                        task="pay",
                        instance=instance,
                    )
                    yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                    env.publish_event(
                        key,
                        {"event_type": "paid", "items": cart["items"]},
                        task="pay",
                        instance=instance,
                    )
            else:
                env.publish_event(
                    key,
                    {"event_type": "abandoned", "items": cart["items"]},
                    task="abandon",
                    instance=instance,
                )

        def shoppers():
            gap = env.registry.get_config("Shop.arrival.shoppers")
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(shopper(f"shoppers-{number}"))

        env.process(shoppers())


    def run():
        """Runs the model for one simulation hour."""
        env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
        env.setup_egress([ConsoleEgress()])
        build(env)
        try:
            env.run(until=UNTIL)
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        run()
    ```

### Visitors drop out at each step of a signup funnel

Of every 100 visitors, 40 sign up, 20 of those start a trial, and 5 of those pay. A visitor who drops out ends in one `left` task, whose record names the step the visitor left after.

=== "YAML"

    Run it with the sample option, which stops after the first events and prints them:

    ```bash
    desflow run examples/gallery/visitors_drop_out_at_each_signup_step.yaml --sample 5
    ```

    ```yaml title="examples/gallery/visitors_drop_out_at_each_signup_step.yaml"
    # Visitors drop out at each step of a signup funnel.
    #
    # Of every 100 visitors, 40 sign up, 20 of those start a trial, and 5 of those pay. A
    # visitor who drops out ends in one `left` task, whose record names the step the
    # visitor left after.
    #
    # Needs: nothing
    # Run it with: desflow run examples/gallery/visitors_drop_out_at_each_signup_step.yaml

    simulation:
      sim_id: Funnel
      factor: 0
      random_seed: 42
      logical_start_time: 2026-01-01T00:00:00

    egress:
      - {type: Console}

    arrivals:
      visitors: {dist: exponential, rate: 0.5, spawn: visited}

    tasks:
      visited:
        payload: {stage: visited}
        next:
          weights: {signed_up: 40, left: 60}
      signed_up:
        service_time: {gen: uniform, low: 5, high: 30}
        payload: {stage: signed_up}
        next:
          weights: {trial_started: 50, left: 50}
      trial_started:
        service_time: {gen: uniform, low: 10, high: 60}
        payload: {stage: trial_started}
        next:
          weights: {paid: 25, left: 75}
      paid:
        payload: {stage: paid}
      left:
        payload:
          stage: left
          after: {gen: upstream, path: stage}

    run: {until: 20 min}
    ```

    <!-- records: gallery-visitors_drop_out_at_each_signup_step -->
    ```text
    {"stream_type":"event","sim_ts":5.685646145804752,"timestamp":"...","key":"visitors-1","task":"visited","instance":0,"value":{"stage":"visited"}}
    {"stream_type":"event","sim_ts":5.685646145804752,"timestamp":"...","key":"visitors-1","task":"left","instance":0,"value":{"stage":"left","after":"visited"}}
    {"stream_type":"event","sim_ts":6.716550977533494,"timestamp":"...","key":"visitors-2","task":"visited","instance":1,"value":{"stage":"visited"}}
    {"stream_type":"event","sim_ts":6.716550977533494,"timestamp":"...","key":"visitors-2","task":"left","instance":1,"value":{"stage":"left","after":"visited"}}
    {"stream_type":"event","sim_ts":8.101630035966295,"timestamp":"...","key":"visitors-3","task":"visited","instance":2,"value":{"stage":"visited"}}
    ```

=== "Builder"

    The Builder file builds the same model with the builder API and prints every record of its run:

    ```bash
    python examples/gallery/visitors_drop_out_at_each_signup_step_builder.py
    ```

    ```python title="examples/gallery/visitors_drop_out_at_each_signup_step_builder.py"
    """Visitors drop out at each step of a signup funnel.

    The Builder form of visitors_drop_out_at_each_signup_step.yaml, with the same records.
    Of every 100 visitors, 40 sign up, 20 of those start a trial, and 5 of those pay. A
    visitor who drops out ends in one `left` task, whose record names the step the visitor
    left after.

    Needs: nothing
    Run it with: python examples/gallery/visitors_drop_out_at_each_signup_step_builder.py
    """

    import logging
    from datetime import datetime

    from desflow import ConsoleEgress, SimulationContext
    from desflow.values import Template, generator

    UNTIL = 1200


    def build() -> SimulationContext:
        """Builds the model."""
        app = (
            SimulationContext(
                sim_id="Funnel",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1),
            )
            .add_egress(ConsoleEgress())
            .add_arrival("visitors", "exponential", rate=0.5, spawn="visited")
        )

        left_payload = Template(
            {"stage": "left", "after": {"gen": "upstream", "path": "stage"}},
            name="tasks.left",
        )

        @app.task(name="visited", next={"weights": {"signed_up": 40, "left": 60}})
        def visited(task_id):
            return {"stage": "visited"}

        @app.task(
            name="signed_up",
            service_time=generator({"gen": "uniform", "low": 5, "high": 30}),
            next={"weights": {"trial_started": 50, "left": 50}},
        )
        def signed_up(task_id):
            return {"stage": "signed_up"}

        @app.task(
            name="trial_started",
            service_time=generator({"gen": "uniform", "low": 10, "high": 60}),
            next={"weights": {"paid": 25, "left": 75}},
        )
        def trial_started(task_id):
            return {"stage": "trial_started"}

        @app.task(name="paid")
        def paid(task_id):
            return {"stage": "paid"}

        @app.task(name="left")
        def left(task_id):
            return left_payload

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build().run(until=UNTIL)
    ```

=== "Low-level"

    The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

    ```bash
    python examples/gallery/visitors_drop_out_at_each_signup_step_low_level.py
    ```

    The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `route.{step}` with the place of the task filled in. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's. The branch of each step is chosen by a `Router` whose random sequence is `Sampler.generator("route.{step}")`, and one more for each step, the sequence of the blueprint's `next`. The gaps between arrivals come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.visitors`, the sequence the blueprint uses for its arrivals.

    ```python title="examples/gallery/visitors_drop_out_at_each_signup_step_low_level.py"
    """Visitors drop out at each step of a signup funnel.

    The Low-level form of visitors_drop_out_at_each_signup_step.yaml, with the same
    records, built on `DynamicRealtimeEnvironment`. Each visitor is one process that goes
    through the steps. A `Router` for each step makes the choice as the blueprint's `next`
    does, and the time a step takes is drawn with `Sampler`, from the random sequence the
    blueprint's generator uses.

    Needs: nothing
    Run it with: python examples/gallery/visitors_drop_out_at_each_signup_step_low_level.py
    """

    import itertools
    import logging
    from datetime import datetime

    from desflow import (
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        Router,
        Sampler,
    )

    SEED = 42
    UNTIL = 1200
    # Each step: its name, the least and the most seconds it takes (none for the first),
    # and the weights of the choice that follows it
    STEPS = [
        ("visited", None, {"signed_up": 40, "left": 60}),
        ("signed_up", (5, 30), {"trial_started": 50, "left": 50}),
        ("trial_started", (10, 60), {"paid": 25, "left": 75}),
    ]


    def build(env):
        """Starts the visitor source on `env`."""
        sampler = Sampler(seed=SEED)
        times, routers = {}, {}
        bounds_of = {step: bounds for step, bounds, _ in STEPS}
        for step, bounds, weights in STEPS:
            routers[step] = Router(
                {"weights": weights}, rng=sampler.generator(f"route.{step}")
            )
            if bounds is not None:
                times[step] = sampler.generator(f"tasks.{step}.service_time")
        gap = DistributionConfig(dist="exponential", rate=0.5)
        gap.stream = "arrival.visitors"
        instances = {
            task: itertools.count()
            for task in ("visited", "signed_up", "trial_started", "paid", "left")
        }

        def visitor(key):
            step = "visited"
            while step in routers:
                instance = next(instances[step])
                if step in times:
                    env.publish_event(
                        key, {"status": "started"}, task=step, instance=instance
                    )
                    low, high = bounds_of[step]
                    yield env.timeout(low + (high - low) * times[step].random())
                env.publish_event(key, {"stage": step}, task=step, instance=instance)
                came_from, step = step, routers[step].choose(None, env=env)
            # `paid` and `left` end the visit. `left` names the step the visitor left after.
            payload = {"stage": step}
            if step == "left":
                payload["after"] = came_from
            env.publish_event(key, payload, task=step, instance=next(instances[step]))

        def visitors():
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(visitor(f"visitors-{number}"))

        env.process(visitors())


    def run():
        """Runs the model for 20 simulation minutes."""
        env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
        env.setup_egress([ConsoleEgress()])
        build(env)
        try:
            env.run(until=UNTIL)
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        run()
    ```

### Each bet is settled after the match, and a winner is paid the stake times the odds

A bet is placed with a stake and odds. One to two minutes later it is settled: 45 percent win and are paid the stake times the odds, the rest lose and are paid nothing, and the settled record repeats the bet id.

=== "YAML"

    Run it with the sample option, which stops after the first events and prints them:

    ```bash
    desflow run examples/gallery/bets_are_settled_and_winners_paid.yaml --sample 5
    ```

    ```yaml title="examples/gallery/bets_are_settled_and_winners_paid.yaml"
    # Each bet is settled after the match, and a winner is paid the stake times the odds.
    #
    # A bet is placed with a stake and odds. One to two minutes later it is settled: 45
    # percent win and are paid the stake times the odds, the rest lose and are paid
    # nothing, and the settled record repeats the bet id.
    #
    # Needs: nothing
    # Run it with: desflow run examples/gallery/bets_are_settled_and_winners_paid.yaml

    simulation:
      sim_id: Book
      factor: 0
      random_seed: 42
      logical_start_time: 2026-01-01T00:00:00

    egress:
      - {type: Console}

    arrivals:
      bets: {dist: exponential, rate: 0.2, spawn: place_bet}

    tasks:
      place_bet:
        payload:
          bet_id: {gen: sequence, start: 1}
          stake:
            gen: weighted
            options: {5: 50, 10: 30, 50: 15, 100: 5}
          odds: {gen: uniform, low: 1.5, high: 4, decimals: 2}
        next: settle_bet
      settle_bet:
        vars:
          won: {gen: boolean, p: 0.45}
        service_time: {gen: uniform, low: 60, high: 120}
        payload:
          bet_id: {gen: upstream, path: bet_id}
          result: {gen: when, condition: won, then: won, else: lost}
          payout:
            gen: when
            condition: won
            then:
              gen: multiply
              args:
                - {gen: upstream, path: stake}
                - {gen: upstream, path: odds}
              decimals: 2
            else: 0

    run: {until: 20 min}
    ```

    <!-- records: gallery-bets_are_settled_and_winners_paid -->
    ```text
    {"stream_type":"event","sim_ts":5.905507390799418,"timestamp":"...","key":"bets-1","task":"place_bet","instance":0,"value":{"bet_id":1,"stake":10,"odds":3.83}}
    {"stream_type":"event","sim_ts":5.905507390799418,"timestamp":"...","key":"bets-1","task":"settle_bet","instance":0,"value":{"status":"started"}}
    {"stream_type":"event","sim_ts":7.5806398141048446,"timestamp":"...","key":"bets-2","task":"place_bet","instance":1,"value":{"bet_id":2,"stake":10,"odds":2.74}}
    {"stream_type":"event","sim_ts":7.5806398141048446,"timestamp":"...","key":"bets-2","task":"settle_bet","instance":1,"value":{"status":"started"}}
    {"stream_type":"event","sim_ts":12.491926619839258,"timestamp":"...","key":"bets-3","task":"place_bet","instance":2,"value":{"bet_id":3,"stake":5,"odds":1.5}}
    ```

=== "Builder"

    The Builder file builds the same model with the builder API and prints every record of its run:

    ```bash
    python examples/gallery/bets_are_settled_and_winners_paid_builder.py
    ```

    ```python title="examples/gallery/bets_are_settled_and_winners_paid_builder.py"
    """Each bet is settled after the match, and a winner is paid the stake times the odds.

    The Builder form of bets_are_settled_and_winners_paid.yaml, with the same records. A bet
    is placed with a stake and odds. One to two minutes later it is settled: 45 percent win
    and are paid the stake times the odds, and the rest lose and are paid nothing.

    Needs: nothing
    Run it with: python examples/gallery/bets_are_settled_and_winners_paid_builder.py
    """

    import logging
    from datetime import datetime

    from desflow import ConsoleEgress, SimulationContext
    from desflow.values import Template, generator

    UNTIL = 1200


    def build() -> SimulationContext:
        """Builds the model."""
        app = (
            SimulationContext(
                sim_id="Book",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1),
            )
            .add_egress(ConsoleEgress())
            .add_arrival("bets", "exponential", rate=0.2, spawn="place_bet")
        )

        place_payload = Template(
            {
                "bet_id": {"gen": "sequence", "start": 1},
                "stake": {
                    "gen": "weighted",
                    "options": {5: 50, 10: 30, 50: 15, 100: 5},
                },
                "odds": {"gen": "uniform", "low": 1.5, "high": 4, "decimals": 2},
            },
            name="tasks.place_bet",
        )
        settle_payload = Template(
            {
                "bet_id": {"gen": "upstream", "path": "bet_id"},
                "result": {
                    "gen": "when",
                    "condition": "won",
                    "then": "won",
                    "else": "lost",
                },
                "payout": {
                    "gen": "when",
                    "condition": "won",
                    "then": {
                        "gen": "multiply",
                        "args": [
                            {"gen": "upstream", "path": "stake"},
                            {"gen": "upstream", "path": "odds"},
                        ],
                        "decimals": 2,
                    },
                    "else": 0,
                },
            },
            name="tasks.settle_bet",
        )

        @app.task(name="place_bet", next="settle_bet")
        def place_bet(task_id):
            return place_payload

        @app.task(
            name="settle_bet",
            vars={"won": generator({"gen": "boolean", "p": 0.45})},
            service_time=generator({"gen": "uniform", "low": 60, "high": 120}),
        )
        def settle_bet(task_id):
            return settle_payload

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build().run(until=UNTIL)
    ```

=== "Low-level"

    The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

    ```bash
    python examples/gallery/bets_are_settled_and_winners_paid_low_level.py
    ```

    The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `tasks.place_bet.payload.stake`, `tasks.place_bet.payload.odds` and `tasks.settle_bet.vars.won` and 1 more. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's. The gaps between arrivals come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.bets`, the sequence the blueprint uses for its arrivals.

    ```python title="examples/gallery/bets_are_settled_and_winners_paid_low_level.py"
    """Each bet is settled after the match, and a winner is paid the stake times the odds.

    The Low-level form of bets_are_settled_and_winners_paid.yaml, with the same records,
    built on `DynamicRealtimeEnvironment`. A bet is placed with a stake and odds. One to two
    minutes later it is settled: 45 percent win and are paid the stake times the odds, and
    the rest lose and are paid nothing.

    Needs: nothing
    Run it with: python examples/gallery/bets_are_settled_and_winners_paid_low_level.py
    """

    import itertools
    import logging
    from datetime import datetime

    from desflow import (
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        Sampler,
    )

    SEED = 42
    UNTIL = 1200
    STAKES = {5: 50, 10: 30, 50: 15, 100: 5}


    def weighted(rng, options):
        """One key of `options`, chosen by its weight, as the `weighted` generator does."""
        point = rng.random() * sum(options.values())
        running = 0.0
        for value, weight in options.items():
            running += weight
            if point < running:
                return value
        return value


    def build(env):
        """Starts the bet source on `env`."""
        sampler = Sampler(seed=SEED)
        stake = sampler.generator("tasks.place_bet.payload.stake")
        odds = sampler.generator("tasks.place_bet.payload.odds")
        wins = sampler.generator("tasks.settle_bet.vars.won")
        until_settled = sampler.generator("tasks.settle_bet.service_time")
        gap = DistributionConfig(dist="exponential", rate=0.2)
        gap.stream = "arrival.bets"
        bet_ids = itertools.count(1)

        def bet(key, instance):
            placed = {
                "bet_id": next(bet_ids),
                "stake": weighted(stake, STAKES),
                "odds": round(1.5 + (4 - 1.5) * odds.random(), 2),
            }
            env.publish_event(key, placed, task="place_bet", instance=instance)
            env.publish_event(
                key, {"status": "started"}, task="settle_bet", instance=instance
            )
            yield env.timeout(60 + (120 - 60) * until_settled.random())
            # The blueprint's `won` var is first read when the settled record is made
            won = bool(wins.random() < 0.45)
            payout = round(placed["stake"] * placed["odds"], 2) if won else 0
            env.publish_event(
                key,
                {
                    "bet_id": placed["bet_id"],
                    "result": "won" if won else "lost",
                    "payout": payout,
                },
                task="settle_bet",
                instance=instance,
            )

        def bets():
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(bet(f"bets-{number}", number - 1))

        env.process(bets())


    def run():
        """Runs the model for 20 simulation minutes."""
        env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
        env.setup_egress([ConsoleEgress()])
        build(env)
        try:
            env.run(until=UNTIL)
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        run()
    ```

### An audit that finds an issue needs a follow-up, which waits for the same two auditors

An audit of one of 150 merchants is due about every 3 hours and takes an auditor about 4 hours, and 15 percent find an issue. A follow-up takes about an hour and goes to the back of the same queue, so audits and follow-ups compete for the two auditors.

=== "YAML"

    Run it with the sample option, which stops after the first events and prints them:

    ```bash
    desflow run examples/gallery/audits_with_findings_need_a_follow_up.yaml --sample 5
    ```

    ```yaml title="examples/gallery/audits_with_findings_need_a_follow_up.yaml"
    # An audit that finds an issue needs a follow-up, which waits for the same two auditors.
    #
    # An audit of one of 150 merchants is due about every 3 hours and takes an auditor
    # about 4 hours, and 15 percent find an issue. A follow-up takes about an hour and
    # goes to the back of the same queue, so audits and follow-ups compete for the two
    # auditors.
    #
    # Needs: nothing
    # Run it with: desflow run examples/gallery/audits_with_findings_need_a_follow_up.yaml

    simulation:
      sim_id: Compliance
      factor: 0
      random_seed: 42
      logical_start_time: 2026-01-01T00:00:00

    egress:
      - {type: Console}

    resources:
      auditors: {current_cap: 2, max_cap: 2}

    services:
      audit: {dist: normal, mean: 4 hours, std: 1 hour}
      follow_up: {dist: normal, mean: 1 hour, std: 15 min}

    arrivals:
      audits_due: {dist: exponential, rate: 0.0001, spawn: audit}

    tasks:
      audit:
        service: audit
        resource: auditors
        payload:
          merchant: {gen: integer, bounds: [1, 150]}
          finding:
            gen: weighted
            options: {clean: 85, issue: 15}
        next:
          when:
            - {condition: "payload.finding == 'issue'", task: follow_up}
      follow_up:
        service: follow_up
        resource: auditors
        payload:
          merchant: {gen: upstream, path: merchant}
          event_type: follow_up_done

    run: {until: 14 days}
    ```

    <!-- records: gallery-audits_with_findings_need_a_follow_up -->
    ```text
    {"stream_type":"event","sim_ts":1105.7526406613883,"timestamp":"...","key":"audits_due-1","task":"audit","instance":0,"value":{"path_id":"Compliance.service.audit","sim_id":"Compliance","service":"audit","status":"queued"}}
    {"stream_type":"event","sim_ts":1105.7526406613883,"timestamp":"...","key":"audits_due-1","task":"audit","instance":0,"value":{"path_id":"Compliance.service.audit","sim_id":"Compliance","service":"audit","status":"started"}}
    {"stream_type":"event","sim_ts":19750.84009484635,"timestamp":"...","key":"audits_due-1","task":"audit","instance":0,"value":{"merchant":33,"finding":"clean"}}
    {"stream_type":"event","sim_ts":21228.07052368513,"timestamp":"...","key":"audits_due-2","task":"audit","instance":1,"value":{"path_id":"Compliance.service.audit","sim_id":"Compliance","service":"audit","status":"queued"}}
    {"stream_type":"event","sim_ts":21228.07052368513,"timestamp":"...","key":"audits_due-2","task":"audit","instance":1,"value":{"path_id":"Compliance.service.audit","sim_id":"Compliance","service":"audit","status":"started"}}
    ```

=== "Builder"

    The Builder file builds the same model with the builder API and prints every record of its run:

    ```bash
    python examples/gallery/audits_with_findings_need_a_follow_up_builder.py
    ```

    ```python title="examples/gallery/audits_with_findings_need_a_follow_up_builder.py"
    """An audit that finds an issue needs a follow-up, which waits for the same auditors.

    The Builder form of audits_with_findings_need_a_follow_up.yaml, with the same records.
    An audit of one of 150 merchants is due about every 3 hours and takes an auditor about 4
    hours, and 15 percent find an issue. A follow-up of about an hour goes to the back of
    the same queue.

    Needs: nothing
    Run it with: python examples/gallery/audits_with_findings_need_a_follow_up_builder.py
    """

    import logging
    from datetime import datetime

    from desflow import ConsoleEgress, SimulationContext
    from desflow.values import Template

    UNTIL = 14 * 24 * 3600


    def build() -> SimulationContext:
        """Builds the model."""
        app = (
            SimulationContext(
                sim_id="Compliance",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1),
            )
            .add_egress(ConsoleEgress())
            .add_resource("auditors", current_cap=2, max_cap=2)
            .add_service("audit", dist="normal", mean="4 hours", std="1 hour")
            .add_service("follow_up", dist="normal", mean="1 hour", std="15 min")
            .add_arrival("audits_due", "exponential", rate=0.0001, spawn="audit")
        )

        audit_payload = Template(
            {
                "merchant": {"gen": "integer", "bounds": [1, 150]},
                "finding": {"gen": "weighted", "options": {"clean": 85, "issue": 15}},
            },
            name="tasks.audit",
        )
        follow_up_payload = Template(
            {
                "merchant": {"gen": "upstream", "path": "merchant"},
                "event_type": "follow_up_done",
            },
            name="tasks.follow_up",
        )

        @app.task(
            name="audit",
            service_id="audit",
            resource_id="auditors",
            next={
                "when": [{"condition": "payload.finding == 'issue'", "task": "follow_up"}]
            },
        )
        def audit(task_id):
            return audit_payload

        @app.task(name="follow_up", service_id="follow_up", resource_id="auditors")
        def follow_up(task_id):
            return follow_up_payload

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build().run(until=UNTIL)
    ```

=== "Low-level"

    The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

    ```bash
    python examples/gallery/audits_with_findings_need_a_follow_up_low_level.py
    ```

    The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `tasks.audit.payload.merchant` and `tasks.audit.payload.finding`. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's.

    ```python title="examples/gallery/audits_with_findings_need_a_follow_up_low_level.py"
    """An audit that finds an issue needs a follow-up, which waits for the same auditors.

    The Low-level form of audits_with_findings_need_a_follow_up.yaml, with the same records,
    built on `DynamicRealtimeEnvironment`. An audit of one of 150 merchants is due about
    every 3 hours and takes an auditor about 4 hours, and 15 percent find an issue. A
    follow-up of about an hour goes to the back of the same queue.

    Needs: nothing
    Run it with: python examples/gallery/audits_with_findings_need_a_follow_up_low_level.py
    """

    import itertools
    import logging
    from datetime import datetime

    from desflow import (
        CapacityConfig,
        ConsoleEgress,
        DistributionConfig,
        DynamicRealtimeEnvironment,
        DynamicResource,
        Sampler,
        SimParameter,
    )

    SEED = 42
    UNTIL = 14 * 24 * 3600
    PARAMS = SimParameter(
        sim_id="Compliance",
        arrival={"audits_due": DistributionConfig(dist="exponential", rate=0.0001)},
        # DistributionConfig takes seconds: 4 hours (std 1 hour) for an audit,
        # 1 hour (std 15 min) for a follow-up
        service={
            "audit": DistributionConfig(dist="normal", mean=14400, std=3600),
            "follow_up": DistributionConfig(dist="normal", mean=3600, std=900),
        },
        resources={"auditors": CapacityConfig(current_cap=2, max_cap=2)},
    )


    def weighted(rng, options):
        """One key of `options`, chosen by its weight, as the `weighted` generator does."""
        point = rng.random() * sum(options.values())
        running = 0.0
        for value, weight in options.items():
            running += weight
            if point < running:
                return value
        return value


    def build(env):
        """Starts the audit source on `env`."""
        env.registry.register_sim_parameter(PARAMS)
        sampler = Sampler(seed=SEED)
        merchant = sampler.generator("tasks.audit.payload.merchant")
        finding = sampler.generator("tasks.audit.payload.finding")
        auditors = DynamicResource(env, "Compliance", "auditors")
        instances = {"audit": itertools.count(), "follow_up": itertools.count()}

        def step(key, task, payload):
            """Queues for an auditor, works for the task's service time, then publishes
            what `payload()` gives."""
            path_id = f"Compliance.service.{task}"
            instance = next(instances[task])

            def publish(value):
                env.publish_event(key, value, task=task, instance=instance)

            publish(
                {
                    "path_id": path_id,
                    "sim_id": "Compliance",
                    "service": task,
                    "status": "queued",
                }
            )
            with auditors.request() as request:
                yield request
                publish(
                    {
                        "path_id": path_id,
                        "sim_id": "Compliance",
                        "service": task,
                        "status": "started",
                    }
                )
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                made = payload()
                publish(made)
            return made

        def audit_payload():
            return {
                "merchant": int(merchant.integers(1, 151)),
                "finding": weighted(finding, {"clean": 85, "issue": 15}),
            }

        def audit(key):
            made = yield from step(key, "audit", audit_payload)
            if made["finding"] == "issue":
                yield from step(
                    key,
                    "follow_up",
                    lambda: {"merchant": made["merchant"], "event_type": "follow_up_done"},
                )

        def audits_due():
            gap = env.registry.get_config("Compliance.arrival.audits_due")
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(audit(f"audits_due-{number}"))

        env.process(audits_due())


    def run():
        """Runs the model for 14 simulation days."""
        env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
        env.setup_egress([ConsoleEgress()])
        build(env)
        try:
            env.run(until=UNTIL)
        finally:
            env.teardown()


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        run()
    ```

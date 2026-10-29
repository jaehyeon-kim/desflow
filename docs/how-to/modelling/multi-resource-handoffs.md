# Hand Off to a Resource

A handoff passes an order from one resource to the next without letting go of the first too early. Here a picker carries each order to a packing bench. The picker stays with the order until the bench is free. For a moment the order holds both resources, and then it releases the picker.

![The order holds the picker for 1 s of walking and while it waits for the bench; when it reaches the bench it releases the picker, then holds the bench for 4 s of packing](../../assets/diagrams/picker-handoff.png)

---

## Before You Start

- [Competing Resources](../../learn/shop-logic/competing-resources.md) measures what holding two resources costs.
- [Several Resources](../../user-guide/resources/several-resources.md#several-resources) describes `keep` and the rule on the order of acquisition.

---

## Steps

1. Give the first task the resource and `keep` it: `pick` holds the picker and hands it to the next task.
2. Give the next task the second resource: `pack` waits for the bench while the order still holds the picker, and gives the picker back once it publishes `started`.
3. Acquire the two resources in the same order everywhere. A task that takes the bench first and the picker second would be refused when the model is built, because two orders could then wait for each other for ever.

In the low-level API, `pick` passes its granted request to `pack`, which releases it in place of a `with` block:

=== "YAML"

    ```yaml title="docs/snippets/yaml/multi_resource_handoffs.yaml:tasks"
    # The picker carries the order to the bench and stays with it until it is on the
    # bench: keep hands the picker to the next task instead of releasing it
    pick:
      service: walking
      resource: picker
      keep: [picker]
      payload: {event_type: order_picked}
      next: pack
    # pack gives the picker back as soon as it publishes started
    pack:
      service: packing
      resource: bench
      payload: {event_type: order_packed}
    ```

=== "Builder"

    ```python title="docs/snippets/guides/multi_resource_handoffs.py:tasks"
    # The picker carries the order to the bench and stays with it until it is on the
    # bench: keep hands the picker to the next task instead of releasing it
    @app.task(service_id="walking", resource_id="picker", keep=["picker"], next="pack")
    def pick(task_id: int):
        return {"event_type": "order_picked"}


    # pack gives the picker back as soon as it publishes started
    @app.task(service_id="packing", resource_id="bench")
    def pack(task_id: int):
        return {"event_type": "order_packed"}
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/multi_resource_handoffs_low_level.py:tasks"
    def pick(env, shop, key):
        """Carries the order to the bench, and hands the picker to pack."""
        task_id, path_id = next(shop["ids"]["pick"]), "Shop.service.walking"
        env.publish_event(key, {"path_id": path_id, "status": "queued"}, "pick", task_id)
        picker = shop["picker"].request()
        yield picker
        env.publish_event(key, {"path_id": path_id, "status": "started"}, "pick", task_id)
        yield env.timeout(shop["sampler"].sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"event_type": "order_picked"}, "pick", task_id)
        env.process(pack(env, shop, key, picker))  # the order still holds the picker


    def pack(env, shop, key, picker):
        """Waits for the bench while holding the picker, then gives the picker back."""
        task_id, path_id = next(shop["ids"]["pack"]), "Shop.service.packing"
        env.publish_event(key, {"path_id": path_id, "status": "queued"}, "pack", task_id)
        with shop["bench"].request() as bench:
            yield bench
            env.publish_event(
                key, {"path_id": path_id, "status": "started"}, "pack", task_id
            )
            shop["picker"].release(picker)  # the order is on the bench
            yield env.timeout(shop["sampler"].sample(env.registry.get_config(path_id)))
            env.publish_event(key, {"event_type": "order_packed"}, "pack", task_id)
    ```

---

## Check the Result

Save the files from [Full Files](#full-files) and run `desflow run multi_resource_handoffs.yaml`, `python multi_resource_handoffs.py` or `python multi_resource_handoffs_low_level.py`. Each runs for 30 seconds in real time and prints each event, and the three print the same events, because they use the same seed, 42.

Each order is queued for the picker, picked, queued for the bench and packed, in that order. Orders arrive about once a second, and the one bench takes 4 seconds to pack each, so it can pack only one order every 4 seconds. The bench is therefore the [bottleneck](../../learn/concepts/queueing-basics.md#flow-balance-and-the-bottleneck): an order that waits for the bench keeps the picker, and the orders behind it wait for the picker.

A task that needs both resources for the whole of its work lists them instead, `resource: [picker, bench]`. The `competing-resources` example on [Several Resources](../../user-guide/resources/several-resources.md#example-competing-resources) has both kinds.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/multi_resource_handoffs.yaml"
        # The Hand Off to a Resource guide: a picker carries each order to the packing bench
        # and stays with it until the bench is free.
        # Run it with: desflow run multi_resource_handoffs.yaml
        simulation:
          sim_id: Shop
          factor: 1
          random_seed: 42

        egress:
          - type: Console

        resources:
          picker: {current_cap: 1, max_cap: 1}
          bench: {current_cap: 1, max_cap: 1}

        services:
          # The picker takes 1 s to carry an order to the bench, and packing takes 4 s
          walking: {dist: constant, value: 1 s}
          packing: {dist: constant, value: 4 s}

        arrivals:
          orders: {dist: exponential, rate: 1.0, spawn: pick}

        tasks:
          # The picker carries the order to the bench and stays with it until it is on the
          # bench: keep hands the picker to the next task instead of releasing it
          pick:
            service: walking
            resource: picker
            keep: [picker]
            payload: {event_type: order_picked}
            next: pack
          # pack gives the picker back as soon as it publishes started
          pack:
            service: packing
            resource: bench
            payload: {event_type: order_packed}

        run:
          until: 30 s
        ```

    === "Builder"

        ```python title="docs/snippets/guides/multi_resource_handoffs.py"
        import logging

        from desflow import ConsoleEgress, SimulationContext

        logging.basicConfig(level=logging.INFO)

        app = (
            SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
            .add_resource("picker", current_cap=1, max_cap=1)
            .add_resource("bench", current_cap=1, max_cap=1)
            # The picker takes 1 s to carry an order to the bench, and packing takes 4 s
            .add_service("walking", dist="constant", value=1.0)
            .add_service("packing", dist="constant", value=4.0)
            .add_arrival("orders", dist="exponential", rate=1.0)
            .add_egress(ConsoleEgress())
        )


        # The picker carries the order to the bench and stays with it until it is on the
        # bench: keep hands the picker to the next task instead of releasing it
        @app.task(service_id="walking", resource_id="picker", keep=["picker"], next="pack")
        def pick(task_id: int):
            return {"event_type": "order_picked"}


        # pack gives the picker back as soon as it publishes started
        @app.task(service_id="packing", resource_id="bench")
        def pack(task_id: int):
            return {"event_type": "order_packed"}


        @app.arrival_loop("orders")
        def arrival_loop(context: SimulationContext):
            while True:
                order = yield context.wait_for_arrival("orders")
                context.spawn(pick(context.next_instance("pick"), entity=order))


        if __name__ == "__main__":
            app.run(until=30.0)
        ```

    === "Low-level"

        ```python title="docs/snippets/guides/multi_resource_handoffs_low_level.py"
        """The Hand Off to a Resource guide, low-level API.

        The low-level version of multi_resource_handoffs.py: pick requests the picker and
        passes the granted request to pack, which releases it once the order is on the bench.
        python multi_resource_handoffs_low_level.py runs 30 seconds in real time.
        """

        import itertools
        import logging

        from desflow import (
            CapacityConfig,
            ConsoleEgress,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            Sampler,
            SimParameter,
        )

        logging.basicConfig(level=logging.INFO)

        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=1.0)},
            service={
                # The picker takes 1 s to carry an order to the bench, and packing takes 4 s
                "walking": DistributionConfig(dist="constant", value=1.0),
                "packing": DistributionConfig(dist="constant", value=4.0),
            },
            resources={
                "picker": CapacityConfig(current_cap=1, max_cap=1),
                "bench": CapacityConfig(current_cap=1, max_cap=1),
            },
        )


        def pick(env, shop, key):
            """Carries the order to the bench, and hands the picker to pack."""
            task_id, path_id = next(shop["ids"]["pick"]), "Shop.service.walking"
            env.publish_event(key, {"path_id": path_id, "status": "queued"}, "pick", task_id)
            picker = shop["picker"].request()
            yield picker
            env.publish_event(key, {"path_id": path_id, "status": "started"}, "pick", task_id)
            yield env.timeout(shop["sampler"].sample(env.registry.get_config(path_id)))
            env.publish_event(key, {"event_type": "order_picked"}, "pick", task_id)
            env.process(pack(env, shop, key, picker))  # the order still holds the picker


        def pack(env, shop, key, picker):
            """Waits for the bench while holding the picker, then gives the picker back."""
            task_id, path_id = next(shop["ids"]["pack"]), "Shop.service.packing"
            env.publish_event(key, {"path_id": path_id, "status": "queued"}, "pack", task_id)
            with shop["bench"].request() as bench:
                yield bench
                env.publish_event(
                    key, {"path_id": path_id, "status": "started"}, "pack", task_id
                )
                shop["picker"].release(picker)  # the order is on the bench
                yield env.timeout(shop["sampler"].sample(env.registry.get_config(path_id)))
                env.publish_event(key, {"event_type": "order_packed"}, "pack", task_id)


        def orders(env, shop):
            """Starts pick for every order, keyed orders-1, orders-2 and so on."""
            gap = env.registry.get_config("Shop.arrival.orders")
            for number in itertools.count(1):
                yield env.timeout(shop["sampler"].sample(gap))
                env.process(pick(env, shop, f"orders-{number}"))


        def run(until=30.0, factor=1.0, egress=None):
            env = DynamicRealtimeEnvironment(factor=factor)
            env.registry.register_sim_parameter(PARAMS)
            env.setup_egress(egress or [ConsoleEgress()])
            shop = {
                "picker": DynamicResource(env, "Shop", "picker"),
                "bench": DynamicResource(env, "Shop", "bench"),
                "sampler": Sampler(seed=42),
                "ids": {"pick": itertools.count(), "pack": itertools.count()},
            }
            env.process(orders(env, shop))
            try:
                env.run(until=until)
            finally:
                env.teardown()


        if __name__ == "__main__":
            run()
        ```

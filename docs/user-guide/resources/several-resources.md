# Several Resources

A task can hold more than one [resource](index.md) at once; this page shows how, and an example in which two resources compete.

---

## How to Use It

A task can hold several resources at once: `resource: [picker, bench]` asks for a picker, then, while it holds the picker, for the bench. The task is served once it holds all of them, and it releases all of them when it finishes. While it waits for the bench it holds the picker: the holding is partial, and nothing is granted to several resources at once. `patience` covers the whole acquisition, and an interrupt gives back every unit held.

A task can also hand a unit to the next task of its flow with `keep`: the next task holds it from the start and gives it back once it publishes `started`, so an order keeps the picker until it is on the packing bench. The `competing-resources` example does both:

=== "YAML"

    ```yaml title="examples/yaml/competing_resources.yaml:tasks"
    tasks:
      pick:
        service: picking
        resource: picker
        # The picker stays with the order until pack has started.
        keep: [picker]
        payload: {event_type: order_picked}
        next: pack
      pack:
        service: packing
        resource: bench
        payload: {event_type: order_packed}
      wrap:
        service: wrapping
        # Holds the picker, then the bench too: acquired in this order.
        resource: [picker, bench]
        payload: {event_type: gift_wrapped}
    ```

=== "Builder"

    ```python title="examples/builder/competing_resources_example.py:tasks"
    # The picker stays with the order until pack has started
    @app.task(service_id="picking", resource_id="picker", keep=["picker"], next="pack")
    def pick(task_id, context):
        return {"event_type": "order_picked"}


    @app.task(service_id="packing", resource_id="bench")
    def pack(task_id, context):
        return {"event_type": "order_packed"}


    # Holds the picker, then the bench too: acquired in this order
    @app.task(service_id="wrapping", resource_id=["picker", "bench"])
    def wrap(task_id, context):
        return {"event_type": "gift_wrapped"}
    ```

=== "Low-level"

    ```python title="examples/low-level/competing_resources_example.py:tasks"
    def pick(shop, key):
        """Picks an order and keeps the picker for pack, which gives it back."""
        env, stats = shop.env, shop.env.stats
        task_id, publish, status, duration = shop.step(key, "pick", "picking")
        stats.flow_started("orders")  # the order enters the shop
        status("queued")
        stats.task_queued("pick", task_id)
        picker = shop.picker.request()
        yield picker
        status("started")
        stats.task_started("pick", task_id)
        yield env.timeout(duration())
        publish({"event_type": "order_picked"})
        stats.task_finished("pick", task_id)
        env.process(pack(shop, key, picker))


    def pack(shop, key, picker):
        env, stats = shop.env, shop.env.stats
        task_id, publish, status, duration = shop.step(key, "pack", "packing")
        status("queued")
        stats.task_queued("pack", task_id)
        with shop.bench.request() as bench:
            yield bench
            status("started")
            stats.task_started("pack", task_id)
            shop.picker.release(picker)  # the order is on the bench
            yield env.timeout(duration())
            publish({"event_type": "order_packed"})
            stats.task_finished("pack", task_id)
        stats.flow_finished("orders")


    def wrap(shop, key):
        """Holds the picker, then the bench too: acquired in this order."""
        env, stats = shop.env, shop.env.stats
        task_id, publish, status, duration = shop.step(key, "wrap", "wrapping")
        stats.flow_started("gifts")
        status("queued")
        stats.task_queued("wrap", task_id)
        with shop.picker.request() as picker:
            yield picker
            with shop.bench.request() as bench:
                yield bench
                status("started")
                stats.task_started("wrap", task_id)
                yield env.timeout(duration())
                publish({"event_type": "gift_wrapped"})
                stats.task_finished("wrap", task_id)
        stats.flow_finished("gifts")
    ```

    In the low-level API several resources are nested requests, and a handoff is a request released by the next process.

**One order of acquisition.** Two tasks that acquire the same two resources in opposite orders can each hold one and wait for ever for the other. desflow refuses such a model when it is built, naming both tasks: every task, `keep` handoffs included, must acquire shared resources in one order. [Competing Resources](../../tutorials/competing-resources.md) shows what a partial holding costs.

### Example: Competing Resources

The `competing-resources` example: orders keep the picker until they are on the bench, and gifts hold the picker and the bench together. It needs no Docker service and ends on its own after 120 simulation seconds.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get competing-resources --api yaml
        cd competing-resources/yaml

        # 3. Run it
        desflow run competing_resources.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get competing-resources --api yaml
        cd competing-resources/yaml

        # 3. Run it
        desflow run competing_resources.yaml
        ```

    The whole file is [`examples/yaml/competing_resources.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/competing_resources.yaml).

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get competing-resources --api builder
        cd competing-resources/builder

        # 3. Run it
        uv run --no-project --with desflow competing_resources_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get competing-resources --api builder
        cd competing-resources/builder

        # 3. Run it
        python competing_resources_example.py
        ```

    The whole file is [`examples/builder/competing_resources_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/competing_resources_example.py).

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get competing-resources --api low-level
        cd competing-resources/low-level

        # 3. Run it
        uv run --no-project --with desflow competing_resources_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get competing-resources --api low-level
        cd competing-resources/low-level

        # 3. Run it
        python competing_resources_example.py
        ```

    The whole file is [`examples/low-level/competing_resources_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/competing_resources_example.py).

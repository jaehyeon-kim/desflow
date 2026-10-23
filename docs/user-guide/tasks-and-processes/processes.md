# Processes

A process runs alongside the [tasks](index.md); this page shows how to write one, and an example in which processes and tasks interact.

---

## How to Use It

### A Process

A process runs alongside the tasks. This one sends the packers on a 5-second break every 50 seconds:

=== "YAML"

    ```yaml title="docs/snippets/yaml/tasks.yaml:process"
    processes:
      # Called as breaks(context, resource="packers", every=50, duration=5)
      - function: !python shop_logic.breaks
        kwargs: {resource: packers, every: 50, duration: 5}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/tasks.py:builder-process"
    def breaks(context: SimulationContext, resource: str, every: float, duration: float):
        """Takes every packer off for a break of `duration` seconds every `every` seconds."""
        path = f"{context.sim_id}.resources.{resource}.current_cap"
        registry = context.env.registry
        while True:
            yield context.env.timeout(every)
            normal = registry.get(path).value
            registry.update(path, 0)
            yield context.env.timeout(duration)
            registry.update(path, normal)


    app.add_process(breaks, resource="packers", every=50, duration=5)
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/tasks.py:low-level-process"
    def breaks_process(env, resource: str, every: float, duration: float):
        """What add_process starts, by hand: a plain SimPy generator."""
        path = f"Shop.resources.{resource}.current_cap"
        while True:
            yield env.timeout(every)
            normal = env.registry.get(path).value
            env.registry.update(path, 0)
            yield env.timeout(duration)
            env.registry.update(path, normal)
    ```

Setting `current_cap` to 0 stops new work but not the work in progress: [Resources](../resources/index.md) explains what happens to busy units when a capacity drops.

---

## Stability and Limits

The limits of this page are listed with those of the whole section, in [Stability and Limits](index.md#stability-and-limits) on the Tasks and Processes page.

---

## Example: Process Interaction

The `process-interaction` example puts these together: a child flow, a failure that `on_fail` handles, and orders that wait for a courier's signal or a time limit. It needs no Docker service and ends on its own after 300 simulation seconds.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get process-interaction --api yaml
        cd process-interaction/yaml

        # 3. Run it
        desflow run process_interaction.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get process-interaction --api yaml
        cd process-interaction/yaml

        # 3. Run it
        desflow run process_interaction.yaml
        ```

    The whole file is [`examples/yaml/process_interaction.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/process_interaction.yaml).

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get process-interaction --api builder
        cd process-interaction/builder

        # 3. Run it
        uv run --no-project --with desflow process_interaction_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get process-interaction --api builder
        cd process-interaction/builder

        # 3. Run it
        python process_interaction_example.py
        ```

    The whole file is [`examples/builder/process_interaction_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/process_interaction_example.py).

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get process-interaction --api low-level
        cd process-interaction/low-level

        # 3. Run it
        uv run --no-project --with desflow process_interaction_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get process-interaction --api low-level
        cd process-interaction/low-level

        # 3. Run it
        python process_interaction_example.py
        ```

    The whole file is [`examples/low-level/process_interaction_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/process_interaction_example.py).

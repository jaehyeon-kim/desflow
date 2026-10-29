# Processes

A process runs alongside the [tasks](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/index.md). This page shows how to write one, and an example in which processes and tasks interact.

______________________________________________________________________

## What It Is

A process is anything in the shop that runs over time but is not one entity's work: a break for the packers, a drift in a parameter, a delivery schedule. An arrival starts one task per entity, while a process starts once, when the run starts, and runs alongside the tasks.

______________________________________________________________________

## How to Use It

### A Process

This process sends the packers on a 5-second break every 50 seconds:

```yaml
processes:
  # Called as breaks(context, resource="packers", every=50, duration=5)
  - function: !python shop_logic.breaks
    kwargs: {resource: packers, every: 50, duration: 5}
```

```python
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

```python
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

Setting `current_cap` to 0 stops new work but not the work in progress: [Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/index.md) explains what happens to busy units when a capacity drops.

______________________________________________________________________

## How It Works

`add_process(func, **kwargs)` starts `func(context, **kwargs)` when the run starts, and a YAML `processes` entry does the same with a `!python` function. Inside it, `context.env` is the `DynamicRealtimeEnvironment`, so a process can do anything a low-level script does: `context.env.timeout`, `context.env.registry.update`, `context.env.publish_event`. A process is how low-level code joins a builder or YAML model.

`context.env.active_process` is the running SimPy process. Its `name` is the task's name, because a task's process is named after the task. Its `target` is the event it waits on now, or None once it has ended. A condition reads the same three values as `process.name`, `process.instance` (the task's instance id, the `instance` field of its records) and `process.target`, a string such as `timeout`, `signal.<name>`, `task.<name>`, `condition`, `resource.<name>`, `get.<name>`, `put.<name>` or `service`. In a task's own expressions they name that task. In an `interrupt` or `running` `where`, they name each instance tested.

______________________________________________________________________

## Stability and Limits

The limits of this page are listed with those of the whole section, in [Stability and Limits](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#stability-and-limits) on the Tasks and Processes page.

______________________________________________________________________

## Example: Process Interaction

The `process-interaction` example combines a child flow, a failure that `on_fail` handles, and orders that wait for a courier's signal or a time limit. It needs no Docker service and ends on its own after 300 simulation seconds.

The tutorial page [Process Interaction](https://jaehyeon.me/desflow/latest/learn/shop-logic/process-interaction/index.md) teaches these features step by step.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get process-interaction --api yaml
cd process-interaction/yaml

# 3. Find the bottleneck, with no run
desflow explain process_interaction.yaml

# 4. Run it
desflow run process_interaction.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get process-interaction --api yaml
cd process-interaction/yaml

# 3. Find the bottleneck, with no run
desflow explain process_interaction.yaml

# 4. Run it
desflow run process_interaction.yaml
```

The whole file is [`examples/yaml/process_interaction.yaml`](https://github.com/jaehyeon-kim/desflow/blob/v0.26.0/examples/yaml/process_interaction.yaml).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get process-interaction --api builder
cd process-interaction/builder

# 3. Run it
uv run --no-project --with desflow process_interaction_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get process-interaction --api builder
cd process-interaction/builder

# 3. Run it
python process_interaction_example.py
```

The whole file is [`examples/builder/process_interaction_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v0.26.0/examples/builder/process_interaction_example.py).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get process-interaction --api low-level
cd process-interaction/low-level

# 3. Run it
uv run --no-project --with desflow process_interaction_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get process-interaction --api low-level
cd process-interaction/low-level

# 3. Run it
python process_interaction_example.py
```

The whole file is [`examples/low-level/process_interaction_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v0.26.0/examples/low-level/process_interaction_example.py).

# Processes

A process runs alongside the [tasks](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/index.md). This page shows how to write one.

______________________________________________________________________

## What It Is

A process is anything in the shop that runs over time but is not one entity's work: a break for the packers, a drift in a parameter, a delivery schedule. An arrival starts one task per entity, while a process starts once, when the run starts, and runs alongside the tasks. A process can wait, change values in the registry, publish records or start tasks.

______________________________________________________________________

## How to Use It

### A Process

This process sends the packers on a 5-second break every 50 seconds:

A blueprint writes this without Python. One entity exists when the run starts, and two tasks hand it to each other: each waits, writes the capacity with an `on:` action, and names the other as its `next`.

```yaml
# One entity exists when the run starts and drives the breaks
shifts: {initial: 1, spawn: take_break}
```

```yaml
# The packers go on a 5-second break every 50 seconds: wait, set the capacity with
# on:, and hand over to the other task, which sets it back
take_break:
  wait: {timeout: 50 s}
  on:
    - {path: Shop.resources.packers.current_cap, value: 0}
  payload: {event_type: break_started}
  next: end_break
end_break:
  wait: {timeout: 5 s}
  on:
    - {path: Shop.resources.packers.current_cap, value: 1}
  payload: {event_type: break_ended}
  next: take_break
```

A `processes` entry names a Python function of your own, for work the keys cannot say:

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

______________________________________________________________________

## How It Works

`add_process(func, **kwargs)` starts `func(context, **kwargs)` when the run starts, and a YAML `processes` entry does the same with a `!python` function. Inside it, `context.env` is the `DynamicRealtimeEnvironment`, so a process can do anything a low-level script does: `context.env.timeout`, `context.env.registry.update`, `context.env.publish_event`. A process is how low-level code joins a builder or YAML model.

`context.env.active_process` is the running SimPy process. Its `name` is the task's name, because a task's process is named after the task. Its `target` is the event it waits on now, or None once it has ended. A condition reads the same three values as `process.name`, `process.instance` (the task's instance id, the `instance` field of its records) and `process.target`, a string such as `timeout`, `signal.<name>`, `task.<name>`, `condition`, `resource.<name>`, `get.<name>`, `put.<name>` or `service`. In a task's own expressions they name that task. In an `interrupt` or `running` `where`, they name each instance tested.

______________________________________________________________________

## Stability and Limits

- **A process is not an entity.** It has no key, no `instance` and no records of its own unless it publishes them. Work that one entity does is a [task](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/index.md).
- **A capacity of 0 stops new work only.** Setting `current_cap` to 0 does not stop the work in progress: [Units and Queues](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#under-live-changes) explains what happens to busy units when a capacity drops.

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

The whole file is [`examples/yaml/process_interaction.yaml`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc3/examples/yaml/process_interaction.yaml).

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

The whole file is [`examples/builder/process_interaction_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc3/examples/builder/process_interaction_example.py).

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

The whole file is [`examples/low-level/process_interaction_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc3/examples/low-level/process_interaction_example.py).

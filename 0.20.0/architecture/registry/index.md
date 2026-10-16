# Live Parameters: `SimulationRegistry`

Every run has one `SimulationRegistry`, at `env.registry`. It maps dot-notation paths, such as `Shop.resources.packers.current_cap`, to the values the simulation reads. An ingress connector, a scenario or a process changes a parameter by updating its path, and the simulation follows the change without restarting.

______________________________________________________________________

## Registry Paths

`run()` flattens the configuration into the registry, and every path an ingress message, a scenario or `registry.get` names has one of these forms:

| Builder call                                          | Paths                                                                                                                                                                                       |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `add_arrival(name, ...)`, `add_service(name, ...)`    | `<sim_id>.arrival.<name>.rate` and `<sim_id>.service.<name>.rate` for an exponential distribution, `.value` for a constant, otherwise `.mean` and `.std`, and `.low` for a truncated normal |
| `add_resource(name, ...)`, `add_container(name, ...)` | `<sim_id>.resources.<name>.current_cap` and `.max_cap`, `<sim_id>.containers.<name>.current_cap` and `.max_cap`                                                                             |
| `add_variable(name, value)`                           | `<sim_id>.variables.<name>`                                                                                                                                                                 |

Stores, registered through `SimParameter(stores=...)` on the low-level API, take `<sim_id>.stores.<name>.current_cap` and `.max_cap`. An update to a path that does not exist is logged as a warning and ignored.

`compile_parameters()` on a `SimulationContext` returns the `SimParameter` that `run()` registers, so the paths of a configuration can be checked before the run.

docs/snippets/pages/registry.py:paths

```python
params = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="normal", mean=3.0, std=0.5)},
    resources={"packers": CapacityConfig(current_cap=2, max_cap=5)},
    variables={"conveyor_speed": 1.5},
)
env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(params)
# Registered paths include:
#   Shop.arrival.orders.rate
#   Shop.service.packing.mean, Shop.service.packing.std
#   Shop.resources.packers.current_cap, Shop.resources.packers.max_cap
#   Shop.variables.conveyor_speed
print(env.registry.get("Shop.resources.packers.current_cap").value)  # 2
```

______________________________________________________________________

## How an Update Is Applied

`env.registry.update(path, value)` applies one change. The checks run in order, and an accepted value reaches the parts that read it:

- A path that does not exist is logged as a warning, and the update is ignored.
- A value of a different type is converted to the type the path holds, so the string `"5"` sent to an integer path becomes `5`. The conversion is Python's own, so `2.5` sent to an integer path becomes `2`. A value that cannot be converted is logged as an error, and the update is ignored.
- A distribution parameter outside its range, such as a negative `std` or a rate of 0, is logged as an error, and the update is ignored. The rules are the ones checked at load, listed under [`dist`](https://jaehyeon.me/desflow/latest/architecture/yaml/#sections).
- A `current_cap` below 0 or above the path's `max_cap` is stored as 0 or `max_cap`, with a warning.
- A value equal to the current one changes nothing.
- Otherwise the value is replaced, and the configuration object the path belongs to is updated too, such as the `rate` of a `DistributionConfig` or an entry of the variables.

Ingress connectors run on a background thread, so they do not call `update` themselves. Each puts a `(path, value)` pair on a queue, and a SimPy process started by `setup_ingress` applies everything queued every 0.1 simulation seconds. At `factor=1.0` an update therefore lands within 0.1 seconds of arriving. The simulation time it lands at depends on when it arrives, so a change that must happen at an exact simulation time is a scenario.

docs/snippets/pages/registry.py:update

```python
env.registry.update("Shop.resources.packers.current_cap", 3)  # applied
env.registry.update("Shop.resources.packers.current_cap", 9)  # stored as 5, the max_cap
env.registry.update("Shop.service.packing.std", -1.0)  # refused, the old 0.5 stays
env.registry.update("Shop.resources.lath.current_cap", 3)  # no such path: ignored
```

______________________________________________________________________

## What Follows a Change

- **Arrival and service distributions.** A `Sampler` reads the configuration object each time it draws, so the next draw uses the new value. `context.wait_for_arrival` draws when it is called, so the arrival already being waited for keeps its old gap. `@app.task` draws the service time once the resource is acquired, so a task still in the queue uses the new value.
- **Resource capacity.** A `DynamicResource` follows `current_cap`, rounded down to a whole number and limited to between 0 and the `max_cap` the registry holds. A change to `max_cap` applies `current_cap` again under the new limit. [Resources](https://jaehyeon.me/desflow/latest/building-blocks/resources/#shrinking-safety-guarantee) describes what happens when the capacity shrinks below the number of tasks in progress.
- **Container capacity.** A `DynamicContainer` follows `current_cap`, fractions included even when the starting value is a whole number, limited to between 0 and the `max_cap` the registry holds. A change to `max_cap` applies `current_cap` again under the new limit. A `DynamicStore` does the same with whole numbers.
- **Variables.** The new value is stored. A process reads it with `env.registry.get(path).value`.

docs/snippets/pages/registry.py:follow

```python
packing = env.registry.get_config("Shop.service.packing")
print(packing.mean)  # 3.0: the Sampler reads this object on its next draw
env.registry.update("Shop.service.packing.mean", 4.0)
print(packing.mean)  # 4.0: the same object, updated in place
```

______________________________________________________________________

## Sources of Updates

Each source lands its change at a different moment:

- **Ingress connectors**: Kafka, Redis, PostgreSQL and `LocalIngress`. [Connectors](https://jaehyeon.me/desflow/latest/architecture/connectors/#ingress-connectors-inputs) gives the message each one expects.
- **A scenario** in a YAML blueprint, compared with `LocalIngress` below.
- **A process**, which calls `env.registry.update(path, value)` directly, as the breaks process in [Python in a Blueprint](https://jaehyeon.me/desflow/latest/guides/yaml-advanced/#processes-payloads-and-kwargs) does.

### Scenarios versus `LocalIngress`

A [YAML blueprint](https://jaehyeon.me/desflow/latest/architecture/yaml/index.md) can carry a `scenario`: a list of registry changes, each with the simulation time it applies at, such as `{at: 10, path: Shop.resources.packers.current_cap, value: 3}`. [Script an experiment](https://jaehyeon.me/desflow/latest/guides/yaml-blueprints/#script-an-experiment) shows a complete file.

docs/snippets/yaml/scenario.yaml

```yaml
# The first blueprint with a scripted experiment: a second packer starts at 30 s,
# orders arrive twice as fast from 1 min, and the extra packer leaves at
# 2 min. Every change lands at exactly that simulation time, on every run.
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 1

egress:
  - type: Console

resources:
  packers: {current_cap: 1, max_cap: 3}

services:
  packing: {dist: normal, mean: 4.0, std: 1.0}

arrivals:
  orders: {dist: exponential, rate: 0.2, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {status: finished}
    id_field: order_id

telemetry:
  # Shows each change as it lands.
  - interval: 10
    publish:
      packers.capacity: packers.capacity
      packers.queue_length: packers.queue_length

scenario:
  - {at: 30, path: Shop.resources.packers.current_cap, value: 2}
  - {at: 1 min, path: Shop.arrival.orders.rate, value: 0.4}
  - {at: 2 min, path: Shop.resources.packers.current_cap, value: 1}

run:
  until: 3 min
```

A scenario is not a connector. It is compiled into a SimPy process that waits on the simulation clock, so each change lands at exactly its `at`, on every run and at any `factor`, including `factor=0`. Every path is checked against the registry when the file is loaded, so a misspelt path stops the load with its line number. `LocalIngress` waits on the wall clock instead, and an unknown path is only logged as a warning when it is due.

A scenario and an ingress connector can be used together. Both write to the same registry, so a scripted baseline can run while an operator steers over Kafka. When both change the same path, the later write wins.

______________________________________________________________________

## Waiting for a Change

`env.registry.get(path).wait_for_change()` returns a SimPy event that fires when the value at that path changes. Each value has one change signal, and the first process waiting takes it. `DynamicResource`, `DynamicContainer` and `DynamicStore` already wait on their `current_cap` and `max_cap`, so wait only on paths that nothing else waits on, such as variables.

docs/snippets/pages/registry.py:wait

```python
def watch_speed(env):
    """Logs the conveyor speed every time it changes."""
    speed = env.registry.get("Shop.variables.conveyor_speed")
    while True:
        yield speed.wait_for_change()
        print(f"t={env.now}: conveyor speed is now {speed.value}")


env.process(watch_speed(env))
```

# Add Resources Mid-run

Sometimes the layout itself changes during a run: a dock opens a new bay, or a shop adds a gift wrapping station. Here a supervisor process adds gift wrappers at 12 seconds, and from then on new jobs go to the wrappers as well as the packers.

A blueprint and the builder create their resources before the clock starts and have no setting that adds one later. A process can add one, because it has the environment. A low-level script adds one the same way.

______________________________________________________________________

## Before You Start

- [Low-level API](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/low-level-api/index.md) explains how to register parameters and create resources.
- [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/index.md) explains the registry paths a resource reads.
- [Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/index.md) shows how a blueprint and the builder run a process.

______________________________________________________________________

## Steps

1. Keep the resources that exist in a mapping, so the process that starts new work also finds a resource added later.
1. When the layout changes, register a `SimParameter` that holds only the new resource. The registry adds its paths, here `Shop.resources.wrappers.current_cap` and `Shop.resources.wrappers.max_cap`.
1. Create the `DynamicResource` for it, which reads its capacity from those paths, and add it to the mapping.
1. Start work on it as on any other resource.

In a blueprint and in the builder, the supervisor is a process that receives the context, and `context.env` is the environment. In the low-level API it is a SimPy process of the script:

```yaml
simulation:
  sim_id: Shop
  factor: 1
  strict: false

egress:
  - type: Console

resources:
  packers: {current_cap: 1, max_cap: 5}

processes:
  # Called as supervisor(context); it adds the wrappers during the run
  - function: !python dynamic_topology_logic.supervisor

run:
  until: 20 s
```

`dynamic_topology_logic.py` beside it holds the supervisor:

```python
def supervisor(context):
    """Starts a job on every resource each round, and adds the wrappers at 12 s."""
    env = context.env  # the environment, as a low-level script has it
    resources = {"packers": context.get_resource("packers")}
    job_id = 0
    while True:
        yield env.timeout(NEW_JOBS_EVERY)
        if env.now >= ADD_WRAPPERS_AT and "wrappers" not in resources:
            env.registry.register_sim_parameter(
                SimParameter(
                    sim_id="Shop",
                    resources={"wrappers": CapacityConfig(current_cap=2, max_cap=5)},
                )
            )
            resources["wrappers"] = DynamicResource(env, "Shop", "wrappers")
            env.publish_event(
                "supervisor",
                {
                    "status": "wrappers_added",
                    "capacity": resources["wrappers"].capacity,
                },
            )
        for name in list(resources):
            env.process(worker(env, resources, name, job_id))
        job_id += 1
```

```python
app = (
    SimulationContext(sim_id="Shop", factor=1.0, strict=False)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=1, max_cap=5)
    # Called as supervisor(context); it adds the wrappers during the run
    .add_process(supervisor)
)
```

`supervisor` is the function in `dynamic_topology_logic.py`, which the YAML tab shows.

```python
# Give the gift wrappers their registry paths, then create the resource
# that reads its capacity from them.
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop",
        resources={"wrappers": CapacityConfig(current_cap=2, max_cap=5)},
    )
)
resources["wrappers"] = DynamicResource(env, "Shop", "wrappers")
```

______________________________________________________________________

## Check the Result

Run `desflow run dynamic_topology.yaml`, `python dynamic_topology_builder.py` or `python dynamic_topology.py`. Each runs for 20 seconds in real time, and the three print the same events:

- Every 4 seconds the supervisor starts one job on each resource it has. Each job publishes `queued`, `started` and `finished` with the resource it used, and takes 3 seconds.
- At 12 seconds the supervisor publishes `{"status": "wrappers_added", "capacity": 2}`, and `wrappers-2` starts in the same round.
- Each run ends with `env.teardown()`, which writes the records still in the buffers. `run()` calls it for the blueprint and the builder.

The wrappers' capacity is live like any other. A scenario, a connector or a process can change `Shop.resources.wrappers.current_cap` once the path exists, as [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/index.md) describes.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Add Resources Mid-run guide: a supervisor adds gift wrappers at 12 seconds.
# Run it with: desflow run dynamic_topology.yaml, beside dynamic_topology_logic.py
simulation:
  sim_id: Shop
  factor: 1
  strict: false

egress:
  - type: Console

resources:
  packers: {current_cap: 1, max_cap: 5}

processes:
  # Called as supervisor(context); it adds the wrappers during the run
  - function: !python dynamic_topology_logic.supervisor

run:
  until: 20 s
```

```python
"""The Add Resources Mid-run guide: the supervisor process of the blueprint and the
builder, which adds the gift wrappers during the run.

dynamic_topology.yaml names `supervisor` with !python, and dynamic_topology_builder.py
adds it with add_process.
"""

from desflow import CapacityConfig, DynamicResource, SimParameter

WORK = 3.0  # seconds each job takes
NEW_JOBS_EVERY = 4.0  # seconds between rounds of new jobs
ADD_WRAPPERS_AT = 12.0  # simulation time at which the gift wrappers are added


def worker(env, resources, name, job_id):
    resource = resources[name]
    key = f"{name}-{job_id}"
    env.publish_event(key, {"resource": name, "status": "queued"})
    with resource.request() as req:
        yield req
        env.publish_event(key, {"resource": name, "status": "started"})
        yield env.timeout(WORK)
        env.publish_event(key, {"resource": name, "status": "finished"})


def supervisor(context):
    """Starts a job on every resource each round, and adds the wrappers at 12 s."""
    env = context.env  # the environment, as a low-level script has it
    resources = {"packers": context.get_resource("packers")}
    job_id = 0
    while True:
        yield env.timeout(NEW_JOBS_EVERY)
        if env.now >= ADD_WRAPPERS_AT and "wrappers" not in resources:
            env.registry.register_sim_parameter(
                SimParameter(
                    sim_id="Shop",
                    resources={"wrappers": CapacityConfig(current_cap=2, max_cap=5)},
                )
            )
            resources["wrappers"] = DynamicResource(env, "Shop", "wrappers")
            env.publish_event(
                "supervisor",
                {
                    "status": "wrappers_added",
                    "capacity": resources["wrappers"].capacity,
                },
            )
        for name in list(resources):
            env.process(worker(env, resources, name, job_id))
        job_id += 1
```

```python
"""The Add Resources Mid-run guide, builder API: a supervisor adds gift wrappers at
12 seconds.

Run it with: python dynamic_topology_builder.py, beside dynamic_topology_logic.py
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from dynamic_topology_logic import supervisor

logging.basicConfig(level=logging.INFO)

app = (
    SimulationContext(sim_id="Shop", factor=1.0, strict=False)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=1, max_cap=5)
    # Called as supervisor(context); it adds the wrappers during the run
    .add_process(supervisor)
)

if __name__ == "__main__":
    app.run(until=20.0)
```

It imports `supervisor` from `dynamic_topology_logic.py`, which the YAML tab shows.

```python
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

logging.basicConfig(level=logging.INFO)

WORK = 3.0  # seconds each job takes
NEW_JOBS_EVERY = 4.0  # seconds between rounds of new jobs
ADD_WRAPPERS_AT = 12.0  # simulation time at which the gift wrappers are added

PARAMS = SimParameter(
    sim_id="Shop",
    resources={"packers": CapacityConfig(current_cap=1, max_cap=5)},
)


def worker(env, resources, name, job_id):
    resource = resources[name]
    key = f"{name}-{job_id}"
    env.publish_event(key, {"resource": name, "status": "queued"})
    with resource.request() as req:
        yield req
        env.publish_event(key, {"resource": name, "status": "started"})
        yield env.timeout(WORK)
        env.publish_event(key, {"resource": name, "status": "finished"})


def supervisor(env, resources):
    job_id = 0
    while True:
        yield env.timeout(NEW_JOBS_EVERY)

        if env.now >= ADD_WRAPPERS_AT and "wrappers" not in resources:
            # Give the gift wrappers their registry paths, then create the resource
            # that reads its capacity from them.
            env.registry.register_sim_parameter(
                SimParameter(
                    sim_id="Shop",
                    resources={"wrappers": CapacityConfig(current_cap=2, max_cap=5)},
                )
            )
            resources["wrappers"] = DynamicResource(env, "Shop", "wrappers")
            env.publish_event(
                "supervisor",
                {
                    "status": "wrappers_added",
                    "capacity": resources["wrappers"].capacity,
                },
            )

        # One new job for every resource that exists now.
        for name in list(resources):
            env.process(worker(env, resources, name, job_id))
        job_id += 1


def run(until=20.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor, strict=False)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])

    resources = {"packers": DynamicResource(env, "Shop", "packers")}
    env.process(supervisor(env, resources))
    try:
        env.run(until=until)
    finally:
        env.teardown()  # flushes the buffers and stops the threads
    return resources


if __name__ == "__main__":
    run()
```

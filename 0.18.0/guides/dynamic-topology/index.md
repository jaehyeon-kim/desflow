# Dynamic Topology: Resources Changed Mid-run

Sometimes the layout itself changes during a run: a dock opens a new bay, or a line adds a machine. Here a supervisor process adds a molder at 12 seconds, and from then on new jobs go to the molder as well as the lathe.

The builder (`SimulationContext`) creates every resource before the clock starts and has no call to add one later. A resource added during the run is written with the [low-level API](https://jaehyeon.me/dynamic-des/latest/architecture/low-level/index.md): register its parameters, then create the `DynamicResource` that reads them.

______________________________________________________________________

## Example

docs/snippets/guides/dynamic_topology.py

```python
import logging

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

logging.basicConfig(level=logging.INFO)

WORK = 3.0  # seconds each job takes
NEW_JOBS_EVERY = 4.0  # seconds between rounds of new jobs
ADD_MOLDER_AT = 12.0  # simulation time at which the molder is added

PARAMS = SimParameter(
    sim_id="Line_A",
    resources={"lathe": CapacityConfig(current_cap=1, max_cap=5)},
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

        if env.now >= ADD_MOLDER_AT and "molder" not in resources:
            # Give the molder its registry paths, then create the resource that
            # reads its capacity from them.
            env.registry.register_sim_parameter(
                SimParameter(
                    sim_id="Line_A",
                    resources={"molder": CapacityConfig(current_cap=2, max_cap=5)},
                )
            )
            resources["molder"] = DynamicResource(env, "Line_A", "molder")
            env.publish_event(
                "supervisor",
                {"status": "molder_added", "capacity": resources["molder"].capacity},
            )

        # One new job for every resource that exists now.
        for name in list(resources):
            env.process(worker(env, resources, name, job_id))
        job_id += 1


def run(until=20.0, factor=1.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])

    resources = {"lathe": DynamicResource(env, "Line_A", "lathe")}
    env.process(supervisor(env, resources))
    try:
        env.run(until=until)
    finally:
        env.teardown()  # flushes the buffers and stops the threads
    return resources


if __name__ == "__main__":
    run()
```

Save it as `dynamic_topology.py` and run `python dynamic_topology.py`. It runs for 20 seconds in real time (`factor=1.0`) and prints each event.

______________________________________________________________________

## How It Works

- **`PARAMS`** registers only the lathe when the run starts.
- **`supervisor`** wakes every `NEW_JOBS_EVERY` seconds. At `ADD_MOLDER_AT` it calls `env.registry.register_sim_parameter` with a `SimParameter` that holds only the molder, which adds the paths `Line_A.resources.molder.current_cap` and `Line_A.resources.molder.max_cap`. It then creates `DynamicResource(env, "Line_A", "molder")`, which reads its capacity from those paths, and publishes `molder_added`.
- **New jobs** are started for every resource in `resources`, so the molder gets jobs from the round in which it is added.
- **`worker`** is one job: it publishes `queued`, `started` and `finished` with the resource it used.
- **`run`** creates the environment, starts the supervisor and calls `env.teardown()` at the end, which writes the records still in the buffers.

The molder's capacity is live like any other. A scenario, a connector or a process can change `Line_A.resources.molder.current_cap` once the path exists, as [Live Parameters](https://jaehyeon.me/dynamic-des/latest/architecture/registry/index.md) describes.

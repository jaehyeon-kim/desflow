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


# --8<-- [start:supervisor]
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


# --8<-- [end:supervisor]

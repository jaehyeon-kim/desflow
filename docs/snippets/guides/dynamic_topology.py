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

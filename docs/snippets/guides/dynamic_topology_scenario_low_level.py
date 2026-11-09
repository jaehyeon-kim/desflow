"""The Add Resources Mid-run guide, low-level API: the gift wrappers exist from the
start with no units, and `run_scenario` gives them 2 units at 12 seconds.

`build(env)` starts the processes, and `run()` runs them for 20 seconds.
"""

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
    run_scenario,
)

UNTIL = 20
# --8<-- [start:scenario]
PARAMS = SimParameter(
    sim_id="Shop",
    resources={
        "packers": CapacityConfig(current_cap=1, max_cap=5),
        # No units yet: a job that asks for one waits
        "wrappers": CapacityConfig(current_cap=0, max_cap=5),
    },
)
# At 12 seconds the wrappers get 2 units, and the step records what it did
SCHEDULE = [
    {
        "at": 12,
        "path": "Shop.resources.wrappers.current_cap",
        "value": 2,
        "record": {"status": "wrappers_added", "capacity": 2},
    }
]
# --8<-- [end:scenario]


def build(env: DynamicRealtimeEnvironment) -> None:
    resources = {
        "packers": DynamicResource(env, "Shop", "packers"),
        "wrappers": DynamicResource(env, "Shop", "wrappers"),
    }

    def job(key, name, task, task_id):
        env.publish_event(key, {"status": "queued"}, task=task, instance=task_id)
        with resources[name].request() as request:
            yield request
            env.publish_event(key, {"status": "started"}, task=task, instance=task_id)
            yield env.timeout(3)
            env.publish_event(
                key,
                {"resource": name, "status": "finished"},
                task=task,
                instance=task_id,
            )

    def rounds(arrival, name, task, first):
        yield env.timeout(first)
        number = 0
        while True:
            env.process(job(f"{arrival}-{number + 1}", name, task, number))
            number += 1
            yield env.timeout(4)

    env.process(rounds("pack_rounds", "packers", "pack_job", 4))
    env.process(rounds("wrap_rounds", "wrappers", "wrap_job", 12))
    env.process(run_scenario(env, SCHEDULE, until=UNTIL))


def run(egress=None):
    env = DynamicRealtimeEnvironment(factor=0, strict=False)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress(egress or [ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()

"""DX-8: A resource added during the run and a job on every resource each round.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. `run_scenario` gives the wrappers their units at 12 s.
"""

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
    run_scenario,
)

UNTIL = 20
PARAMS = SimParameter(
    sim_id="Shop",
    resources={
        "packers": CapacityConfig(current_cap=1, max_cap=5),
        "wrappers": CapacityConfig(current_cap=0, max_cap=5),
    },
)
SCHEDULE = [{"at": 12, "path": "Shop.resources.wrappers.current_cap", "value": 2}]


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

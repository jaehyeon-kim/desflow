"""GA-2: A machine slows down as it wears, then is repaired.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. `run_scenario` applies each change at its time.
"""

import itertools

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    run_scenario,
)

UNTIL = 18000
PARAMS = SimParameter(
    sim_id="Press",
    arrival={"blanks": DistributionConfig(dist="exponential", rate=0.05)},
    service={"stamp": DistributionConfig(dist="normal", mean=10, std=1)},
    resources={"station": CapacityConfig(current_cap=1)},
)
SCHEDULE = [
    {"at": 3600, "path": "Press.service.stamp.mean", "value": 12},
    {"at": 7200, "path": "Press.service.stamp.mean", "value": 14},
    {"at": 10800, "path": "Press.service.stamp.mean", "value": 16},
    {"at": 14400, "path": "Press.service.stamp.mean", "value": 10},
]


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    station = DynamicResource(env, "Press", "station")
    gap = env.registry.get_config("Press.arrival.blanks")
    gap.stream = "arrival.blanks"  # the random sequence the blueprint's draw uses
    service = env.registry.get_config("Press.service.stamp")
    service.stream = "service.stamp"

    def blank(key, task_id):
        path_id = "Press.service.stamp"
        env.publish_event(
            key,
            {"path_id": path_id, "status": "queued"},
            task="blank",
            instance=task_id,
        )
        with station.request() as request:
            yield request
            env.publish_event(
                key,
                {"path_id": path_id, "status": "started"},
                task="blank",
                instance=task_id,
            )
            yield env.timeout(sampler.sample(service))
            env.publish_event(
                key, {"event_type": "stamped"}, task="blank", instance=task_id
            )

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(blank(f"blanks-{number}", number - 1))

    env.process(source())
    env.process(run_scenario(env, SCHEDULE, until=UNTIL))

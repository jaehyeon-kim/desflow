"""A machine slows down as it wears, then is repaired, low-level API.

The low-level version of `machine_slows_as_it_wears.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. The wear
is a process that sets the mean at the times of the steps.

Needs: nothing
Run it with: python examples/gallery/machine_slows_as_it_wears_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    run_scenario,
)

UNTIL = 18000
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
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
    """Starts the arrivals and the scenario on `env`."""
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
            {
                "path_id": path_id,
                "sim_id": "Press",
                "service": "stamp",
                "status": "queued",
            },
            task="blank",
            instance=task_id,
        )
        with station.request() as request:
            yield request
            env.publish_event(
                key,
                {
                    "path_id": path_id,
                    "sim_id": "Press",
                    "service": "stamp",
                    "status": "started",
                },
                task="blank",
                instance=task_id,
            )
            yield env.timeout(sampler.sample(service))
            env.publish_event(
                key,
                {"event_type": "stamped"},
                task="blank",
                instance=task_id,
            )

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(blank(f"blanks-{number}", number - 1))

    env.process(source())
    env.process(run_scenario(env, SCHEDULE, until=UNTIL))


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

"""Callers hang up when they wait for an agent for longer than they will tolerate.

The Low-level form of callers_hang_up_after_waiting_too_long.yaml, with the same
records, built on `DynamicRealtimeEnvironment`. Calls arrive at 54 an hour and two
agents answer about 30 an hour, so a queue builds. A caller who waits longer than their
patience, 1 to 5 minutes, hangs up. A scenario step adds two agents after half an hour,
so calls wait less and fewer callers hang up. A process adds them at 30 minutes,
through the registry.

Needs: nothing
Run it with: python examples/gallery/callers_hang_up_after_waiting_too_long_low_level.py
"""

import itertools
import logging
from datetime import datetime

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

SEED = 42
UNTIL = 3600
PARAMS = SimParameter(
    sim_id="Centre",
    arrival={"calls": DistributionConfig(dist="exponential", rate=0.015)},
    # DistributionConfig takes seconds: 4 min (std 1 min) for a call
    service={"talk": DistributionConfig(dist="normal", mean=240, std=60)},
    resources={"agents": CapacityConfig(current_cap=2, max_cap=4)},
)


def build(env):
    """Starts the call source and the shift change on `env`."""
    env.registry.register_sim_parameter(PARAMS)
    sampler = Sampler(seed=SEED)
    patience = sampler.generator("tasks.answer_call.patience")
    agents = DynamicResource(env, "Centre", "agents")
    path_id = "Centre.service.talk"
    instances = {"answer_call": itertools.count(), "hang_up": itertools.count()}

    def call(key):
        instance = next(instances["answer_call"])

        def publish(value):
            env.publish_event(key, value, task="answer_call", instance=instance)

        waits = 60 + (300 - 60) * patience.random()
        publish(
            {
                "path_id": path_id,
                "sim_id": "Centre",
                "service": "talk",
                "status": "queued",
            }
        )
        with agents.request() as request:
            got = yield request | env.timeout(waits)
            if request not in got:
                publish(
                    {
                        "path_id": path_id,
                        "sim_id": "Centre",
                        "service": "talk",
                        "status": "reneged",
                    }
                )
                env.publish_event(
                    key,
                    {"event_type": "hung_up"},
                    task="hang_up",
                    instance=next(instances["hang_up"]),
                )
                return
            publish(
                {
                    "path_id": path_id,
                    "sim_id": "Centre",
                    "service": "talk",
                    "status": "started",
                }
            )
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            publish({"event_type": "answered", "call_id": instance})

    def calls():
        gap = env.registry.get_config("Centre.arrival.calls")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(call(f"calls-{number}"))

    def shift_change():
        yield env.timeout(1800)
        env.registry.update("Centre.resources.agents.current_cap", 4)

    env.process(calls())
    env.process(shift_change())


def run():
    """Runs the model for one simulation hour."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

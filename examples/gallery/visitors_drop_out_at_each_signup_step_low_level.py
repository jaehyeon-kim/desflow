"""Visitors drop out at each step of a signup funnel.

The Low-level form of visitors_drop_out_at_each_signup_step.yaml, with the same
records, built on `DynamicRealtimeEnvironment`. Each visitor is one process that goes
through the steps. A `Router` for each step makes the choice as the blueprint's `next`
does, and the time a step takes is drawn with `Sampler`, from the random sequence the
blueprint's generator uses.

Needs: nothing
Run it with: python examples/gallery/visitors_drop_out_at_each_signup_step_low_level.py
"""

import itertools
import logging
from datetime import datetime

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Router,
    Sampler,
)

SEED = 42
UNTIL = 1200
# Each step: its name, the least and the most seconds it takes (none for the first),
# and the weights of the choice that follows it
STEPS = [
    ("visited", None, {"signed_up": 40, "left": 60}),
    ("signed_up", (5, 30), {"trial_started": 50, "left": 50}),
    ("trial_started", (10, 60), {"paid": 25, "left": 75}),
]


def build(env):
    """Starts the visitor source on `env`."""
    sampler = Sampler(seed=SEED)
    times, routers = {}, {}
    bounds_of = {step: bounds for step, bounds, _ in STEPS}
    for step, bounds, weights in STEPS:
        routers[step] = Router(
            {"weights": weights}, rng=sampler.generator(f"route.{step}")
        )
        if bounds is not None:
            times[step] = sampler.generator(f"tasks.{step}.service_time")
    gap = DistributionConfig(dist="exponential", rate=0.5)
    gap.stream = "arrival.visitors"
    instances = {
        task: itertools.count()
        for task in ("visited", "signed_up", "trial_started", "paid", "left")
    }

    def visitor(key):
        step = "visited"
        while step in routers:
            instance = next(instances[step])
            if step in times:
                env.publish_event(
                    key, {"status": "started"}, task=step, instance=instance
                )
                low, high = bounds_of[step]
                yield env.timeout(low + (high - low) * times[step].random())
            env.publish_event(key, {"stage": step}, task=step, instance=instance)
            came_from, step = step, routers[step].choose(None, env=env)
        # `paid` and `left` end the visit. `left` names the step the visitor left after.
        payload = {"stage": step}
        if step == "left":
            payload["after"] = came_from
        env.publish_event(key, payload, task=step, instance=next(instances[step]))

    def visitors():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(visitor(f"visitors-{number}"))

    env.process(visitors())


def run():
    """Runs the model for 20 simulation minutes."""
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

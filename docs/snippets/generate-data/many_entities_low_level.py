"""Many entities: ten tills, each with its own key, low-level API.

The low-level version of `many_entities.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
till is one process with its own key. Each draw is made with `Sampler`, from the
random sequence the blueprint's generator at that place uses, so the draws are the
blueprint's, and the ten tills share that one sequence.

Needs: nothing.
Run it with: python docs/snippets/generate-data/many_entities_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 300
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the ten tills on `env`."""
    sampler = Sampler(seed=42)
    takings = sampler.generator("tasks.report.payload.takings")
    tills_gap = DistributionConfig(dist="constant", value=60)
    # The random sequence the blueprint's draw uses
    tills_gap.stream = "arrival.tills"
    sent = {"report": 0}

    def till(key):
        while True:
            yield env.timeout(sampler.sample(tills_gap))
            payload = {
                "till": key,
                "takings": round(float(takings.normal(400, 60)), 2),
            }
            env.publish_event(key, payload, task="report", instance=sent["report"])
            sent["report"] += 1

    for number in range(1, 11):
        env.process(till(f"till-{number}"))


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

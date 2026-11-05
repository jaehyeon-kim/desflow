"""A point-in-time training set: feature records and the label that follows each, low-level API.

The low-level version of `point_in_time_training_set.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<name>)`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's.

Needs: nothing
Run it with: python examples/gallery/point_in_time_training_set_low_level.py
"""

import itertools
import logging
from datetime import datetime, timedelta, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 40 * 86400
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the customers on `env`."""
    sampler = Sampler(seed=42)
    sessions = sampler.generator("tasks.features.payload.sessions_30d")
    spend = sampler.generator("tasks.features.payload.spend_30d")
    few = sampler.generator("tasks.label.payload.churned.then")
    many = sampler.generator("tasks.label.payload.churned.else")
    counts = {"features": 0, "label": 0}

    def customer(key):
        for _ in itertools.count():
            yield env.timeout(86400)
            env.process(flow(key))

    def flow(key):
        """The features of a customer now, then the label 7 days later."""
        moment = START + timedelta(seconds=env.now)
        features = {
            "customer_id": key,
            "as_of": moment.isoformat(timespec="milliseconds"),
            "sessions_30d": int(sessions.integers(0, 31)),
            "spend_30d": round(500 * spend.random(), 2),
        }
        env.publish_event(key, features, task="features", instance=counts["features"])
        counts["features"] += 1
        instance = counts["label"]  # the label waits, so it is queued first
        counts["label"] += 1
        env.publish_event(key, {"status": "queued"}, task="label", instance=instance)
        yield env.timeout(7 * 86400)
        env.publish_event(key, {"status": "started"}, task="label", instance=instance)
        # A customer with few sessions leaves with a high chance
        chance, rng = (0.6, few) if features["sessions_30d"] < 5 else (0.05, many)
        label = {
            "customer_id": key,
            "as_of": features["as_of"],
            "churned": bool(rng.random() < chance),
        }
        env.publish_event(key, label, task="label", instance=instance)

    for number in range(1, 6):
        env.process(customer(f"customer-{number}"))


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

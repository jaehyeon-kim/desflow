"""Upstream: a value from the record of the task that routed here, low-level API.

The low-level version of `upstream.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. The order publishes its record, then the
invoice takes the amount from it, as `upstream` reads the record of the task that routed
to it.

Needs: nothing.
Run it with: python examples/generators/upstream_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""
    sampler = Sampler(seed=42)
    amount = sampler.generator("tasks.order.payload.amount")

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            key, instance = f"orders-{number}", number - 1
            order = {"amount": int(amount.integers(10, 20 + 1))}
            env.publish_event(key, order, task="order", instance=instance)
            env.publish_event(
                key, {"amount": order["amount"]}, task="invoice", instance=instance
            )

    env.process(orders())


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

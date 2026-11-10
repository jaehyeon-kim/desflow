"""Format: text built from a template and named parts, low-level API.

The low-level version of `format.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. `format` fills the template with the values of
its `names`, each drawn from the place `names.<name>` below the generator.

Needs: nothing.
Run it with: python examples/generators/format_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the order process on `env`."""
    sampler = Sampler(seed=42)
    region = sampler.generator("tasks.order.payload.label.names.region")
    digits = sampler.generator("tasks.order.payload.label.names.number")
    regions = ["north", "south"]

    def make(number):
        """Draws the payload of order `number`."""
        name = regions[int(region.integers(len(regions)))]
        return {"label": f"{name}-{int(digits.integers(10, 99 + 1))}"}

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            env.publish_event(
                f"orders-{number}", make(number), task="order", instance=number - 1
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

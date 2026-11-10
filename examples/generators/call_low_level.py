"""Call: what a Python function returns, low-level API.

The low-level version of `call.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. The function is the one a blueprint names with
`!python`. It gets no context, because the script has none and passes None.

Needs: nothing.
Run it with: python examples/generators/call_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from call_helpers import with_tax

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""
    sampler = Sampler(seed=42)
    price = sampler.generator("tasks.order.payload.total.kwargs.price")

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            total = with_tax(None, int(price.integers(10, 50 + 1)))
            env.publish_event(
                f"orders-{number}", {"total": total}, task="order", instance=number - 1
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

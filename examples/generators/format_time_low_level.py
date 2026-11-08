"""Format Time: the simulation time in a chosen format, low-level API.

The low-level version of `format_time.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`.
`format_time` writes the simulation time with a `strftime` pattern, or as whole seconds
since 1 January 1970.

Needs: nothing.
Run it with: python examples/generators/format_time_low_level.py
"""

import itertools
import logging
from datetime import datetime, timedelta, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the order process on `env`."""

    def make(number):
        """Draws the payload of order `number`."""
        moment = START + timedelta(seconds=env.now)
        return {
            "day": moment.strftime("%Y-%m-%d %H:%M"),
            "epoch_s": int(moment.timestamp()),
        }

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

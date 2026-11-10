"""Now: the simulation time, with an offset, low-level API.

The low-level version of `now.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. `now` is the simulation time plus the
offset, as ISO text. The script reads `env.now`, never the wall clock.

Needs: nothing.
Run it with: python examples/generators/now_low_level.py
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

        def iso(offset=0):
            """The simulation time plus `offset` seconds, as ISO text."""
            moment = START + timedelta(seconds=env.now + offset)
            return moment.isoformat(timespec="milliseconds")

        return {"placed_at": iso(), "due_at": iso(3600)}

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

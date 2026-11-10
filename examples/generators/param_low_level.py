"""Param: the live value of a variable, low-level API.

The low-level version of `param.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. The variable is registered in the
environment's registry, and the script reads its value when each order is published, as
`param` does.

Needs: nothing.
Run it with: python examples/generators/param_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, SimParameter

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""
    env.registry.register_sim_parameter(
        SimParameter(sim_id="Gen", variables={"tax_rate": 0.2})
    )

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            rate = env.registry.get("Gen.variables.tax_rate").value
            env.publish_event(
                f"orders-{number}",
                {"tax_rate": rate},
                task="order",
                instance=number - 1,
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

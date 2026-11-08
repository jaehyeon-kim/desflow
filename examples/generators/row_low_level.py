"""Row: one row of a CSV file, low-level API.

The low-level version of `row.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<place>)`, the random sequence of the blueprint's generator at that
place, so the draws are the blueprint's. The script reads the file from the working
directory, so run it from the repository root. A cell of digits is a number. `order:
shuffle` draws one permutation of the rows with `permutation(len(rows))` and gives the
rows in that order.

Needs: nothing.
Run it with: python examples/generators/row_low_level.py
"""

import csv
import itertools
import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def _cell(text):
    """A cell of digits is a whole number, one with a point is a decimal number."""
    for kind in (int, float):
        try:
            return kind(text)
        except ValueError:
            pass
    return text


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the processes on `env`."""
    sampler = Sampler(seed=42)
    shuffle = sampler.generator("tasks.order.payload.product")
    with open("examples/generators/products.csv", newline="") as file:
        rows = [
            {name: _cell(text) for name, text in row.items()}
            for row in csv.DictReader(file)
        ]
    order = [int(i) for i in shuffle.permutation(len(rows))]

    def orders():
        for number in itertools.count(1):
            yield env.timeout(60)
            payload = {"product": rows[order[number - 1]]}
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=number - 1
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

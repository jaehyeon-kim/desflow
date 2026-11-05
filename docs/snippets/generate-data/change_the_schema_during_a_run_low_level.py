"""Change the schema during a run, low-level API.

The low-level version of `change_the_schema_during_a_run.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`. A process publishes each label record with
`publish_label` at the time of its change. The order is a dictionary. It gains
`currency` from 60 s and writes `total` as `amount` from 120 s, and from 180 s the
amount is text, as the blueprint's `omit_when` and `when` on `t` do. The value is drawn with
`Sampler`, from the random sequence the blueprint's generator at that place uses.

Needs: nothing.
Run it with: python docs/snippets/generate-data/change_the_schema_during_a_run_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 240
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}

CHANGES = [  # (time in seconds, the label record)
    (60, {"change": "field_added", "field": "currency"}),
    (120, {"change": "field_renamed", "field": "total", "to": "amount"}),
    (180, {"change": "field_retyped", "field": "amount", "to": "text"}),
]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the orders and the changes on `env`."""
    sampler = Sampler(seed=42)
    value = sampler.generator("tasks.order.vars.value")
    gap = DistributionConfig(dist="constant", value=10)
    # The random sequence the blueprint's draw uses
    gap.stream = "arrival.orders"

    def order(number):
        """The payload of one order, as it is at the time `t` it is written."""
        t = env.now
        total = round(float(value.uniform(5, 200)), 2)
        payload = {"order_id": number}
        if t >= 60:
            payload["currency"] = "EUR"
        if t < 120:
            payload["total"] = total
        else:
            payload["amount"] = f"{total}" if t >= 180 else total
        return payload

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.publish_event(
                f"orders-{number}",
                order(number),
                task="order",
                instance=number - 1,
            )

    def changes():
        for at, record in CHANGES:
            yield env.timeout(at - env.now)
            env.publish_label(record)

    env.process(source())
    env.process(changes())


def run() -> None:
    """Runs the model unpaced: the orders to one sink, the labels to another."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress(), ConsoleEgress()], labels=[False, True])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

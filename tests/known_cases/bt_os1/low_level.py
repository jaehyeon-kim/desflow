"""BT-OS1 to BT-OS10: the order-streams JSON producer.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The values come from the `Template` of the task, rendered
by a `ValueRuntime` with the seed of the document, so each field draws from the random
sequence the document's generator at its place draws from.
"""

from datetime import datetime, timezone
from typing import Any

from desflow import DynamicRealtimeEnvironment, Sampler, SimParameter
from desflow.values import Template, ValueRuntime

from tests.known_cases.bt_os1.builder import PAYLOAD

UNTIL = 2000
PARAMS = SimParameter(sim_id="Orders", variables={"delay_seconds": 5})
ENVIRONMENT = {
    "logical_start_time": datetime(2026, 1, 1, tzinfo=timezone.utc),
    "timezone": "UTC",
}


class _Context:
    """What `Template.render` needs of a run: its value runtime."""

    def __init__(self, values: Any) -> None:
        self.values = values


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    context = _Context(ValueRuntime(42, env, sampler, None))
    template = Template(PAYLOAD, name="tasks.order", check=False)

    def orders() -> Any:
        number = 0
        while True:
            yield env.timeout(1)
            value = template.render(context)
            env.publish_event(value["order_id"], value, task="order", instance=number)
            number += 1

    env.process(orders())

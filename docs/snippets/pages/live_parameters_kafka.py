"""Sets the packers capacity of the running Kafka example.

`KafkaAdminConnector.send_config` publishes `{"path": ..., "value": ...}` to the
topic `KafkaIngress` reads, `sim-config` in the example. Call `set_packer_capacity(3)`
while the example runs.
"""

import asyncio

from desflow import KafkaAdminConnector


def set_packer_capacity(value: int) -> None:
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    asyncio.run(
        admin.send_config("sim-config", "Shop.resources.packers.current_cap", value)
    )

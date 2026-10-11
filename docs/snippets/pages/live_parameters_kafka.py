"""Sets the lathe capacity of the running Kafka example.

`KafkaAdminConnector.send_config` publishes `{"path_id": ..., "value": ...}` to the
topic `KafkaIngress` reads, `sim-config` in the example. Call `set_lathe_capacity(3)`
while the example runs.
"""

import asyncio

from dynamic_des import KafkaAdminConnector


def set_lathe_capacity(value: int) -> None:
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    asyncio.run(
        admin.send_config("sim-config", "Line_A.resources.lathe.current_cap", value)
    )

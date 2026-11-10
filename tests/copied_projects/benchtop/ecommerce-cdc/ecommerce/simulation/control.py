"""Changes a simulation parameter while the simulation runs.

It sends the new value to the control topic, where desflow's Kafka ingress updates
the registry, so the next arrival, wait or chance drawn uses it.

Run: python -m ecommerce.simulation.control ecommerce.resources.pickers.current_cap 1
"""

import argparse
import asyncio

from desflow import KafkaAdminConnector

from ecommerce.core.config import (
    ARRIVALS,
    CHANCES,
    CONTROL_TOPIC,
    KAFKA,
    SERVICES,
    SIM_ID,
)

PARAMETERS = [
    *(f"{SIM_ID}.arrival.{name}.rate" for name in ARRIVALS),
    *(f"{SIM_ID}.service.{name}.mean" for name in SERVICES),
    *(f"{SIM_ID}.variables.{name}" for name in CHANCES),
    f"{SIM_ID}.resources.pickers.current_cap",
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Changes a parameter of the running shop."
    )
    parser.add_argument("path", choices=PARAMETERS, help="the parameter to change")
    parser.add_argument("value", type=float, help="its new value")
    args = parser.parse_args()
    asyncio.run(
        KafkaAdminConnector(KAFKA).send_config(CONTROL_TOPIC, args.path, args.value)
    )
    print(f"Sent {args.path} = {args.value} to {CONTROL_TOPIC}")

"""The shop of Use Avro and Pydantic, low-level API, with its predictions serialized
through a secured schema registry.

The egress is `registry_egress()` of kafka_security_connections.py, and the shop is that of
avro_and_pydantic_low_level.py. Both files, and avro_and_pydantic_models.py, must be beside
this script. Set the variables of kafka_security_registry.yaml, then run
`python kafka_security_registry_low_level.py`. It runs for one minute.
"""

import logging

from avro_and_pydantic_low_level import shop
from kafka_security_connections import registry_egress

# --8<-- [start:shop]
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    env = shop([registry_egress()])  # the shop of Use Avro and Pydantic
    try:
        env.run(until=60)
    finally:
        env.teardown()
# --8<-- [end:shop]

"""Settings: the odctl service addresses, this project's names, and the simulation's parameters.

The addresses are as seen from the host, on 127.0.0.1 rather than localhost, because with
IPv6 enabled in Docker some services reset connections to the IPv6 address of localhost.
"""

# odctl's PostgreSQL creates the `cdc` schema, puts it on the search path and publishes
# it as `cdc_pub`, which the Debezium connector reads.
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"
SCHEMA = "cdc"
REPLICATION_SLOT = "ecommerce_cdc"

KAFKA = "127.0.0.1:9092"
SCHEMA_REGISTRY = (
    "http://127.0.0.1:8081"  # Karapace, which Connect reaches as karapace:8081
)
TOPIC_PREFIX = "ecommerce"  # Debezium names each topic ecommerce.cdc.<table>
CONTROL_TOPIC = "ecommerce-control"  # parameter changes the running simulation reads

CONNECT_URL = "http://127.0.0.1:8083"
CONNECTORS = {"ecommerce-cdc-source": "source.json", "ecommerce-cdc-s3": "s3-sink.json"}

S3 = {
    "endpoint_url": "http://127.0.0.1:8333",
    "aws_access_key_id": "user",
    "aws_secret_access_key": "password",
    "region_name": "us-east-1",
}
BUCKET = "odctl-dev"
S3_PREFIX = "ecommerce-cdc/"  # the sink's files, one folder per topic

# The simulation's parameters. Each is in desflow's registry under `ecommerce.`, so
# `ecommerce.simulation.control` can change it while the simulation runs.
SIM_ID = "ecommerce"
ARRIVALS = {  # mean arrivals per second, exponential
    "visitor": 1.0,
    "move": 0.05,  # a registered user moves address
}
SERVICES = {  # mean and standard deviation, in seconds, lognormal
    "page_view": (4.0, 2.0),  # time on a page before the next one
    # how long an order waits for a picker before it is cancelled
    "patience": (120.0, 60.0),
    "pick": (5.0, 2.0),  # a picker packs an order
    "transit": (60.0, 20.0),  # a shipped order reaches the customer
    "return_after": (60.0, 30.0),  # a returned order comes back after delivery
}
CHANCES = {  # shares of visitors or orders, from 0 to 1
    "returning": 0.6,  # a visitor is a registered user
    "buy": 0.3,  # a visitor buys at the end of the visit
    "return": 0.1,  # a delivered order is returned
}
PICKERS, MAX_PICKERS = 3, 10  # warehouse pickers packing orders at once
INITIAL_USERS = 100

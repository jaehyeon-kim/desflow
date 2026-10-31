"""The shop of the Connect to Secure Kafka page, built with the builder API.

The simulation publishes to a cluster that uses SASL PLAIN, such as Confluent Cloud: set
KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET, then run
`python kafka_security.py`. kafka_security_connections.py, which holds the egress, must be
beside this file.
"""

# ruff: noqa: E402

import logging

from kafka_security_connections import egress

from desflow import SimulationContext

# --8<-- [start:shop]
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(egress)  # the SASL egress above
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 min")
# --8<-- [end:shop]

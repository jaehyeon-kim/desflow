"""The shop of the Connect to Secure Kafka page, built with the builder API.

`build(egress)` gives the shop with the egress it is given. Run `python kafka_security.py`
to publish to a cluster that uses SASL PLAIN, such as Confluent Cloud: set
KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET. Set KAFKA_CONNECTION to mtls
or msk to publish to a cluster of that kind instead, as kafka_security_connections.py sets
out. That file, which holds the egresses, must be beside this one.
"""

# ruff: noqa: E402

import logging

from kafka_security_connections import (
    msk_egress,
    mtls_egress,
    sasl_egress,
    selected_egress,
)

from desflow import KafkaEgress, SimulationContext


# --8<-- [start:shop]
def build(egress: KafkaEgress) -> SimulationContext:
    """Builds the shop, with `egress` as its one sink."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        .add_egress(egress)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

    return app


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    build(selected_egress()).run(until="1 min")
# --8<-- [end:shop]


# --8<-- [start:sasl]
def sasl_app() -> SimulationContext:
    """The shop with the egress of a cluster that uses SASL."""
    return build(sasl_egress())


# --8<-- [end:sasl]


# --8<-- [start:mtls]
def mtls_app() -> SimulationContext:
    """The shop with the egress of a cluster that requires mutual TLS."""
    return build(mtls_egress())


# --8<-- [end:mtls]


# --8<-- [start:msk]
def msk_app() -> SimulationContext:
    """The shop with the egress of an Amazon MSK cluster."""
    return build(msk_egress())


# --8<-- [end:msk]

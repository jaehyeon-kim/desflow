"""The Route Records to Topics guide: a shop whose records go to three Kafka topics.

It needs a Kafka broker on 127.0.0.1:9092: start one with `odctl up kafka-lite`. Run it
with `python complex_routing.py`, beside complex_routing_logic.py; it runs for one
minute.
"""

import logging

from complex_routing_logic import create_topics, custom_kafka_router, packed
from desflow import KafkaEgress, SimulationContext

# --8<-- [start:egress]
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_egress(
        KafkaEgress(
            bootstrap_servers="127.0.0.1:9092",
            topic_router=custom_kafka_router,
        )
    )
)
# --8<-- [end:egress]

# --8<-- [start:shop]
app.add_arrival("orders", dist="exponential", rate=0.5)
app.add_service("packing", dist="constant", value=1.5)
app.with_stats("minimal", interval=10)  # readings for shop-telemetry


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return packed(task_id)  # every tenth order fails


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


# --8<-- [end:shop]

# --8<-- [start:run]
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    create_topics()  # with a router, KafkaEgress creates no topics
    app.run(until="1 min")
# --8<-- [end:run]

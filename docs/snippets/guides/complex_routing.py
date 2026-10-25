"""The Route Records to Topics guide: a shop whose records go to three Kafka topics.

It needs a Kafka broker on 127.0.0.1:9092: start one with `odctl up kafka-lite`. Run it
with `python complex_routing.py`; it runs for one minute.
"""

import logging
from typing import Any, Dict

from desflow import KafkaAdminConnector, KafkaEgress, SimulationContext

TOPICS = ["shop-events", "shop-telemetry", "system-alarms"]


# --8<-- [start:router]
def custom_kafka_router(data: Dict[str, Any]) -> str:
    """Picks a topic for each record by its kind and its value."""
    if data.get("stream_type") == "telemetry":
        return "shop-telemetry"

    value = data.get("value")
    if isinstance(value, dict) and (
        value.get("status") == "packing_failed" or value.get("level") == "ERROR"
    ):
        return "system-alarms"

    # Every other event, including a payload that is not a dict.
    return "shop-events"


# --8<-- [end:router]


# --8<-- [start:egress]
app = (
    SimulationContext(sim_id="Shop", factor=1.0)
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
    if task_id % 10 == 9:  # every tenth order fails, for system-alarms
        return {"status": "packing_failed"}
    return {"status": "packed"}


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
    # With a router, KafkaEgress creates no topics, so create them first
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    admin.create_topics([{"name": topic} for topic in TOPICS])
    app.run(until="1 min")
# --8<-- [end:run]

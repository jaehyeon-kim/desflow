"""The Route Records to Topics guide: a shop whose failed orders go to their own topic.

It needs a Kafka broker on 127.0.0.1:9092: start one with `odctl up kafka-lite`. Run it
with `python complex_routing.py`, beside complex_routing_logic.py; it runs for one
minute. With ROUTING=router in the environment, `python complex_routing.py` sends the
records through a function instead, which wins over the topic in the payload.
"""

import logging
import os

from complex_routing_logic import custom_kafka_router
from desflow import KafkaEgress, SimulationContext
from desflow.values import Template

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")


# --8<-- [start:egress]
def payload_egress() -> KafkaEgress:
    """Sends each record to the topic its payload names, and creates the topics."""
    return KafkaEgress(
        bootstrap_servers=BOOTSTRAP,
        event_topic="shop-events",
        telemetry_topic="shop-telemetry",
        topic_policy="create",
        topics={"system-alarms": {}},
    )


# --8<-- [end:egress]


# --8<-- [start:router]
def router_egress() -> KafkaEgress:
    """Sends each record to the topic a function picks."""
    return KafkaEgress(
        bootstrap_servers=BOOTSTRAP,
        topic_router=custom_kafka_router,
        topics={"shop-events": {}, "shop-telemetry": {}, "system-alarms": {}},
    )


# --8<-- [end:router]


def build(egress: KafkaEgress) -> SimulationContext:
    """Builds the shop, whose payload names a topic, with `egress` as its one sink."""
    # --8<-- [start:shop]
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.add_egress(egress)
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_arrival("orders", "exponential", rate=0.5, spawn="pack_order")
    app.add_service("packing", "constant", value=1.5)
    app.with_stats("minimal", interval=10)  # readings for shop-telemetry
    payload = Template(
        {
            "status": {
                "gen": "when",
                "condition": "failed",
                "then": "packing_failed",
                "else": "packed",
            },
            "__topic__": {
                "gen": "when",
                "condition": "failed",
                "then": "system-alarms",
                "else": "shop-events",
            },
        },
        name="tasks.pack_order",
        vars={"failed": {"gen": "expr", "expr": "process.instance % 10 == 9"}},
    )

    @app.task(name="pack_order", service_id="packing", resource_id="packers")
    def pack_order(task_id):
        """Gives one order record."""
        return payload

    # --8<-- [end:shop]
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    chosen = (
        router_egress() if os.environ.get("ROUTING") == "router" else payload_egress()
    )
    build(chosen).run(until="1 min")

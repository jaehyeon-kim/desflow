"""The topic router of the Complex Routing guide, attached to `KafkaEgress`.

Building the egress opens no connection, so tests/unit run this file without a broker.
"""

from typing import Any, Dict

from dynamic_des import KafkaEgress, SimulationContext


# --8<-- [start:router]
def custom_kafka_router(data: Dict[str, Any]) -> str:
    """Picks a topic for each record by its kind and its value."""
    if data.get("stream_type") == "telemetry":
        return "factory-telemetry"

    value = data.get("value")
    if isinstance(value, dict) and (
        value.get("status") == "machine_failed" or value.get("level") == "ERROR"
    ):
        return "system-alarms"

    # Every other event, including a payload that is not a dict.
    return "factory-events"


# --8<-- [end:router]


# --8<-- [start:egress]
app = (
    SimulationContext(sim_id="Factory_A", factor=1.0)
    .add_resource("lathe", current_cap=1, max_cap=1)
    .add_egress(
        KafkaEgress(
            bootstrap_servers="localhost:9092",
            topic_router=custom_kafka_router,
        )
    )
)
# --8<-- [end:egress]

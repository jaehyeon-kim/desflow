"""DX-20: A file sink that writes each event under its day.

Builder form of `model.yaml`.
"""

from datetime import datetime, timezone

from desflow import JsonlStorageEgress, SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("packing", dist="constant", value=1.5)
    app.add_arrival("orders", "constant", value=2, spawn="pack_order")
    app.add_egress(JsonlStorageEgress(default_path="events/other.jsonl"))
    payload = Template(
        {
            "event_type": "order_packed",
            "__path__": {
                "gen": "format_time",
                "format": "events/day=%Y-%m-%d/events.jsonl",
            },
        },
        name="tasks.pack_order",
    )

    @app.task(name="pack_order", service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

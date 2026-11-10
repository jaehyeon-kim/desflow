"""DX-3: A payload that draws the delivery.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=1)
    app.add_resource("packers", current_cap=1, max_cap=3)
    app.add_arrival("orders", "constant", value=5, spawn="pack_order")
    pack_order_payload = Template(
        {
            "status": "finished",
            "order_id": {"gen": "expr", "expr": "process.instance"},
            "delivery": {
                "gen": "weighted",
                "options": {"standard": 0.9, "express": 0.1},
            },
        },
        name="tasks.pack_order",
    )

    @app.task(name="pack_order", service_time=2, resource_id="packers")
    def pack_order(task_id):
        return pack_order_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=6000)

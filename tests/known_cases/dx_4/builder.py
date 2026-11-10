"""DX-4: A payload that is constants and the instance number.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=1)
    app.add_resource("packers", current_cap=1, max_cap=3)
    app.add_arrival("orders", "constant", value=5, spawn="pack")

    @app.task(name="pack", service_time=2, resource_id="packers")
    def pack(task_id, entity):
        return {
            "order_id": task_id,
            "amount": 19.99,
            "event_type": "prediction",
            "task_id": entity.key,
            "confidence": 0.9,
        }

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=40)

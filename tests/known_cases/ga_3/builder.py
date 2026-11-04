"""GA-3: Capacity drops to one at 20 seconds, and the queue grows.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.with_stats(level="minimal", interval=1)
    app.add_resource("packers", current_cap=3)
    app.add_service("pack", dist="normal", mean=2.5, std=0.3)
    app.add_arrival("orders", "exponential", rate=1, spawn="order")
    app.add_scenario(
        [{"at": 20, "path": "Shop.resources.packers.current_cap", "value": 1}]
    )

    @app.task(name="order", resource_id="packers", service_id="pack")
    def order(task_id):
        return {"event_type": "packed"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=60)

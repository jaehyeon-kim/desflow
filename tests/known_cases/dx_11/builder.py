"""DX-11: A fixed number of arrivals.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Bank", factor=0)
    app.add_resource("counter", current_cap=1)
    app.add_service("serving", "constant", value=3)
    app.add_arrival("customers", "constant", value=2, spawn="serve", max_events=3)

    @app.task(name="serve", resource_id="counter", service_id="serving")
    def serve(task_id):
        return {"customer": "served"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

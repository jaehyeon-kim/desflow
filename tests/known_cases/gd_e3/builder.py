"""GD-E3: Read the next event time and step one event at a time.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="E", factor=0, random_seed=42)
    app.add_service("one", dist="constant", value=1)
    app.add_initial_arrival("a", size, spawn="ticking")

    @app.task(name="ticking", service_id="one", next="ticking")
    def ticking(task_id):
        return {"event_type": "tick"}

    return app


def run(app: SimulationContext):
    """Runs the model with the `run` section of `model.yaml`."""
    return app.run(until=10, steps=True)

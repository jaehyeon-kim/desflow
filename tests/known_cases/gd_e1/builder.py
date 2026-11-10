"""GD-E1: Run the model in segments to a series of times.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="E", factor=0, random_seed=42)
    app.add_service("tick", dist="constant", value=1)
    app.add_initial_arrival("a", size, spawn="ticking")

    @app.task(name="ticking", service_id="tick", next="ticking")
    def ticking(task_id):
        return {"event_type": "tick"}

    return app


def run(app: SimulationContext):
    """Runs the model with the `run` section of `model.yaml`."""
    return app.run(until=99, segments={"every": 1})

"""GD-E4: Start the clock at a time other than 0.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="E", factor=0, random_seed=42, initial_time=100)
    app.add_initial_arrival("a", size, spawn="only")

    @app.task(name="only", service_time=1)
    def only(task_id):
        return {"v": 42}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

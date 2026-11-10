"""P38: Balking at a long queue.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("a", size, spawn="job")

    @app.task(name="job", balk="queue.m >= 2", resource_id="m", service_time=1.5)
    def job(task_id):
        return {"event_type": "served"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

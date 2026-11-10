"""P39: Balk, then route to another task.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("a", size, spawn="job")

    @app.task(
        name="job",
        balk="queue.m >= 1",
        on_balk="other",
        resource_id="m",
        service_time=2,
    )
    def job(task_id):
        return {"event_type": "served"}

    @app.task(name="other")
    def other(task_id):
        return {"event_type": "other"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

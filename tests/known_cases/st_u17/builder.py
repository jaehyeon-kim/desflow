"""ST-U17: Empty any-of and all-of conditions succeed at once.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="empty_any")

    @app.task(name="empty_any", wait={"any": {}}, next="empty_all")
    def empty_any(task_id):
        return {"event_type": "any_empty"}

    @app.task(name="empty_all", wait={"all": {}})
    def empty_all(task_id):
        return {"event_type": "all_empty"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

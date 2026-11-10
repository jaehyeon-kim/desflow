"""ST-X02: A failing child that nobody waits for stops the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="parent")

    @app.task(name="parent", spawn={"task": "child"}, next="pause")
    def parent(task_id):
        return {"event_type": "parent"}

    @app.task(name="pause", service_time=1)
    def pause(task_id):
        return {"event_type": "parent_done"}

    @app.task(name="child", fail="True", fail_reason="Onoes!")
    def child(task_id):
        return {"event_type": "child"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(on_unhandled_failure="stop")

"""CV-069: Release a unit and wait for the release.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_resource("m", current_cap=2)
    app.add_initial_arrival("h", size, spawn="take")
    app.add_initial_arrival("o", 2, spawn="watch")

    @app.task(name="take", seize="m", service_time=1, release="m", next="zero")
    def take(task_id):
        return {"event_type": "held"}

    @app.task(name="zero", wait={"timeout": 0})
    def zero(task_id):
        return {"event_type": "after_release"}

    @app.task(name="watch", wait={"timeout": 1}, next="again")
    def watch(task_id):
        return {"event_type": "o"}

    @app.task(name="again", wait={"timeout": 0})
    def again(task_id):
        return {"event_type": "o_again"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

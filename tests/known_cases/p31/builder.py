"""P31: Hold across several steps and release one of two.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("lathe", current_cap=1)
    app.add_resource("press", current_cap=1)
    app.add_initial_arrival("a", size, spawn="take_lathe")

    @app.task(name="take_lathe", seize="lathe", service_time=1, next="take_press")
    def take_lathe(task_id):
        return {"event_type": "lathe"}

    @app.task(
        name="take_press",
        seize="press",
        service_time=2,
        release="lathe",
        next="finish",
    )
    def take_press(task_id):
        return {"event_type": "press"}

    @app.task(name="finish", service_time=1, release="press")
    def finish(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""DS-H6: Release a unit that another entity holds.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="H6", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("holder", size, spawn="hold")
    app.add_initial_arrival("waiter", 1, spawn="want")
    app.add_initial_arrival("third", 1, spawn="free_it")

    @app.task(name="hold", seize="m", service_time=5, release="m")
    def hold(task_id):
        return {"event_type": "held"}

    @app.task(name="want", resource_id="m", service_time=10)
    def want(task_id):
        return {"event_type": "served"}

    @app.task(
        name="free_it",
        service_time=1,
        release={"resource": "m", "task": "hold", "where": "process.instance == 0"},
    )
    def free_it(task_id):
        return {"event_type": "freed"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

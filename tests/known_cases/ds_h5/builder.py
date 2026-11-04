"""DS-H5: Tasks that take resources in opposite orders.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="H5", factor=0, random_seed=42)
    app.add_resource("r1", current_cap=1)
    app.add_resource("r2", current_cap=1)
    app.add_initial_arrival("first", size, spawn="one_two")
    app.add_initial_arrival("second", 1, spawn="two_one")

    @app.task(name="one_two", seize="r1", service_time=1, next="one_two_b")
    def one_two(task_id):
        return {"event_type": "got_r1"}

    @app.task(
        name="one_two_b", seize="r2", service_time=1, release="r2", next="one_two_c"
    )
    def one_two_b(task_id):
        return {"event_type": "got_both"}

    @app.task(name="one_two_c", release="r1")
    def one_two_c(task_id):
        return {"event_type": "free_r1"}

    @app.task(name="two_one", seize="r2", service_time=1, next="two_one_b")
    def two_one(task_id):
        return {"event_type": "got_r2"}

    @app.task(
        name="two_one_b", seize="r1", service_time=1, release="r1", next="two_one_c"
    )
    def two_one_b(task_id):
        return {"event_type": "got_both"}

    @app.task(name="two_one_c", release="r2")
    def two_one_c(task_id):
        return {"event_type": "free_r2"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

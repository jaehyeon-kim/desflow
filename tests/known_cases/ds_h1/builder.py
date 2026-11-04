"""DS-H1: A unit held across several steps.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="H1", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, queue="priority", preempt=True)
    app.add_initial_arrival("job", size, spawn="take")
    app.add_initial_arrival("other", 1, spawn="waiter")

    @app.task(name="take", seize={"resource": "m", "priority": 5}, next="a")
    def take(task_id):
        return {"event_type": "got"}

    @app.task(name="a", service_time=2, next="b")
    def a(task_id):
        return {"event_type": "a"}

    @app.task(name="b", service_time=3, release="m")
    def b(task_id):
        return {"event_type": "b"}

    @app.task(
        name="waiter",
        seize={"resource": "m", "priority": 9, "preempt": False},
        release="m",
    )
    def waiter(task_id):
        return {"event_type": "waiter_granted"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

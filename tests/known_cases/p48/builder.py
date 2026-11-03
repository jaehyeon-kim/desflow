"""P48: Periodic shifts.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, queue="priority")
    app.add_service("ten", dist="constant", value=10)
    app.add_service("brk", dist="constant", value=3)
    app.add_service("one", dist="constant", value=1)
    app.add_initial_arrival("shift", size, spawn="offduty")
    app.add_arrival("jobs", "constant", value=1, spawn="job")

    @app.task(name="offduty", service_id="ten", next="brk")
    def offduty(task_id):
        return {"event_type": "on_duty_ends"}

    @app.task(
        name="brk", resource_id="m", priority=-1, service_id="brk", next="offduty"
    )
    def brk(task_id):
        return {"event_type": "break"}

    @app.task(name="job", resource_id="m", priority=0, service_id="one")
    def job(task_id):
        return {"event_type": "job"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=35)

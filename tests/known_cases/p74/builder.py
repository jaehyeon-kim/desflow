"""P74: Handle an unfinished request.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("h", size, spawn="hold")
    app.add_arrival("late", "constant", value=1)

    @app.task(name="hold", resource_id="m", service_time=5)
    def hold(task_id):
        return {"event_type": "held"}

    @app.task(
        name="job", resource_id="m", patience=4, service_time=0, on_renege="elsewhere"
    )
    def job(task_id):
        return {"event_type": "served"}

    @app.task(name="elsewhere")
    def elsewhere(task_id):
        return {"event_type": "elsewhere"}

    @app.arrival_loop("late")
    def late_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("late")
            context.spawn(job(context.next_instance("job"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

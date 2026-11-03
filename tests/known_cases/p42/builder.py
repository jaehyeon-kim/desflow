"""P42: Renege, then route to another task.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_arrival("a", "constant", value=1, first_at=0)

    @app.task(
        name="job", resource_id="m", patience=2.5, service_time=3, on_renege="elsewhere"
    )
    def job(task_id):
        return {"event_type": "served"}

    @app.task(name="elsewhere", service_time=1)
    def elsewhere(task_id):
        return {"event_type": "elsewhere"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("a")
            context.spawn(job(context.next_instance("job"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""DS-H8: A queue limit that changes during the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="H8", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, queue_limit=1)
    app.add_service("s", dist="constant", value=10)
    app.add_arrival("early", "constant", value=1, first_at=0)
    app.add_arrival("late", "constant", value=1, first_at=5)

    @app.task(name="job", resource_id="m", service_id="s")
    def job(task_id):
        return {"event_type": "done"}

    @app.arrival_loop("early")
    def early_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("early")
            context.spawn(job(context.next_instance("job"), entity=entity))

    @app.arrival_loop("late")
    def late_arrivals(context):
        for _ in range(2):
            entity = yield context.wait_for_arrival("late")
            context.spawn(job(context.next_instance("job"), entity=entity))

    app.add_scenario([{"at": 4, "path": "H8.resources.m.queue_limit", "value": 2}])
    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

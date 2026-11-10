"""DS-I3: A flow after an interrupt, then the time left.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="I3", factor=0, random_seed=42)
    app.add_service("long", dist="constant", value=10)
    app.add_service("fix", dist="constant", value=5)
    app.add_arrival("jobs", "constant", value=1, first_at=0)
    app.add_arrival("breaker", "constant", value=2)

    @app.task(
        name="work", service_id="long", on_interrupt="resume", on_interrupt_run="repair"
    )
    def work(task_id):
        return {"event_type": "done"}

    @app.task(name="repair", service_id="fix")
    def repair(task_id):
        return {"event_type": "repaired"}

    @app.task(name="hit", interrupt={"task": "work", "cause": "x"})
    def hit(task_id):
        return {"event_type": "hit"}

    @app.arrival_loop("jobs")
    def jobs_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("jobs")
            context.spawn(work(context.next_instance("work"), entity=entity))

    @app.arrival_loop("breaker")
    def breaker_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("breaker")
            context.spawn(hit(context.next_instance("hit"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""DS-V4: A random value drawn for each entity and used as a priority.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def _drawn() -> dict:
    """An attribute `x` drawn for each entity, a generator of its own per arrival."""
    return {"x": generator({"gen": "sample", "dist": "exponential", "rate": 2})}


def build() -> SimulationContext:
    app = SimulationContext(sim_id="V4", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1, queue="priority")
    app.add_arrival("j0", "constant", value=1, first_at=0, attributes=_drawn())
    app.add_arrival("j1", "constant", value=1, first_at=0, attributes=_drawn())
    app.add_arrival("j2", "constant", value=1, first_at=0, attributes=_drawn())
    app.add_arrival("j3", "constant", value=1, first_at=0, attributes=_drawn())
    job_payload = Template(
        {"event_type": "served", "x": {"gen": "attr", "name": "x"}}, name="job"
    )

    @app.task(name="job", resource_id="m", service_time=1, priority="attr.x")
    def job(task_id):
        return job_payload

    @app.arrival_loop("j0")
    def j0_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("j0")
            context.spawn(job(context.next_instance("job"), entity=entity))

    @app.arrival_loop("j1")
    def j1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("j1")
            context.spawn(job(context.next_instance("job"), entity=entity))

    @app.arrival_loop("j2")
    def j2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("j2")
            context.spawn(job(context.next_instance("job"), entity=entity))

    @app.arrival_loop("j3")
    def j3_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("j3")
            context.spawn(job(context.next_instance("job"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

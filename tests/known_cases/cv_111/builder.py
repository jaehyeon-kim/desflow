"""CV-111: Handle a failure of several steps.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_arrival(
        "a", "constant", value=1, attributes={"bad": generator({"gen": "boolean"})}
    )

    @app.task(
        name="step_one",
        service_time=1,
        fail="attr.bad",
        fail_reason="step one failed",
        on_fail="handle",
        next="step_two",
    )
    def step_one(task_id):
        return {"event_type": "one"}

    @app.task(
        name="step_two",
        service_time=1,
        fail="attr.bad",
        fail_reason="step two failed",
        on_fail="handle",
    )
    def step_two(task_id):
        return {"event_type": "two"}

    @app.task(name="handle")
    def handle(task_id):
        return {"event_type": "handled"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("a")
            context.spawn(step_one(context.next_instance("step_one"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

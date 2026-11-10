"""CV-113: A cleanup that always runs.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_arrival(
        "a", "constant", value=1, attributes={"bad": generator({"gen": "boolean"})}
    )
    app.add_arrival("i", "constant", value=2.5)

    @app.task(
        name="body",
        service_time=2,
        fail="attr.bad",
        fail_reason="body failed",
        on_fail="cleanup",
        on_interrupt="abandon",
        on_interrupt_next="cleanup",
        next="cleanup",
    )
    def body(task_id):
        return {"event_type": "body"}

    @app.task(name="cleanup")
    def cleanup(task_id):
        return {"event_type": "cleanup"}

    @app.task(name="hit", interrupt={"task": "body", "cause": "x"})
    def hit(task_id):
        return {"event_type": "hit"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("a")
            context.spawn(body(context.next_instance("body"), entity=entity))

    @app.arrival_loop("i")
    def i_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("i")
            context.spawn(hit(context.next_instance("hit"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

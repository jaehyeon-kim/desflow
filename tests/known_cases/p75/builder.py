"""P75: Read the remaining duration of a component.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_initial_arrival("a", 1, spawn="work")
    app.add_arrival("i", "constant", value=4)

    @app.task(
        name="work", service_time=10, on_interrupt="abandon", on_interrupt_next="left"
    )
    def work(task_id):
        return {"event_type": "done"}

    left_payload = Template(
        {"event_type": "left", "remaining": {"gen": "expr", "expr": "cause.left"}},
        name="left",
    )

    @app.task(name="left")
    def left(task_id):
        return left_payload

    @app.task(name="hit", interrupt={"task": "work", "cause": "x"})
    def hit(task_id):
        return {"event_type": "hit"}

    @app.arrival_loop("i")
    def i_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("i")
            context.spawn(hit(context.next_instance("hit"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""CV-004: Read whether a signal or a wait branch succeeded.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_signal("x", once=True)
    app.add_initial_arrival("w", size, spawn="wait_x")
    app.add_arrival("f", "constant", value=1)
    wait_x_payload = Template(
        {
            "event_type": "ok",
            "signal_ok": {"gen": "expr", "expr": "signal.x.ok"},
            "branch_ok": {"gen": "expr", "expr": "wait.x.ok"},
        },
        name="wait_x",
    )

    @app.task(name="wait_x", wait={"signal": "x"})
    def wait_x(task_id):
        return wait_x_payload

    @app.task(name="fire_it", signal="x")
    def fire_it(task_id):
        return {"event_type": "fired"}

    @app.arrival_loop("f")
    def f_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("f")
            context.spawn(fire_it(context.next_instance("fire_it"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

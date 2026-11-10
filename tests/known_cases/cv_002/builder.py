"""CV-002: Read whether a signal has fired.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_signal("x", once=True)
    app.add_arrival("f", "constant", value=1)
    app.add_arrival("l", "constant", value=2)

    @app.task(name="fire_it", signal="x")
    def fire_it(task_id):
        return {"event_type": "fired"}

    look_payload = Template(
        {
            "event_type": "look",
            "triggered": {"gen": "expr", "expr": "signal.x.count >= 1"},
        },
        name="look",
    )

    @app.task(name="look")
    def look(task_id):
        return look_payload

    @app.arrival_loop("f")
    def f_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("f")
            context.spawn(fire_it(context.next_instance("fire_it"), entity=entity))

    @app.arrival_loop("l")
    def l_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("l")
            context.spawn(look(context.next_instance("look"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

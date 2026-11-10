"""CV-128: A resource picked by a computed name.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_resource("left", current_cap=1)
    app.add_resource("right", current_cap=1)
    app.add_arrival(
        "a",
        "constant",
        value=1,
        attributes={"side": generator({"gen": "one_of", "choices": ["left", "right"]})},
    )

    @app.task(
        name="pick",
        next={
            "when": [{"condition": "attr.side == 'left'", "task": "at_left"}],
            "default": "at_right",
        },
    )
    def pick(task_id):
        return {"event_type": "pick"}

    @app.task(name="at_left", resource_id="left", service_time=2)
    def at_left(task_id):
        return {"event_type": "served_left"}

    @app.task(name="at_right", resource_id="right", service_time=2)
    def at_right(task_id):
        return {"event_type": "served_right"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("a")
            context.spawn(pick(context.next_instance("pick"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

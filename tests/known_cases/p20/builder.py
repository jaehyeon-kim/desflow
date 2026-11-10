"""P20: Branch by an attribute with fixed weights.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_variable("open", 1)
    app.add_arrival(
        "a",
        "constant",
        value=1,
        attributes={"vip": generator({"gen": "boolean", "p": 0.3})},
    )

    @app.task(
        name="route",
        next={
            "when": [
                {"condition": "attr.vip", "task": "fast"},
                {"condition": "registry['P.variables.open'] == 0", "task": "closed"},
            ],
            "default": "weighted",
        },
    )
    def route(task_id):
        return {"event_type": "route"}

    @app.task(name="weighted", next={"weights": {"a": 3, "b": 1}})
    def weighted(task_id):
        return {"event_type": "weighted"}

    @app.task(name="fast")
    def fast(task_id):
        return {"event_type": "fast"}

    @app.task(name="closed")
    def closed(task_id):
        return {"event_type": "closed"}

    @app.task(name="a")
    def a(task_id):
        return {"event_type": "a"}

    @app.task(name="b")
    def b(task_id):
        return {"event_type": "b"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(6):
            entity = yield context.wait_for_arrival("a")
            context.spawn(route(context.next_instance("route"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

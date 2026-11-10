"""P22: A branch by fixed weights.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=1)

    @app.task(name="route", next={"weights": {"a": 3, "b": 1}})
    def route(task_id):
        return {"event_type": "route"}

    @app.task(name="a")
    def a(task_id):
        return {"event_type": "a"}

    @app.task(name="b")
    def b(task_id):
        return {"event_type": "b"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(8):
            entity = yield context.wait_for_arrival("a")
            context.spawn(route(context.next_instance("route"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""P21: A branch by state.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_variable("open", 1)
    app.add_arrival("a", "constant", value=1)
    app.add_step(3.5, path="P.variables.open", value=0)

    @app.task(
        name="route",
        next={
            "when": [
                {"condition": "registry['P.variables.open'] == 0", "task": "closed"}
            ],
            "default": "fast",
        },
    )
    def route(task_id):
        return {"event_type": "route"}

    @app.task(name="fast")
    def fast(task_id):
        return {"event_type": "fast"}

    @app.task(name="closed")
    def closed(task_id):
        return {"event_type": "closed"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(6):
            entity = yield context.wait_for_arrival("a")
            context.spawn(route(context.next_instance("route"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

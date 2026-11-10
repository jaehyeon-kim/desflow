"""CV-137: The same seed gives the same run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=7)
    app.add_arrival("a", "exponential", rate=1)

    @app.task(name="t")
    def t(task_id):
        return {"event_type": "arrived"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(5):
            entity = yield context.wait_for_arrival("a")
            context.spawn(t(context.next_instance("t"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

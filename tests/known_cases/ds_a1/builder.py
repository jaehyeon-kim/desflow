"""DS-A1: A fixed number of arrivals.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="A1", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=2)

    @app.task(name="t")
    def t(task_id):
        return {"event_type": "arrived"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("a")
            context.spawn(t(context.next_instance("t"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""P11: A timeout from a named distribution.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_service("s", dist="lognormal", mean=2, std=0.5)
    app.add_arrival("a", "constant", value=1)

    @app.task(name="t", service_id="s")
    def t(task_id):
        return {"event_type": "done"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(5):
            entity = yield context.wait_for_arrival("a")
            context.spawn(t(context.next_instance("t"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

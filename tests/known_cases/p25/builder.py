"""P25: Rework chosen by weights.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival("a", "constant", value=2)

    @app.task(name="make", service_time=1, next="check")
    def make(task_id):
        return {"event_type": "made"}

    @app.task(name="check", next={"weights": {"make": 1, "pass": 4}})
    def check(task_id):
        return {"event_type": "checked"}

    @app.task(name="pass")
    def pass_task(task_id):
        return {"event_type": "pass"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(5):
            entity = yield context.wait_for_arrival("a")
            context.spawn(make(context.next_instance("make"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

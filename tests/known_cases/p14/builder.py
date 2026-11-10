"""P14: A delay from the state.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_variable("slow", 3)
    app.add_arrival("a", "constant", value=5)

    @app.task(name="u", service_time="registry['P.variables.slow']", next="v")
    def u(task_id):
        return {"event_type": "by_state"}

    @app.task(name="v", service_time="1 + t / 10")
    def v(task_id):
        return {"event_type": "by_time"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("a")
            context.spawn(u(context.next_instance("u"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=30)

"""P30: Hold a unit across several steps and release one of two.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_container("m", current_cap=1, max_cap=1, level=1)
    app.add_service("s1", dist="constant", value=1)
    app.add_service("s2", dist="constant", value=2)
    app.add_arrival("a", "constant", value=0.5, first_at=0)

    @app.task(
        name="t1", service_id="s1", get={"container": "m", "amount": 1}, next="t2"
    )
    def t1(task_id):
        return {"event_type": "t1"}

    @app.task(name="t2", service_id="s2", put={"container": "m", "amount": 1})
    def t2(task_id):
        return {"event_type": "t2"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(3):
            entity = yield context.wait_for_arrival("a")
            context.spawn(t1(context.next_instance("t1"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

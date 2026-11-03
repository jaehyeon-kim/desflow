"""ST-R25: A filter get waits for a matching item.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_store("s", current_cap=2)
    app.add_initial_arrival("g", size, spawn="want_b")
    app.add_initial_arrival("p", 1, spawn="put_a")

    @app.task(name="want_b", get={"store": "s", "filter": "item == 'b'"})
    def want_b(task_id):
        return {"event_type": "got_b"}

    @app.task(name="put_a", put={"store": "s", "item": "a"}, next="put_b")
    def put_a(task_id):
        return {"event_type": "put_a"}

    @app.task(name="put_b", put={"store": "s", "item": "b"})
    def put_b(task_id):
        return {"event_type": "put_b"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

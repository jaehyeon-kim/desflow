"""ST-R16: A container with a putter and a getter.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_container("buf", current_cap=2, level=0)
    app.add_initial_arrival("putter", size, spawn="wait_one")
    app.add_initial_arrival("getter", 1, spawn="get_one")

    @app.task(name="wait_one", service_time=1, next="put")
    def wait_one(task_id):
        return {"event_type": "begin"}

    @app.task(name="put", put={"container": "buf", "amount": 2}, next="pause")
    def put(task_id):
        return {"event_type": "p"}

    @app.task(name="pause", service_time=1, next="put")
    def pause(task_id):
        return {"event_type": "pause"}

    @app.task(name="get_one", get={"container": "buf", "amount": 1}, next="pause_get")
    def get_one(task_id):
        return {"event_type": "g"}

    @app.task(name="pause_get", service_time=1, next="get_two")
    def pause_get(task_id):
        return {"event_type": "pause"}

    @app.task(name="get_two", get={"container": "buf", "amount": 1})
    def get_two(task_id):
        return {"event_type": "g"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=5)

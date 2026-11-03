"""ST-R09: Priority resource with four requests.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1, queue="priority")
    app.add_initial_arrival("p0", size, spawn="pem", attributes={"delay": 0, "prio": 2})
    app.add_initial_arrival("p1", 1, spawn="pem", attributes={"delay": 2, "prio": 3})
    app.add_initial_arrival("p2", 1, spawn="pem", attributes={"delay": 2, "prio": 3})
    app.add_initial_arrival("p3", 1, spawn="pem", attributes={"delay": 4, "prio": 1})

    @app.task(name="pem", service_time="attr.delay", next="use")
    def pem(task_id):
        return {"event_type": "arrived"}

    @app.task(name="use", resource_id="res", priority="attr.prio", service_time=5)
    def use(task_id):
        return {"event_type": "granted_and_done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

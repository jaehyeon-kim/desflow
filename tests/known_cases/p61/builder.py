"""P61: Blocking with a finite buffer.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 4) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m1", current_cap=1)
    app.add_resource("m2", current_cap=1)
    app.add_store("buf", current_cap=1)
    app.add_service("one", dist="constant", value=1)
    app.add_service("five", dist="constant", value=5)
    app.add_initial_arrival("orders", size, spawn="s1")

    @app.task(
        name="s1", service_id="one", resource_id="m1", put={"store": "buf"}, next="s2g"
    )
    def s1(task_id):
        return {}

    @app.task(name="s2g", get={"store": "buf"}, next="s2")
    def s2g(task_id):
        return {}

    @app.task(name="s2", service_id="five", resource_id="m2")
    def s2(task_id):
        return {"done": 1}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=30)

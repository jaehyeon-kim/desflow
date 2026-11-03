"""P56: Clone and join all.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_service("one", dist="constant", value=1)
    app.add_service("three", dist="constant", value=3)
    app.add_initial_arrival("orders", size, spawn="join")

    @app.task(name="join", wait={"all": {"x": {"task": "bx"}, "y": {"task": "by"}}})
    def join(task_id):
        return {"joined": 1}

    @app.task(name="bx", service_id="one")
    def bx(task_id):
        return {"x": 1}

    @app.task(name="by", service_id="three")
    def by(task_id):
        return {"y": 3}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=10)

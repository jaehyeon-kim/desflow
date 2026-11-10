"""ST-R06: A process that asks again does not pass the queue.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("children", 2, spawn="round", attributes={"n": 0})

    @app.task(
        name="round",
        resource_id="res",
        service_time=1,
        set={"n": generator({"gen": "expr", "expr": "attr.n + 1"})},
        next={"when": [{"condition": "attr.n < 3", "task": "round"}]},
    )
    def round(task_id):
        return {"event_type": "acquired"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""ST-R10: A priority queue with a limit of one.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1, queue="priority", queue_limit=1)
    app.add_variable("queue_full", False)
    app.add_initial_arrival("users", size, spawn="pem")

    @app.task(
        name="pem",
        open={
            "a": {"request": {"resource": "res", "priority": 1}},
            "b": {"request": {"resource": "res", "priority": 1}},
        },
        on=[
            {
                "path": "T.variables.queue_full",
                "value": generator({"gen": "expr", "expr": "len(waiting(res)) >= 1"}),
            }
        ],
        next="third",
    )
    def pem(task_id):
        return {"event_type": "opened"}

    @app.task(
        name="third",
        open={"c": {"request": {"resource": "res", "priority": 1}}},
        wait={"timeout": 0},
    )
    def third(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""P46: Choose the shortest queue or the first free.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m1", current_cap=1)
    app.add_resource("m2", current_cap=1)
    app.add_initial_arrival("a", size, spawn="pick")

    @app.task(
        name="pick",
        next={
            "when": [
                {
                    "condition": "queue.m1 + users.m1 <= queue.m2 + users.m2",
                    "task": "w1",
                }
            ],
            "default": "w2",
        },
    )
    def pick(task_id):
        return {"event_type": "pick"}

    @app.task(name="w1", resource_id="m1", service_time=3)
    def w1(task_id):
        return {"event_type": "served_1"}

    @app.task(name="w2", resource_id="m2", service_time=3)
    def w2(task_id):
        return {"event_type": "served_2"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""P77: Release all units at once.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=3)
    app.add_initial_arrival("a", size, spawn="take_one")

    @app.task(name="take_one", seize="m", next="take_two")
    def take_one(task_id):
        return {"event_type": "one"}

    @app.task(name="take_two", seize="m", next="work")
    def take_two(task_id):
        return {"event_type": "two"}

    @app.task(name="work", service_time=1, next="release_all")
    def work(task_id):
        return {"event_type": "worked"}

    @app.task(
        name="release_all",
        release="m",
        next={
            "when": [
                {
                    "condition": "len([h for h in held(m) if h.entity == process.key]) > 0",
                    "task": "release_all",
                }
            ]
        },
    )
    def release_all(task_id):
        return {"event_type": "releasing"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

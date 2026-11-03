"""GD-E7: A process returns a value to the process that waits for it.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="E", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="parent")

    @app.task(
        name="parent",
        wait={"task": "my_proc"},
        next={"when": [{"condition": "wait.my_proc.value.ret == 42", "task": "ok"}]},
    )
    def parent(task_id):
        return {"event_type": "parent_done"}

    @app.task(name="my_proc", service_time=1)
    def my_proc(task_id):
        return {"ret": 42}

    @app.task(name="ok")
    def ok(task_id):
        return {"assert": "passed"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

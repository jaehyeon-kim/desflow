"""ST-U03: Subscribe to a process that has ended.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("done", once=False)
    app.add_initial_arrival("c", size, spawn="child")
    app.add_initial_arrival("p", 1, spawn="parent")

    @app.task(name="child", service_time=1, signal="done")
    def child(task_id):
        return {"event_type": "child_done"}

    @app.task(
        name="parent",
        service_time=2,
        next={"when": [{"condition": "signal.done.count == 0", "task": "late"}]},
    )
    def parent(task_id):
        return {"event_type": "parent"}

    @app.task(name="late", listen={"signal": "done"})
    def late(task_id):
        return {"event_type": "late"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(on_unhandled_failure="stop")

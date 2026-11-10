"""ST-I05: Interrupt a process that has ended.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_variable("interrupt_raised", False)
    app.add_initial_arrival("parent", size, spawn="parent")

    @app.task(name="parent", spawn={"task": "child"}, next="later")
    def parent(task_id):
        return {"event_type": "parent"}

    @app.task(name="child", service_time=1)
    def child(task_id):
        return {"event_type": "child_done"}

    @app.task(
        name="later",
        service_time=2,
        interrupt={"task": "child"},
        on=[
            {
                "path": "T.variables.interrupt_raised",
                "value": generator({"gen": "expr", "expr": "running(child) == 0"}),
            }
        ],
    )
    def later(task_id):
        return {"event_type": "late"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

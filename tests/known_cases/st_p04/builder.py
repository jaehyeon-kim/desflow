"""ST-P04: Two child calls that return the time.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="parent")

    @app.task(
        name="parent",
        wait={"task": "child"},
        set={"r1": generator({"gen": "expr", "expr": "wait.child.value.now"})},
        next="second",
    )
    def parent(task_id):
        return {"event_type": "first"}

    second_payload = Template(
        {
            "event_type": "second",
            "r1": {"gen": "expr", "expr": "attr.r1"},
            "r2": {"gen": "expr", "expr": "wait.child.value.now"},
        },
        name="second",
    )

    @app.task(name="second", wait={"task": "child"})
    def second(task_id):
        return second_payload

    child_payload = Template({"now": {"gen": "expr", "expr": "t"}}, name="child")

    @app.task(name="child", service_time=1)
    def child(task_id):
        return child_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

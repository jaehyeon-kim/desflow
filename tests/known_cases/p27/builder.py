"""P27: A sub-process with arguments.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_initial_arrival(
        "a", size, spawn="parent", attributes={"k": 2, "k_later": 100}
    )
    parent_payload = Template(
        {
            "event_type": "got",
            "va": {"gen": "expr", "expr": "wait.a.value.v"},
            "vb": {"gen": "expr", "expr": "wait.b.value.v"},
        },
        name="parent",
    )

    @app.task(
        name="parent",
        wait={
            "all": {
                "a": {
                    "task": "child",
                    "args": {"x": generator({"gen": "expr", "expr": "attr.k * 3"})},
                },
                "b": {
                    "task": "child",
                    "args": {
                        "x": generator({"gen": "expr", "expr": "attr.k_later * 3"})
                    },
                },
            }
        },
    )
    def parent(task_id):
        return parent_payload

    child_payload = Template({"v": {"gen": "expr", "expr": "arg.x + 1"}}, name="child")

    @app.task(name="child", service_time=1)
    def child(task_id):
        return child_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

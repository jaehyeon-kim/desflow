"""GD-M1: Collect a value in a list five times.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="M", factor=0, random_seed=42)
    app.add_initial_arrival("a", 1, spawn="add", attributes={"val": 0, "i": 0})
    add_payload = Template(
        {"event_type": "added", "val": {"gen": "expr", "expr": "attr.val + t"}},
        name="add",
    )

    @app.task(
        name="add",
        set={
            "val": generator({"gen": "expr", "expr": "attr.val + t"}),
            "i": generator({"gen": "expr", "expr": "attr.i + 1"}),
        },
        next="pause",
    )
    def add(task_id):
        return add_payload

    @app.task(
        name="pause",
        service_time=1,
        next={"when": [{"condition": "attr.i < 5", "task": "add"}]},
    )
    def pause(task_id):
        return {"event_type": "waited"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

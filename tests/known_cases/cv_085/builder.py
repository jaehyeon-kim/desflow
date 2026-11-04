"""CV-085: Count the items of a store and test membership.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_store("q", items=["a", "b"])
    app.add_initial_arrival("a", size, spawn="look")
    look_payload = Template(
        {
            "event_type": "look",
            "n": {"gen": "expr", "expr": "count(q)"},
            "has_a": {"gen": "expr", "expr": "'a' in items(q)"},
        },
        name="look",
    )

    @app.task(name="look")
    def look(task_id):
        return look_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

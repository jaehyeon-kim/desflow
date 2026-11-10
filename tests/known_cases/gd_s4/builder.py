"""GD-S4: Return the first needle found.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="S", factor=0, random_seed=42)
    app.add_store(
        "haystack",
        items=[{"needle": 0, "n": 1}, {"needle": 0, "n": 2}, {"needle": 1, "n": 3}],
    )
    app.add_initial_arrival("a", size, spawn="find")

    @app.task(
        name="find",
        get={"store": "haystack"},
        next={
            "when": [{"condition": "item.needle == 0", "task": "find"}],
            "default": "found",
        },
    )
    def find(task_id, item):
        return {"event_type": "looked", "item": item}

    found_payload = Template(
        {"event_type": "found", "needle": {"gen": "expr", "expr": "item"}}, name="found"
    )

    @app.task(name="found")
    def found(task_id):
        return found_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

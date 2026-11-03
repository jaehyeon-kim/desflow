"""ST-C20: An all-of condition over no events succeeds at once.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="empty_all")
    empty_all_payload = Template(
        {"event_type": "all_empty", "at": {"gen": "expr", "expr": "t"}},
        name="empty_all",
    )

    @app.task(name="empty_all", wait={"all": {}}, next="empty_any")
    def empty_all(task_id):
        return empty_all_payload

    empty_any_payload = Template(
        {"event_type": "any_empty", "at": {"gen": "expr", "expr": "t"}},
        name="empty_any",
    )

    @app.task(name="empty_any", wait={"any": {}})
    def empty_any(task_id):
        return empty_any_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

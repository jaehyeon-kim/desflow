"""DS-M7: Wait for the whole flow that a task belongs to.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="M7", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="relay")
    app.add_initial_arrival("w", 1, spawn="watch")

    @app.task(name="relay", service_time=3, next="more")
    def relay(task_id):
        return {"event_type": "relay"}

    @app.task(name="more", service_time=3)
    def more(task_id):
        return {"event_type": "more"}

    watch_payload = Template(
        {"event_type": "flow_done", "at": {"gen": "expr", "expr": "t"}}, name="watch"
    )

    @app.task(
        name="watch",
        wait={
            "running": {
                "task": "relay",
                "where": "process.instance == 0",
                "whole_flow": True,
            }
        },
    )
    def watch(task_id):
        return watch_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

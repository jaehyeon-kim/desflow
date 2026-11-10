"""DX-15: A wait for the whole flow of a process.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Relay", factor=0)
    app.add_initial_arrival("a", size, spawn="relay")
    app.add_initial_arrival("w", 1, spawn="watch")

    @app.task(name="relay", service_time=3, next="more")
    def relay(task_id):
        return {"step": "relay"}

    @app.task(name="more", service_time=3)
    def more(task_id):
        return {"step": "more"}

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
        return {"step": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

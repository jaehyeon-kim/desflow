"""ST-P02: Read what a process waits for, then interrupt it.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("w", size, spawn="waiter")
    app.add_initial_arrival("look", 1, spawn="look")

    @app.task(name="waiter", wait={"timeout": 5}, on_interrupt="abandon")
    def waiter(task_id):
        return {"event_type": "waited"}

    @app.task(
        name="look",
        interrupt={"task": "waiter", "where": "process.target == 'timeout'"},
    )
    def look(task_id):
        return {"event_type": "look"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""ST-P03: A waiter waits for a finisher process.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("w", size, spawn="waiter")

    @app.task(name="waiter", wait={"task": "finisher"})
    def waiter(task_id):
        return {"event_type": "waiter_done"}

    @app.task(name="finisher", service_time=5)
    def finisher(task_id):
        return {"event_type": "finisher_done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

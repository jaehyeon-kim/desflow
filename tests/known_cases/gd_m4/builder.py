"""GD-M4: Trace every event the run processes.

Builder form of `model.yaml`.
"""

from desflow import ConsoleEgress, SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="M", factor=0, random_seed=42)
    app.with_stats(level="none", trace=True)
    app.add_egress(ConsoleEgress(), when="trace")
    app.add_initial_arrival("a", size, spawn="ticking")

    @app.task(name="ticking", service_time=1)
    def ticking(task_id):
        return {"event_type": "tick"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

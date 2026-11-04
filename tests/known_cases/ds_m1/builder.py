"""DS-M1: A reading at each change of a resource and an event trace.

Builder form of `model.yaml`.
"""

from desflow import ConsoleEgress, SimulationContext


def build(size: int = 2) -> SimulationContext:
    app = SimulationContext(sim_id="M1", factor=0, random_seed=42)
    app.with_stats(level="basic", on_change=True, trace=True)
    app.add_egress(ConsoleEgress(), when="trace")
    app.add_egress(ConsoleEgress())
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("a", size, spawn="use")

    @app.task(name="use", resource_id="res", service_time=0)
    def use(task_id):
        return {"event_type": "used"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

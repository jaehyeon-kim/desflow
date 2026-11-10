"""ST-R12: A lower number preempts a holder.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1, queue="priority", preempt=True)
    app.add_initial_arrival("a", size, spawn="proc_a")
    app.add_initial_arrival("b", 1, spawn="proc_b")

    @app.task(
        name="proc_a",
        resource_id="res",
        priority=1,
        service_time=0,
        on_interrupt="abandon",
    )
    def proc_a(task_id):
        return {"event_type": "should_not_finish"}

    @app.task(name="proc_b", resource_id="res", priority=0, service_time=0)
    def proc_b(task_id):
        return {"event_type": "got"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

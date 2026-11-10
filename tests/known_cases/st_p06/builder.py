"""ST-P06: A parent interrupted while joining a child is not resumed by it.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="parent")
    app.add_initial_arrival("i", 1, spawn="interruptor")

    @app.task(
        name="parent",
        wait={"task": "child"},
        on_interrupt="abandon",
        on_interrupt_next="after",
    )
    def parent(task_id):
        return {"event_type": "joined"}

    @app.task(name="after", service_time=5)
    def after(task_id):
        return {"event_type": "after_interrupt"}

    @app.task(name="child", service_time=2)
    def child(task_id):
        return {"event_type": "child_done"}

    @app.task(name="interruptor", service_time=1, interrupt={"task": "parent"})
    def interruptor(task_id):
        return {"event_type": "interrupt"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

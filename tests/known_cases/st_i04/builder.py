"""ST-I04: A child interrupted at the instant it is created.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("root", size, spawn="root")

    @app.task(name="root", spawn={"task": "child"}, next="stop_child")
    def root(task_id):
        return {"event_type": "root"}

    @app.task(name="stop_child", interrupt={"task": "child"}, next="root_wait")
    def stop_child(task_id):
        return {"event_type": "stop_child"}

    @app.task(name="root_wait", service_time=1)
    def root_wait(task_id):
        return {"event_type": "root_done"}

    @app.task(
        name="child",
        service_time=10,
        on_interrupt="abandon",
        on_interrupt_next="caught",
    )
    def child(task_id):
        return {"event_type": "should_not_finish"}

    @app.task(name="caught")
    def caught(task_id):
        return {"event_type": "interrupted"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

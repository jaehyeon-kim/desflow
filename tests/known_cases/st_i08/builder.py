"""ST-I08: A child waiting on an event interrupted at 5.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("never", once=True)
    app.add_initial_arrival("parent", size, spawn="parent")

    @app.task(name="parent", spawn={"task": "child"}, service_time=5, next="stop")
    def parent(task_id):
        return {"event_type": "waited"}

    @app.task(name="stop", interrupt={"task": "child"})
    def stop(task_id):
        return {"event_type": "interrupt"}

    @app.task(
        name="child",
        wait={"signal": "never"},
        on_interrupt="abandon",
        on_interrupt_next="caught",
    )
    def child(task_id):
        return {"event_type": "finished"}

    caught_payload = Template(
        {"event_type": "interrupted", "at": {"gen": "expr", "expr": "t"}}, name="caught"
    )

    @app.task(name="caught")
    def caught(task_id):
        return caught_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""ST-I01: A child interrupted at 5 with a cause.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("boss", size, spawn="interruptor")

    @app.task(
        name="interruptor", spawn={"task": "interruptee"}, service_time=5, next="stop"
    )
    def interruptor(task_id):
        return {"event_type": "waited"}

    @app.task(name="stop", interrupt={"task": "interruptee", "cause": "interrupt!"})
    def stop(task_id):
        return {"event_type": "interrupt"}

    @app.task(
        name="interruptee",
        service_time=10,
        on_interrupt="abandon",
        on_interrupt_next="caught",
    )
    def interruptee(task_id):
        return {"event_type": "finished"}

    caught_payload = Template(
        {"event_type": "interrupted", "cause": {"gen": "expr", "expr": "cause.value"}},
        name="caught",
    )

    @app.task(name="caught")
    def caught(task_id):
        return caught_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

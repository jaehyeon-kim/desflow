"""GD-TT2: Three interrupts at one time, each with its own cause.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("fox", size, spawn="fox")
    app.add_initial_arrival("boggis", 1, spawn="farmer", attributes={"name": "boggis"})
    app.add_initial_arrival("bunce", 1, spawn="farmer", attributes={"name": "bunce"})
    app.add_initial_arrival("beans", 1, spawn="farmer", attributes={"name": "beans"})

    @app.task(
        name="fox",
        service_time=10,
        on_interrupt="restart",
        on_interrupt_run="note_cause",
        next="fox",
    )
    def fox(task_id):
        return {"event_type": "ran"}

    note_cause_payload = Template(
        {"event_type": "interrupted", "cause": {"gen": "expr", "expr": "cause.value"}},
        name="note_cause",
    )

    @app.task(name="note_cause")
    def note_cause(task_id):
        return note_cause_payload

    @app.task(
        name="farmer",
        interrupt={
            "task": "fox",
            "cause": generator({"gen": "expr", "expr": "attr.name"}),
        },
    )
    def farmer(task_id):
        return {"event_type": "farmer"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

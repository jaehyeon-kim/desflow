"""ST-U02: A subscriber interrupted when another process ends.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("never", once=True)
    app.add_initial_arrival("s", size, spawn="subscriber")

    @app.task(name="other", service_time=3)
    def other(task_id):
        return {"event_type": "other_done", "value": "ohai"}

    @app.task(
        name="subscriber",
        subscribe={"task": "other"},
        wait={"signal": "never"},
        on_interrupt="abandon",
        on_interrupt_next="told",
    )
    def subscriber(task_id):
        return {"event_type": "should_not_finish"}

    told_payload = Template(
        {
            "event_type": "interrupted",
            "at": {"gen": "expr", "expr": "t"},
            "value": {"gen": "expr", "expr": "cause.value.value.value"},
        },
        name="told",
    )

    @app.task(name="told")
    def told(task_id):
        return told_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

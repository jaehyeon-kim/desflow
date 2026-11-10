"""ST-U05: A subscriber interrupted by a timeout with a value.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("to", once=True)
    app.add_initial_arrival("m", size, spawn="make")
    app.add_initial_arrival("p", 1, spawn="pem")

    @app.task(name="make", on=[{"fire": {"signal": "to", "after": 2, "value": "ohai"}}])
    def make(task_id):
        return {"event_type": "made"}

    @app.task(
        name="pem",
        subscribe={"signal": "to"},
        service_time=10,
        on_interrupt="abandon",
        on_interrupt_next="told",
    )
    def pem(task_id):
        return {"event_type": "should_not_finish"}

    told_payload = Template(
        {
            "event_type": "interrupted",
            "at": {"gen": "expr", "expr": "t"},
            "cause": {"gen": "expr", "expr": "cause.value.value"},
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

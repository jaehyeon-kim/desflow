"""ST-R14: Five processes with priorities and preempt flags.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1, queue="priority", preempt=True)
    app.add_initial_arrival(
        "p0", size, spawn="p", attributes={"delay": 0, "prio": 2, "can": True}
    )
    app.add_initial_arrival(
        "p1", 1, spawn="p", attributes={"delay": 0, "prio": 2, "can": True}
    )
    app.add_initial_arrival(
        "p2", 1, spawn="p", attributes={"delay": 1, "prio": 1, "can": False}
    )
    app.add_initial_arrival(
        "p3", 1, spawn="p", attributes={"delay": 3, "prio": 0, "can": True}
    )
    app.add_initial_arrival(
        "p4", 1, spawn="p", attributes={"delay": 4, "prio": 3, "can": True}
    )

    @app.task(name="p", service_time="attr.delay", next="use")
    def p(task_id):
        return {"event_type": "start"}

    @app.task(
        name="use",
        resource_id="res",
        priority="attr.prio",
        preempt="attr.can",
        service_time=2,
        on_interrupt="abandon",
        on_interrupt_next="lost",
    )
    def use(task_id):
        return {"event_type": "done"}

    lost_payload = Template(
        {
            "event_type": "preempted",
            "by": {"gen": "expr", "expr": "cause.by"},
            "since": {"gen": "expr", "expr": "cause.usage_since"},
        },
        name="lost",
    )

    @app.task(name="lost")
    def lost(task_id):
        return lost_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

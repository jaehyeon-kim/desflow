"""ST-P08: A failed child that nobody waits for any more stops the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_variable("target", -1)
    app.add_initial_arrival(
        "p",
        size,
        spawn="parent",
        attributes={"id": generator({"gen": "sequence", "start": 0})},
    )

    @app.task(
        name="parent",
        spawn={"task": "child_a", "copy_attributes": True},
        next="start_b",
    )
    def parent(task_id):
        return {"event_type": "parent"}

    @app.task(name="start_b", open={"b": {"task": "child_b"}}, next="join")
    def start_b(task_id):
        return {"event_type": "start_b"}

    @app.task(
        name="join",
        wait={"handle": "b"},
        on_interrupt="abandon",
        on_interrupt_next="after",
    )
    def join(task_id):
        return {"event_type": "joined"}

    @app.task(name="after", service_time=0)
    def after(task_id):
        return {"event_type": "after"}

    @app.task(
        name="child_a",
        on=[
            {
                "path": "T.variables.target",
                "value": generator({"gen": "expr", "expr": "attr.id"}),
            },
            {
                "interrupt": {
                    "task": "join",
                    "where": "attr.id == registry['T.variables.target']",
                }
            },
        ],
    )
    def child_a(task_id):
        return {"event_type": "a"}

    @app.task(name="child_b", fail="True", fail_reason="spam")
    def child_b(task_id):
        return {"event_type": "b"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(on_unhandled_failure="stop")

"""ST-I06: Two interrupts reach a process that ends after the first.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="parent")

    @app.task(
        name="parent",
        open={"c": {"task": "child"}},
        wait={"timeout": 0},
        interrupt={"task": "child", "cause": 1},
        next="second",
    )
    def parent(task_id):
        return {"event_type": "go"}

    @app.task(name="second", interrupt={"task": "child", "cause": 2}, next="join")
    def second(task_id):
        return {"event_type": "second"}

    join_payload = Template(
        {
            "event_type": "joined",
            "result": {"gen": "expr", "expr": "wait.c.value.cause"},
        },
        name="join",
    )

    @app.task(name="join", wait={"handle": "c"})
    def join(task_id):
        return join_payload

    @app.task(
        name="child",
        service_time=1,
        on_interrupt="abandon",
        on_interrupt_next="return_cause",
    )
    def child(task_id):
        return {"event_type": "finished"}

    return_cause_payload = Template(
        {"cause": {"gen": "expr", "expr": "cause.value"}}, name="return_cause"
    )

    @app.task(name="return_cause")
    def return_cause(task_id):
        return return_cause_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""ST-U09: All of with a failing process that the parent catches.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")

    @app.task(
        name="pem",
        wait={
            "all": {
                "a": {"timeout": {"after": 1, "value": 1}},
                "x": {"task": "child_with_error"},
                "c": {"timeout": {"after": 3, "value": 3}},
            }
        },
        on_fail="caught",
    )
    def pem(task_id):
        return {"event_type": "unreachable"}

    caught_payload = Template(
        {
            "event_type": "caught",
            "reason": {"gen": "expr", "expr": "upstream.reason"},
            "at": {"gen": "expr", "expr": "t"},
        },
        name="caught",
    )

    @app.task(name="caught")
    def caught(task_id):
        return caught_payload

    @app.task(
        name="child_with_error", service_time=2, fail="True", fail_reason="crashing"
    )
    def child_with_error(task_id):
        return {"event_type": "child"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

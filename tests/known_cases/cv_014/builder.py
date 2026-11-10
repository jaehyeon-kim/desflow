"""CV-014: A callback on a timeout.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_variable("seen", 0)
    app.add_initial_arrival("a", size, spawn="begin")

    @app.task(
        name="begin",
        open={"t": {"timeout": {"after": 2, "value": "v"}}},
        listen={"handle": "t", "on": [{"path": "C.variables.seen", "value": 1}]},
        next="later",
    )
    def begin(task_id):
        return {"event_type": "begin"}

    later_payload = Template(
        {
            "event_type": "later",
            "value": {"gen": "expr", "expr": "wait.t.value"},
            "seen": {"gen": "expr", "expr": "registry['C.variables.seen']"},
        },
        name="later",
    )

    @app.task(name="later", wait={"handle": "t"})
    def later(task_id):
        return later_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""ST-C03: Or of three timeouts continues at the first.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")
    pem_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "first": {"gen": "expr", "expr": "wait.t0.value"},
            "in_result": {
                "gen": "expr",
                "expr": "[wait.t0.done, wait.t1.done, wait.t2.done]",
            },
        },
        name="pem",
    )

    @app.task(
        name="pem",
        wait={
            "any": {
                "t0": {"timeout": {"after": 0, "value": 0}},
                "t1": {"timeout": {"after": 1, "value": 1}},
                "t2": {"timeout": {"after": 2, "value": 2}},
            }
        },
    )
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

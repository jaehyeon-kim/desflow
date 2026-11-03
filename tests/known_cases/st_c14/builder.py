"""ST-C14: The result of a condition does not change later.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="pem")
    pem_payload = Template(
        {
            "event_type": "first_done",
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
                "both": {
                    "all": {
                        "t1": {"timeout": {"after": 1, "value": 1}},
                        "t2": {"timeout": {"after": 2, "value": 2}},
                    }
                },
            }
        },
        set={
            "seen": generator(
                {"gen": "expr", "expr": "[wait.t0.done, wait.t1.done, wait.t2.done]"}
            )
        },
        next="later",
    )
    def pem(task_id):
        return pem_payload

    later_payload = Template(
        {"event_type": "later", "in_result": {"gen": "expr", "expr": "attr.seen"}},
        name="later",
    )

    @app.task(name="later", service_time=2)
    def later(task_id):
        return later_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

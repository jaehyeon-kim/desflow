"""CV-112: Handle a failure by its type.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival(
        "a",
        size,
        spawn="step",
        attributes={"i": generator({"gen": "sequence", "start": 0})},
    )

    @app.task(
        name="step",
        fail="True",
        fail_reason=generator(
            {
                "gen": "expr",
                "expr": "['KeyError', 'ValueError', 'ValueError'][attr.i % 3]",
            }
        ),
        on_fail="handle",
    )
    def step(task_id):
        return {"event_type": "step"}

    @app.task(
        name="handle",
        next={
            "when": [
                {"condition": "upstream.reason == 'KeyError'", "task": "key_handler"}
            ],
            "default": "other_handler",
        },
    )
    def handle(task_id):
        return {"event_type": "handled"}

    @app.task(name="key_handler")
    def key_handler(task_id):
        return {"event_type": "key_error"}

    @app.task(name="other_handler")
    def other_handler(task_id):
        return {"event_type": "other"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

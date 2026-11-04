"""CV-139: Text built from values.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="say", attributes={"n": 3})
    say_payload = Template(
        {
            "event_type": "say",
            "text": {
                "gen": "format",
                "template": "item {n}",
                "names": {"n": {"gen": "attr", "name": "n"}},
            },
            "text2": {"gen": "expr", "expr": "'count ' + str(attr.n)"},
        },
        name="say",
    )

    @app.task(name="say")
    def say(task_id):
        return say_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

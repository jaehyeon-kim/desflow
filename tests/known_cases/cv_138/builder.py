"""CV-138: Random integers, choices, samples and shuffles.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="draw")
    draw_payload = Template(
        {
            "event_type": "draw",
            "n": {"gen": "integer", "bounds": [1, 6]},
            "c": {"gen": "one_of", "choices": ["a", "b", "c"]},
            "s": {"gen": "pick", "choices": ["a", "b", "c", "d"], "count": 2},
        },
        name="tasks.draw",
    )

    @app.task(name="draw")
    def draw(task_id):
        return draw_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

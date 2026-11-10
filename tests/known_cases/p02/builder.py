"""P02: An attribute drawn for each entity.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival(
        "a",
        "exponential",
        rate=1,
        attributes={
            "x": generator({"gen": "sample", "dist": "exponential", "rate": 2})
        },
        spawn="t",
    )
    t_payload = Template(
        {"event_type": "seen", "x": {"gen": "attr", "name": "x"}}, name="t"
    )

    @app.task(name="t")
    def t(task_id):
        return t_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

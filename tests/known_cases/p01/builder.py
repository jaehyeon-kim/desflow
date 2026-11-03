"""P01: Fixed starting attributes.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival(
        "a",
        "exponential",
        rate=1,
        attributes={"kind": "standard", "size": 3},
        spawn="t",
    )
    t_payload = Template(
        {"event_type": "seen", "kind": {"gen": "attr", "name": "kind"}}, name="t"
    )

    @app.task(name="t")
    def t(task_id):
        return t_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

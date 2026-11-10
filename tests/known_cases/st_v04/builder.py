"""ST-V04: Read the value of an event before it triggers.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_variable("value_raised", False)
    app.add_signal("ev", once=True)
    app.add_initial_arrival("a", size, spawn="pem")
    pem_payload = Template(
        {"event_type": "early", "value": {"gen": "expr", "expr": "signal.ev.value"}},
        name="pem",
    )

    @app.task(
        name="pem",
        on=[
            {
                "path": "T.variables.value_raised",
                "value": generator({"gen": "expr", "expr": "signal.ev.count == 0"}),
            }
        ],
    )
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

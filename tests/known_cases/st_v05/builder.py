"""ST-V05: An event succeeded before the process waits.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_signal("ev", once=True)
    app.add_initial_arrival("m", size, spawn="maker")
    app.add_initial_arrival("w", 1, spawn="pem")

    @app.task(
        name="maker", on=[{"fire": {"signal": "ev", "value": "i was already done"}}]
    )
    def maker(task_id):
        return {"event_type": "made"}

    pem_payload = Template(
        {"event_type": "got", "value": {"gen": "expr", "expr": "wait.ev.value"}},
        name="pem",
    )

    @app.task(name="pem", wait={"signal": "ev"})
    def pem(task_id):
        return pem_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

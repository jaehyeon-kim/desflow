"""P29: Seize two units of a pool.

Builder form of `model.yaml`.
"""

from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).parent


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_container("pool", current_cap=3, level=3)
    app.add_service("s", dist="constant", value=1)
    app.add_arrival_replay(
        "a", HERE / "arr.csv", time="time", unit="s", fields=["n"], spawn="job"
    )

    @app.task(
        name="job",
        service_id="s",
        get={"container": "pool", "amount": "attr.n"},
        put={"container": "pool", "amount": "attr.n"},
    )
    def job(task_id):
        return {"event_type": "served"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

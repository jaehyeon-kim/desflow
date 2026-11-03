"""ST-R03: Nine users on three units.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 9) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=3)
    app.add_initial_arrival(
        "users",
        size,
        key=lambda n: str(n - 1),
        spawn="pem",
    )

    @app.task(name="pem", resource_id="res", service_time=1)
    def pem(task_id):
        return {"event_type": "granted"}

    return app


def run(app: SimulationContext):
    """Runs until no event is left: the Builder source has no end of its own, as `model.yaml` has no `run` section."""
    return app.run()

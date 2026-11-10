"""DX-8: A resource added during the run and a job on every resource each round.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, strict=False)
    app.add_resource("packers", current_cap=1, max_cap=5)
    app.add_resource("wrappers", current_cap=0, max_cap=5)
    app.add_arrival("pack_rounds", "constant", value=4, spawn="pack_job")
    app.add_arrival("wrap_rounds", "constant", value=4, first_at=12, spawn="wrap_job")
    app.add_scenario(
        [
            {
                "at": 12,
                "path": "Shop.resources.wrappers.current_cap",
                "value": 2,
                "record": {"status": "wrappers_added", "capacity": 2},
            }
        ]
    )

    @app.task(name="pack_job", resource_id="packers", service_time=3)
    def pack_job(task_id):
        return {"resource": "packers", "status": "finished"}

    @app.task(name="wrap_job", resource_id="wrappers", service_time=3)
    def wrap_job(task_id):
        return {"resource": "wrappers", "status": "finished"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

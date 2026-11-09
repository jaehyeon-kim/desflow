"""The Add Resources Mid-run guide, builder API: the gift wrappers exist from the start
with no units, and a scenario step gives them 2 units at 12 seconds.

`build()` returns the app. Run it with: python dynamic_topology_scenario_builder.py
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_resource("packers", current_cap=1, max_cap=5)
    # --8<-- [start:scenario]
    # No units yet: a job that asks for one waits
    app.add_resource("wrappers", current_cap=0, max_cap=5)
    # At 12 seconds the wrappers get 2 units, and the step records what it did
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
    # --8<-- [end:scenario]
    app.add_arrival("pack_rounds", "constant", value=4, spawn="pack_job")
    app.add_arrival("wrap_rounds", "constant", value=4, first_at=12, spawn="wrap_job")

    @app.task(name="pack_job", resource_id="packers", service_time=3)
    def pack_job(task_id):
        return {"resource": "packers", "status": "finished"}

    @app.task(name="wrap_job", resource_id="wrappers", service_time=3)
    def wrap_job(task_id):
        return {"resource": "wrappers", "status": "finished"}

    return app


if __name__ == "__main__":
    build().run(until=20)

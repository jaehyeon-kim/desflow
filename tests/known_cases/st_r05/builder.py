"""ST-R05: A queued victim interrupted releases its request.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("res", current_cap=1)
    app.add_initial_arrival("blocker", size, spawn="blocker")
    app.add_initial_arrival("victim", 1, spawn="victim")
    app.add_initial_arrival("poke", 1, spawn="poker")

    @app.task(name="blocker", resource_id="res", service_time=1)
    def blocker(task_id):
        return {"event_type": "blocker_done"}

    @app.task(name="victim", resource_id="res", service_time=1, on_interrupt="abandon")
    def victim(task_id):
        return {"event_type": "victim_done"}

    @app.task(name="poker", interrupt={"task": "victim"})
    def poker(task_id):
        return {"event_type": "poke"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

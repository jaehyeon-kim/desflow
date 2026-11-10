"""GD-TT3: Interrupted while queued, keeping the same request.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("a", size, spawn="hold")
    app.add_initial_arrival("b", 1, spawn="victim")
    app.add_initial_arrival("i", 1, spawn="poke")

    @app.task(name="hold", resource_id="m", service_time=1)
    def hold(task_id):
        return {"event_type": "hold_done"}

    @app.task(name="victim", wait={"request": "m"}, on_interrupt="resume")
    def victim(task_id):
        return {"event_type": "victim_done"}

    @app.task(name="poke", interrupt={"task": "victim"})
    def poke(task_id):
        return {"event_type": "poke"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

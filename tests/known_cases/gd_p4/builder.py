"""GD-P4: Wait again for the same request after an interrupt.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("blocker", size, spawn="block")
    app.add_initial_arrival("victim", 1, spawn="victim")
    app.add_arrival("poke", "constant", value=1)

    @app.task(name="block", resource_id="m", service_time=3)
    def block(task_id):
        return {"event_type": "blocked_done"}

    @app.task(
        name="victim", wait={"request": "m"}, on_interrupt="resume", service_time=1
    )
    def victim(task_id):
        return {"event_type": "victim_done"}

    @app.task(name="poker", interrupt={"task": "victim", "cause": "x"})
    def poker(task_id):
        return {"event_type": "poke"}

    @app.arrival_loop("poke")
    def poke_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("poke")
            context.spawn(poker(context.next_instance("poker"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

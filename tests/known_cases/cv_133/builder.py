"""CV-133: An interrupt during a queued request.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("h", size, spawn="hold")
    app.add_arrival("a", "constant", value=0.5)
    app.add_arrival("b", "constant", value=0.5)
    app.add_arrival("c", "constant", value=0.5)
    app.add_arrival("i", "constant", value=1)

    @app.task(name="hold", resource_id="m", service_time=3)
    def hold(task_id):
        return {"event_type": "hold"}

    @app.task(name="left", resource_id="m", service_time=1, on_interrupt="abandon")
    def left(task_id):
        return {"event_type": "left_served"}

    @app.task(name="kept", wait={"request": "m"}, on_interrupt="resume", service_time=1)
    def kept(task_id):
        return {"event_type": "kept_served"}

    @app.task(name="again", resource_id="m", service_time=1, on_interrupt="resume")
    def again(task_id):
        return {"event_type": "again_served"}

    @app.task(name="poke", interrupt={"task": "left", "cause": "x"}, next="poke_kept")
    def poke(task_id):
        return {"event_type": "poke"}

    @app.task(
        name="poke_kept", interrupt={"task": "kept", "cause": "x"}, next="poke_again"
    )
    def poke_kept(task_id):
        return {"event_type": "poke_kept"}

    @app.task(name="poke_again", interrupt={"task": "again", "cause": "x"})
    def poke_again(task_id):
        return {"event_type": "poke_again"}

    @app.arrival_loop("a")
    def a_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("a")
            context.spawn(left(context.next_instance("left"), entity=entity))

    @app.arrival_loop("b")
    def b_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("b")
            context.spawn(kept(context.next_instance("kept"), entity=entity))

    @app.arrival_loop("c")
    def c_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("c")
            context.spawn(again(context.next_instance("again"), entity=entity))

    @app.arrival_loop("i")
    def i_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("i")
            context.spawn(poke(context.next_instance("poke"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

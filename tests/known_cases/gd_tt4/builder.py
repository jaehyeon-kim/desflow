"""GD-TT4: Interrupted during service, keeping the unit.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("a", 1, spawn="user")
    app.add_arrival("b", "constant", value=0.5)
    app.add_arrival("i", "constant", value=1)

    @app.task(name="user", wait={"request": "m"}, on_interrupt="resume", service_time=2)
    def user(task_id):
        return {"event_type": "user_done"}

    @app.task(name="waiting", resource_id="m", service_time=1)
    def waiting(task_id):
        return {"event_type": "waiting_done"}

    @app.task(name="poke", interrupt={"task": "user"})
    def poke(task_id):
        return {"event_type": "poke"}

    @app.arrival_loop("b")
    def b_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("b")
            context.spawn(waiting(context.next_instance("waiting"), entity=entity))

    @app.arrival_loop("i")
    def i_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("i")
            context.spawn(poke(context.next_instance("poke"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

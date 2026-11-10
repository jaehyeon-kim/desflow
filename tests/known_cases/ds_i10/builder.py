"""DS-I10: A wait through a handle that goes on after an interrupt.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="I10", factor=0, random_seed=42)
    app.add_resource("m", current_cap=1)
    app.add_initial_arrival("a", size, spawn="waiter")
    app.add_arrival("b", "constant", value=2)

    @app.task(
        name="waiter",
        open={"limit": {"timeout": 5}},
        wait={"handle": "limit"},
        on_interrupt="resume",
    )
    def waiter(task_id):
        return {"event_type": "ended"}

    @app.task(name="poke", interrupt={"task": "waiter", "cause": "x"})
    def poke(task_id):
        return {"event_type": "poke"}

    @app.arrival_loop("b")
    def b_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("b")
            context.spawn(poke(context.next_instance("poke"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

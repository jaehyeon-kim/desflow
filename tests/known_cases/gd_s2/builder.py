"""GD-S2: Wait until a condition holds.

Builder form of `model.yaml`.
"""

from desflow import NO_RECORD, SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="S", factor=0, random_seed=42)
    app.add_container("tank", current_cap=100, level=0)
    app.add_initial_arrival("w", size, spawn="poll")
    app.add_arrival("f", "constant", value=1.5)

    @app.task(
        name="poll",
        wait={"timeout": 1},
        next={
            "when": [{"condition": "level.tank >= 30", "task": "waiter"}],
            "default": "poll",
        },
    )
    def poll(task_id):
        return NO_RECORD

    @app.task(name="waiter")
    def waiter(task_id):
        return {"event_type": "waituntil_done"}

    @app.task(name="fill", put={"container": "tank", "amount": 10})
    def fill(task_id):
        return {"event_type": "filled"}

    @app.arrival_loop("f")
    def f_arrivals(context):
        for _ in range(4):
            entity = yield context.wait_for_arrival("f")
            context.spawn(fill(context.next_instance("fill"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

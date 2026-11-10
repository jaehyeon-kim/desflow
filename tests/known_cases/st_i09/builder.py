"""ST-I09: Two interrupts at once while waiting on timeouts of 0.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="first_wait")
    app.add_initial_arrival("b", 1, spawn="poke_twice")

    @app.task(
        name="first_wait",
        wait={"timeout": 0},
        on_interrupt="abandon",
        on_interrupt_next="second_wait",
    )
    def first_wait(task_id):
        return {"event_type": "finished"}

    @app.task(
        name="second_wait",
        wait={"timeout": 0},
        on_interrupt="abandon",
        on_interrupt_next="end",
    )
    def second_wait(task_id):
        return {"event_type": "finished"}

    @app.task(name="end")
    def end(task_id):
        return {"event_type": "end"}

    @app.task(name="poke_twice", interrupt={"task": "first_wait"}, next="poke_again")
    def poke_twice(task_id):
        return {"event_type": "poke"}

    @app.task(name="poke_again", interrupt={"task": "first_wait"})
    def poke_again(task_id):
        return {"event_type": "poke_again"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

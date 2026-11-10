"""DX-14: A request yielded again after an interrupt.

Builder form of `model.yaml`.
"""

from desflow import NO_RECORD, SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_resource("machine", current_cap=1)
    app.add_initial_arrival("h", size, spawn="holder")
    app.add_initial_arrival("f", 1, spawn="first")
    app.add_arrival("s", "constant", value=1, spawn="second", max_events=1)
    app.add_arrival("p", "constant", value=2, spawn="pause", max_events=1)

    @app.task(name="holder", resource_id="machine", service_time=5)
    def holder(task_id):
        return {"job": "holder"}

    @app.task(
        name="first",
        open={"slot": {"request": "machine"}},
        wait={"handle": "slot"},
        on_interrupt="resume",
        next="first_hold",
    )
    def first(task_id):
        return NO_RECORD

    @app.task(name="first_hold", service_time=1, release={"handle": "slot"})
    def first_hold(task_id):
        return {"job": "first"}

    @app.task(name="second", resource_id="machine", service_time=1)
    def second(task_id):
        return {"job": "second"}

    @app.task(name="pause", interrupt={"task": "first", "cause": "pause"})
    def pause(task_id):
        return NO_RECORD

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

"""DX-13: A wait on the same event again after an interrupt.

Builder form of `model.yaml`.
"""

from desflow import NO_RECORD, SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_initial_arrival("a", size, spawn="job")
    app.add_arrival("b", "constant", value=2, spawn="pause", max_events=1)

    @app.task(
        name="job",
        open={"limit": {"timeout": 5}},
        wait={"handle": "limit"},
        on_interrupt="resume",
    )
    def job(task_id):
        return {"job": "job-1"}

    @app.task(name="pause", interrupt={"task": "job", "cause": "pause"})
    def pause(task_id):
        return NO_RECORD

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

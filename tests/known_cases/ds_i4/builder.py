"""DS-I4: Suspend until another task resumes.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 3) -> SimulationContext:
    app = SimulationContext(sim_id="I4", factor=0, random_seed=42)
    app.add_initial_arrival("parked", size, spawn="park")
    app.add_arrival("waker", "constant", value=2)
    park_payload = Template(
        {"event_type": "woke", "got": {"gen": "expr", "expr": "wait.suspend.value"}},
        name="park",
    )

    @app.task(name="park", wait={"suspend": True})
    def park(task_id):
        return park_payload

    @app.task(
        name="wake",
        resume={
            "task": "park",
            "where": "process.instance == 1",
            "value": 9,
            "limit": 1,
        },
    )
    def wake(task_id):
        return {"event_type": "wake"}

    @app.arrival_loop("waker")
    def waker_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("waker")
            context.spawn(wake(context.next_instance("wake"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

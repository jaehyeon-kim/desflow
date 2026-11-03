"""GD-S3: Interrupt a process and read the cause.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="S", factor=0, random_seed=42)
    app.add_initial_arrival("v", 1, spawn="victim")
    app.add_arrival("i", "constant", value=1)

    @app.task(
        name="victim",
        service_time=10,
        on_interrupt="abandon",
        on_interrupt_next="caught",
    )
    def victim(task_id):
        return {"event_type": "done"}

    caught_payload = Template(
        {"event_type": "interrupted", "cause": {"gen": "expr", "expr": "cause.value"}},
        name="caught",
    )

    @app.task(name="caught")
    def caught(task_id):
        return caught_payload

    @app.task(name="hit", interrupt={"task": "victim", "cause": "Spam"})
    def hit(task_id):
        return {"event_type": "hit"}

    @app.arrival_loop("i")
    def i_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("i")
            context.spawn(hit(context.next_instance("hit"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

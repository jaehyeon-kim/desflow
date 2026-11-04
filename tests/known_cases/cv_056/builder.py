"""CV-056: Decide on the cause, then resume.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival("w", 1, spawn="work")
    app.add_arrival("i", "constant", value=3)

    @app.task(
        name="work", service_time=10, on_interrupt="abandon", on_interrupt_next="decide"
    )
    def work(task_id):
        return {"event_type": "done"}

    @app.task(
        name="decide",
        set={"left": generator({"gen": "expr", "expr": "cause.left"})},
        next={
            "when": [{"condition": "cause.value == 'resume'", "task": "finish"}],
            "default": "stop",
        },
    )
    def decide(task_id):
        return {"event_type": "decide"}

    @app.task(name="finish", service_time="attr.left")
    def finish(task_id):
        return {"event_type": "finished"}

    @app.task(name="stop")
    def stop(task_id):
        return {"event_type": "stopped"}

    @app.task(name="hit", interrupt={"task": "work", "cause": "resume"})
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

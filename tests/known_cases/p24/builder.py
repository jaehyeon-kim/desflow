"""P24: A loop with a counter.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="work", attributes={"tries": 0})

    @app.task(
        name="work",
        service_time=1,
        set={"tries": generator({"gen": "expr", "expr": "attr.tries + 1"})},
        next={
            "when": [{"condition": "attr.tries < 3", "task": "work"}],
            "default": "done",
        },
    )
    def work(task_id):
        return {"event_type": "tried"}

    done_payload = Template(
        {"event_type": "done", "tries": {"gen": "attr", "name": "tries"}}, name="done"
    )

    @app.task(name="done")
    def done(task_id):
        return done_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""CV-116: A nested generator.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="main", attributes={"d": 1, "total": 0})

    @app.task(name="main", set={"back": "first_return"}, next="shared")
    def main(task_id):
        return {"event_type": "main"}

    @app.task(
        name="shared",
        service_time="attr.d",
        set={"total": generator({"gen": "expr", "expr": "attr.total + attr.d * 2"})},
        next={
            "when": [
                {"condition": "attr.back == 'first_return'", "task": "first_return"}
            ],
            "default": "second_return",
        },
    )
    def shared(task_id):
        return {"event_type": "shared"}

    @app.task(name="first_return", set={"back": "second_return", "d": 2}, next="shared")
    def first_return(task_id):
        return {"event_type": "first_return"}

    second_return_payload = Template(
        {
            "event_type": "second_return",
            "at": {"gen": "expr", "expr": "t"},
            "total": {"gen": "expr", "expr": "attr.total"},
        },
        name="second_return",
    )

    @app.task(name="second_return")
    def second_return(task_id):
        return second_return_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""CV-021: The events of a condition and at least.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="two_of_three")
    app.add_initial_arrival("b", 1, spawn="all_three")
    app.add_initial_arrival("c", 1, spawn="any_of_three")
    branches = {
        "a": {"timeout": {"after": 3, "value": 30}},
        "b": {"timeout": {"after": 1, "value": 10}},
        "c": {"timeout": {"after": 2, "value": 20}},
    }
    for name, wait in (
        ("two_of_three", {"at_least": 2, "of": branches}),
        ("all_three", {"all": branches}),
        ("any_of_three", {"any": branches}),
    ):
        payload = Template(
            {
                "event_type": "done",
                "at": {"gen": "expr", "expr": "t"},
                "a": {"gen": "expr", "expr": "wait.a.value"},
                "b": {"gen": "expr", "expr": "wait.b.value"},
                "c": {"gen": "expr", "expr": "wait.c.value"},
            },
            name=name,
        )
        app.task(name=name, wait=wait)(lambda task_id, payload=payload: payload)

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

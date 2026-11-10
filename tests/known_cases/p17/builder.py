"""P17: Set a computed attribute.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="t", attributes={"n": 0})

    @app.task(
        name="t",
        set={
            "n": generator({"gen": "expr", "expr": "attr.n + 1"}),
            "t0": generator({"gen": "expr", "expr": "t"}),
        },
        service_time=1,
        next="u",
    )
    def t(task_id):
        return {"event_type": "set"}

    u_payload = Template(
        {
            "event_type": "read",
            "n": {"gen": "attr", "name": "n"},
            "t0": {"gen": "attr", "name": "t0"},
        },
        name="u",
    )

    @app.task(name="u")
    def u(task_id):
        return u_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

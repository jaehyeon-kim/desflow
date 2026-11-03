"""ST-U16: Any of events that have already been processed.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_initial_arrival("p", size, spawn="pem")

    @app.task(
        name="pem",
        open={
            "a": {"timeout": {"after": 0, "value": "spam"}},
            "b": {"timeout": {"after": 1, "value": "eggs"}},
        },
        service_time=2,
        next="collect",
    )
    def pem(task_id):
        return {"event_type": "opened"}

    collect_payload = Template(
        {
            "event_type": "done",
            "at": {"gen": "expr", "expr": "t"},
            "values": [
                {"gen": "expr", "expr": "wait.a.value"},
                {"gen": "expr", "expr": "wait.b.value"},
            ],
        },
        name="collect",
    )

    @app.task(
        name="collect", wait={"any": {"a": {"handle": "a"}, "b": {"handle": "b"}}}
    )
    def collect(task_id):
        return collect_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

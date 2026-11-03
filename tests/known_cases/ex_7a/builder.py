"""EX-7a: One generator and one consumer joined by a store.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Pipe", factor=0, random_seed=42)
    app.add_store(
        "pipe",
    )
    app.add_arrival(
        "generator",
        gap=generator({"gen": "integer", "bounds": [6, 10]}),
        spawn="speak",
    )
    app.add_initial_arrival("consumer", size, spawn="listen")

    @app.task(
        name="speak",
        put={
            "store": "pipe",
            "wait": False,
            "item": {
                "sent": generator({"gen": "expr", "expr": "t"}),
                "text": generator(
                    {
                        "gen": "format",
                        "template": "Generator A says hello at {now:g}",
                        "names": {"now": {"gen": "expr", "expr": "t"}},
                    }
                ),
            },
        },
    )
    def speak(task_id):
        return {"event_type": "said"}

    listen_payload = Template(
        {
            "event_type": "received",
            "late": {"gen": "expr", "expr": "item.sent < t"},
            "text": {"gen": "expr", "expr": "item.text"},
        },
        name="listen",
    )

    @app.task(name="listen", get={"store": "pipe"}, next="busy")
    def listen(task_id):
        return listen_payload

    @app.task(
        name="busy",
        service_time=generator({"gen": "integer", "bounds": [4, 8]}),
        next="listen",
    )
    def busy(task_id):
        return {"event_type": "handled"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=100)

"""EX-6: Messages that reach a store after a fixed delay.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Latency", factor=0, random_seed=42)
    app.add_store(
        "cable",
    )
    app.add_arrival("sender", "constant", value=5, spawn="send")
    app.add_initial_arrival("receiver", size, spawn="receive")

    @app.task(
        name="send",
        spawn={
            "task": "latency",
            "args": {
                "msg": generator(
                    {
                        "gen": "format",
                        "template": "Sender sent this at {now:g}",
                        "names": {"now": {"gen": "expr", "expr": "t"}},
                    }
                )
            },
        },
    )
    def send(task_id):
        return {"event_type": "sent"}

    @app.task(
        name="latency",
        service_time=10,
        put={"store": "cable", "item": generator({"gen": "expr", "expr": "arg.msg"})},
    )
    def latency(task_id):
        return {"event_type": "delivered"}

    receive_payload = Template(
        {"event_type": "received", "msg": {"gen": "expr", "expr": "item"}},
        name="receive",
    )

    @app.task(name="receive", get={"store": "cable"}, next="receive")
    def receive(task_id):
        return receive_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=100)

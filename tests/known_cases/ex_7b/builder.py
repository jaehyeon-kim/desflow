"""EX-7b: One generator that broadcasts to several pipes.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Pipe", factor=0, random_seed=42)
    app.add_store("pipe", family=True, members=2)
    app.add_arrival("generator", "constant", value=8, spawn="speak")
    app.add_initial_arrival("consumer_a", size, spawn="listen", attributes={"idx": 0})
    app.add_initial_arrival("consumer_b", 1, spawn="listen", attributes={"idx": 1})
    speak_payload = Template(
        {
            "event_type": "said",
            "sent": {"gen": "expr", "expr": "t"},
            "text": {
                "gen": "format",
                "template": "Generator A says hello at {now:g}",
                "names": {"now": {"gen": "expr", "expr": "t"}},
            },
        },
        name="speak",
    )

    @app.task(name="speak", next="send")
    def speak(task_id):
        return speak_payload

    @app.task(
        name="send",
        open={
            "sends": {
                "each": "members(pipe)",
                "branch": {"put": {"store": {"of": "pipe", "key": "each.value"}}},
            }
        },
    )
    def send(task_id):
        return {"event_type": "sent"}

    listen_payload = Template(
        {
            "event_type": "received",
            "late": {"gen": "expr", "expr": "item.sent < t"},
            "text": {"gen": "expr", "expr": "item.text"},
        },
        name="listen",
    )

    @app.task(
        name="listen", get={"store": {"of": "pipe", "key": "attr.idx"}}, next="busy"
    )
    def listen(task_id):
        return listen_payload

    @app.task(name="busy", service_time=6, next="listen")
    def busy(task_id):
        return {"event_type": "handled"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=100)

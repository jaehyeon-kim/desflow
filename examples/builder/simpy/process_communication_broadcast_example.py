"""Process Communication: SimPy's process_communication.py example, broadcast, builder API.

A generator makes a message every 6 to 10 seconds, stamped with the time it was sent,
and puts a copy into each of two pipes without waiting. Two consumers each take the
messages of their own pipe and work on each for 4 to 8 seconds, so they are late for
some messages: `late` is true when the message waited in its pipe.

Each value is drawn from desflow's random sequence for its place in the model, so the
numbers differ from those SimPy prints.

Needs no containers. Runs unpaced and ends after 100 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    """Builds the two pipes, with `size` consumers on the first."""
    app = SimulationContext(sim_id="Pipes", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    app.add_store("pipe", family=True, members=2)
    app.add_arrival(
        "generator",
        gap=generator({"gen": "integer", "bounds": [6, 10]}),
        spawn="speak",
    )
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

    # The message is the payload of `speak`, which each put of `send` takes as its item
    @app.task(name="speak", next="send")
    def speak(task_id):
        """Makes a message."""
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
        """Puts the message into every pipe."""
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
        """Takes the next message of its own pipe."""
        return listen_payload

    @app.task(
        name="busy",
        service_time=generator({"gen": "integer", "bounds": [4, 8]}),
        next="listen",
    )
    def busy(task_id):
        """Works on the message."""
        return {"event_type": "handled"}

    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs 100 simulation seconds."""
    model.run(until=100)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

"""Process Communication: SimPy's process_communication.py example, one to one, builder API.

A generator puts a message into a store every 6 to 10 seconds, stamped with the time
it was sent. A consumer takes each message and then works on it for 4 to 8 seconds, so
it is late for some messages: `late` is true when the message waited in the store.
`process_communication_broadcast_example.py` sends each message to two consumers.

Each value is drawn from desflow's random sequence for its place in the model, so the
numbers differ from those SimPy prints.

Needs no containers. Runs unpaced and ends after 100 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    """Builds the pipe, with `size` consumers."""
    app = SimulationContext(sim_id="Pipes", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    app.add_store("pipe")
    app.add_arrival(
        "generator",
        gap=generator({"gen": "integer", "bounds": [6, 10]}),
        spawn="speak",
    )
    app.add_initial_arrival("consumer", size, spawn="listen")

    # The generator does not wait for its put, as SimPy's `out_pipe.put(msg)` does not
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
        """Puts a message into the pipe."""
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
        """Takes the next message."""
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

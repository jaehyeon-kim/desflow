"""Event Latency: SimPy's latency.py example, builder API.

A sender sends a message every 5 seconds over a cable. The cable delivers each message
into a store 10 seconds later, and a receiver takes each message as it arrives.

Needs no containers. Runs unpaced and ends after 100 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    """Builds the cable, with `size` receivers."""
    app = SimulationContext(sim_id="Cable", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    app.add_store("cable")
    app.add_arrival("sender", "constant", value=5, spawn="send")
    app.add_initial_arrival("receiver", size, spawn="receive")

    # Each message travels on its own, so the sender goes on at once
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
        """Sends a message."""
        return {"event_type": "sent"}

    @app.task(
        name="latency",
        service_time=10,
        put={"store": "cable", "item": generator({"gen": "expr", "expr": "arg.msg"})},
    )
    def latency(task_id):
        """Delivers the message into the cable's store after the delay."""
        return {"event_type": "delivered"}

    receive_payload = Template(
        {"event_type": "received", "msg": {"gen": "expr", "expr": "item"}},
        name="receive",
    )

    @app.task(name="receive", get={"store": "cable"}, next="receive")
    def receive(task_id):
        """Takes the next message."""
        return receive_payload

    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs 100 simulation seconds."""
    model.run(until=100)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

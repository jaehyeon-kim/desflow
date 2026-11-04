"""Event Latency: SimPy's latency.py example, low-level API.

The low-level version of `builder/simpy/latency_example.py`: SimPy's own `Cable`,
sender and receiver on a `DynamicRealtimeEnvironment`. They publish with
`publish_event` the records that the YAML and builder versions publish: each task's
`queued` and `started`, its own events, and the number of the task's instance. A
sender sends a message every 5 seconds over a cable. The cable delivers each message
into a store 10 seconds later, and a receiver takes each message as it arrives.

Needs no containers. Runs unpaced and ends after 100 simulation seconds.
"""

import itertools
import logging

import simpy

from desflow import ConsoleEgress, DynamicRealtimeEnvironment

UNTIL = 100


class Cable:
    """A cable that delivers each value `delay` seconds after it was put in."""

    def __init__(self, env: DynamicRealtimeEnvironment, delay: float):
        self.env = env
        self.delay = delay
        self.store = simpy.Store(env)
        self.numbers = itertools.count(1)
        self.instances = itertools.count()

    def latency(self, name, value):
        number = next(self.instances)

        def publish(record):
            self.env.publish_event(name, record, task="latency", instance=number)

        publish({"status": "started"})
        yield self.env.timeout(self.delay)
        done = self.store.put(value)
        done.callbacks.append(lambda _: publish({"event_type": "delivered"}))

    def put(self, value):
        """Sends `value`, which travels on its own."""
        name = f"latency-{next(self.numbers)}"
        self.env.process(self.latency(name, value))

    def get(self):
        """The next value delivered."""
        return self.store.get()


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    """Starts the sender and `size` receivers on `env`."""
    cable = Cable(env, 10)
    gets = itertools.count()

    def sender():
        for number in itertools.count(1):
            yield env.timeout(5)
            env.publish_event(
                f"sender-{number}",
                {"event_type": "sent"},
                task="send",
                instance=number - 1,
            )
            cable.put(f"Sender sent this at {env.now:g}")

    def receiver(name):
        def publish(number, record):
            env.publish_event(name, record, task="receive", instance=number)

        while True:
            number = next(gets)  # each message is taken by a new instance
            publish(number, {"status": "queued"})
            msg = yield cable.get()
            publish(number, {"status": "started"})
            publish(number, {"event_type": "received", "msg": msg})

    env.process(sender())
    for number in range(1, size + 1):
        env.process(receiver(f"receiver-{number}"))


def run() -> None:
    """Runs 100 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

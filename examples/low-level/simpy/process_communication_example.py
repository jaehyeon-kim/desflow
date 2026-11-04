"""Process Communication: SimPy's process_communication.py example, one to one, low-level API.

The low-level version of `builder/simpy/process_communication_example.py`: SimPy's own
`message_generator` and `message_consumer` on a `DynamicRealtimeEnvironment`. They
publish with `publish_event` the records that the YAML and builder versions publish:
each task's `queued` and `started`, its own events, and the number of the task's
instance. A generator puts a message into a store every 6 to 10 seconds, stamped with
the time it was sent. A consumer takes each message and then works on it for 4 to 8
seconds, so it is late for some messages.

Each value is drawn from desflow's `Sampler`, from the random sequence the YAML and
builder versions use for the same place, so the three versions publish the same
records.

Needs no containers. Runs unpaced and ends after 100 simulation seconds.
"""

import itertools
import logging

import simpy

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 100


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    """Starts the generator and `size` consumers on `env`."""
    sampler = Sampler(seed=42)
    gaps = sampler.generator("arrivals.generator.gap")
    busy_times = sampler.generator("tasks.busy.service_time")
    pipe = simpy.Store(env)
    numbers = {task: itertools.count() for task in ("listen", "busy")}

    def message_generator(name, out_pipe):
        for number in itertools.count(1):
            yield env.timeout(int(gaps.integers(6, 11)))
            msg = (env.now, f"{name} says hello at {env.now:g}")
            out_pipe.put(msg)  # not waited for, as in SimPy's example
            env.publish_event(
                f"generator-{number}",
                {"event_type": "said"},
                task="speak",
                instance=number - 1,
            )

    def message_consumer(key, in_pipe):
        def publish(task, number, value):
            env.publish_event(key, value, task=task, instance=number)

        while True:
            listen = next(numbers["listen"])
            publish("listen", listen, {"status": "queued"})
            msg = yield in_pipe.get()
            publish("listen", listen, {"status": "started"})
            # The message is late when it waited in the pipe
            received = {
                "event_type": "received",
                "late": msg[0] < env.now,
                "text": msg[1],
            }
            publish("listen", listen, received)
            busy = next(numbers["busy"])
            publish("busy", busy, {"status": "started"})
            yield env.timeout(int(busy_times.integers(4, 9)))
            publish("busy", busy, {"event_type": "handled"})

    env.process(message_generator("Generator A", pipe))
    for number in range(1, size + 1):
        env.process(message_consumer(f"consumer-{number}", pipe))


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

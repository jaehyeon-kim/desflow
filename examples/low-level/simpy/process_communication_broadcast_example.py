"""Process Communication: SimPy's process_communication.py example, broadcast, low-level API.

The low-level version of `builder/simpy/process_communication_broadcast_example.py`:
SimPy's own `BroadcastPipe`, `message_generator` and `message_consumer` on a
`DynamicRealtimeEnvironment`. They publish with `publish_event` the records that the
YAML and builder versions publish: each task's `queued` and `started`, its own events,
and the number of the task's instance. A generator makes a message every 6 to 10
seconds, stamped with the time it was sent, and puts a copy into each of two pipes
without waiting. Two consumers each take the messages of their own pipe and work on each
for 4 to 8 seconds.

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


class BroadcastPipe:
    """A pipe that puts each message into the store of every consumer."""

    def __init__(self, env: DynamicRealtimeEnvironment):
        self.env = env
        self.pipes: list = []

    def put(self, value):
        """Puts `value` into every output store."""
        return self.env.all_of([pipe.put(value) for pipe in self.pipes])

    def get_output_conn(self):
        """A new store that receives every message from now on."""
        pipe = simpy.Store(self.env)
        self.pipes.append(pipe)
        return pipe


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    """Starts the generator, `size` consumers on the first pipe and one on the second."""
    sampler = Sampler(seed=42)
    gaps = sampler.generator("arrivals.generator.gap")
    busy_times = sampler.generator("tasks.busy.service_time")
    bc_pipe = BroadcastPipe(env)
    pipes = [bc_pipe.get_output_conn(), bc_pipe.get_output_conn()]
    numbers = {task: itertools.count() for task in ("listen", "busy")}

    def message_generator(name, out_pipe):
        for number in itertools.count(1):
            yield env.timeout(int(gaps.integers(6, 11)))
            key = f"generator-{number}"
            msg = {
                "event_type": "said",
                "sent": env.now,
                "text": f"{name} says hello at {env.now:g}",
            }
            out_pipe.put(msg)  # not waited for, as in SimPy's example
            env.publish_event(key, msg, task="speak", instance=number - 1)
            env.publish_event(
                key, {"event_type": "sent"}, task="send", instance=number - 1
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
                "late": msg["sent"] < env.now,
                "text": msg["text"],
            }
            publish("listen", listen, received)
            busy = next(numbers["busy"])
            publish("busy", busy, {"status": "started"})
            yield env.timeout(int(busy_times.integers(4, 9)))
            publish("busy", busy, {"event_type": "handled"})

    env.process(message_generator("Generator A", bc_pipe))
    for number in range(1, size + 1):
        env.process(message_consumer(f"consumer_a-{number}", pipes[0]))
    env.process(message_consumer("consumer_b-1", pipes[1]))


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

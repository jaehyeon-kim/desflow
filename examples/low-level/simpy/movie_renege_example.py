"""Movie Renege: SimPy's movie_renege.py example, low-level API.

The low-level version of `builder/simpy/movie_renege_example.py`: SimPy's own
`moviegoer` and `customer_arrivals` on a `DynamicRealtimeEnvironment`. They publish
with `publish_event` the records that the YAML and builder versions publish: each
task's `queued`, `started` and `balked`, its own events, and the number of the task's
instance. A movie theatre has one ticket counter and three movies with 50 tickets
each. A moviegoer picks a movie and 1 to 6 tickets, and does not join the queue once
the movie is sold out. At the counter, a moviegoer who asks for more tickets than are
left argues and leaves; otherwise the sale goes ahead. When fewer than 2 tickets are
left the movie is sold out, and everyone still queueing for it leaves.

SimPy's example counts minutes, and here each of its minutes is one second. So a
moviegoer arrives about every 0.5 seconds, an argument takes 0.5 seconds, a sale 1
second, and the run ends after 120 seconds, SimPy's 120 minutes. Each value is drawn
from desflow's `Sampler`, from the random sequence the YAML and builder versions use
for the same place, so the three versions publish the same records.

Needs no containers. Runs unpaced and ends after 120 simulation seconds.
"""

import itertools
import logging

import simpy

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

MOVIES = ["Python Unchained", "Kill Process", "Pulp Implementation"]
TICKETS = 50
UNTIL = 120
TASKS = ("moviegoer", "leave", "serve", "argue", "buy", "check", "sold", "finish")


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the moviegoers' arrivals on `env`."""
    sampler = Sampler(seed=42)
    gap = DistributionConfig(dist="exponential", rate=2)
    gap.stream = "arrival.goers"  # the sequence the blueprint's arrival draws from
    movie_choices = sampler.generator("arrivals.goers.attributes.movie")
    ticket_counts = sampler.generator("arrivals.goers.attributes.tickets")
    # --8<-- [start:theater]
    counter = simpy.Resource(env, capacity=1)
    available = {movie: TICKETS for movie in MOVIES}
    sold_out = {movie: env.event() for movie in MOVIES}
    numbers = {task: itertools.count() for task in TASKS}
    # --8<-- [end:theater]

    # --8<-- [start:moviegoer-waits]
    def moviegoer(name, movie, num_tickets):
        number = next(numbers["moviegoer"])

        def publish(task, value, instance=None):
            """Publishes `value` as `instance` of `task`, or as a new instance."""
            step = next(numbers[task]) if instance is None else instance
            env.publish_event(name, value, task=task, instance=step)
            return step

        if available[movie] == 0:  # sold out: does not join the queue
            publish("moviegoer", {"status": "balked"}, number)
            return
        # The task's wait and the counter's request it opens each publish queued
        publish("moviegoer", {"status": "queued"}, number)
        publish("moviegoer", {"status": "queued"}, number)
        turn = counter.request()
        result = yield turn | sold_out[movie]
        if turn in result:  # the request publishes started as it is granted
            publish("moviegoer", {"status": "started"}, number)
        publish("moviegoer", {"status": "started"}, number)
        publish("moviegoer", {"event_type": "at_counter"}, number)
        if turn not in result:  # the movie sold out while it queued
            turn.cancel()
            if turn.triggered:
                counter.release(turn)
            publish("leave", {"event_type": "reneged", "movie": movie})
            return
        # --8<-- [end:moviegoer-waits]
        # --8<-- [start:counter-sells]
        publish("serve", {"event_type": "served"})
        if available[movie] < num_tickets:
            arguing = publish("argue", {"status": "started"})
            yield env.timeout(0.5)
            publish("argue", {"event_type": "argued"}, arguing)
            counter.release(turn)
            return
        publish("buy", {"event_type": "bought"})
        available[movie] -= num_tickets
        publish("check", {"event_type": "checked"})
        # --8<-- [end:counter-sells]
        # --8<-- [start:film-sells-out]
        if available[movie] < 2:
            publish("sold", {"event_type": "sold_out", "movie": movie})
            sold_out[movie].succeed()
            available[movie] = 0
        finishing = publish("finish", {"status": "started"})
        yield env.timeout(1)
        publish("finish", {"event_type": "done"}, finishing)
        counter.release(turn)
        # --8<-- [end:film-sells-out]

    # --8<-- [start:moviegoers-arrive]
    def customer_arrivals():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            movie = MOVIES[int(movie_choices.integers(len(MOVIES)))]
            num_tickets = int(ticket_counts.integers(1, 7))
            env.process(moviegoer(f"goers-{number}", movie, num_tickets))

    env.process(customer_arrivals())
    # --8<-- [end:moviegoers-arrive]


def run() -> None:
    """Runs 120 simulation seconds."""
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

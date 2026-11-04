# SimPy 4.1.2, docs/examples/code/movie_renege.py, under SimPy's licence (LICENSE.rst):
#
# The MIT License (MIT)
#
# Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke (also see AUTHORS.txt)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
Movie renege example

Covers:

- Resources: Resource
- Condition events
- Shared events

Scenario:
  A movie theatre has one ticket counter selling tickets for three
  movies (next show only). When a movie is sold out, all people waiting
  to buy tickets for that movie renege (leave queue).

"""

from __future__ import annotations

import random
from typing import Dict, List, NamedTuple, Optional

import simpy

# --8<-- [start:settings]
RANDOM_SEED = 42
TICKETS = 50  # Number of tickets per movie
SELLOUT_THRESHOLD = 2  # Fewer tickets than this is a sellout
SIM_TIME = 120  # Simulate until
# --8<-- [end:settings]


# --8<-- [start:moviegoer-buys-or-leaves]
def moviegoer(env, movie, num_tickets, theater):
    """A moviegoer tries to by a number of tickets (*num_tickets*) for
    a certain *movie* in a *theater*.

    If the movie becomes sold out, she leaves the theater. If she gets
    to the counter, she tries to buy a number of tickets. If not enough
    tickets are left, she argues with the teller and leaves.

    If at most one ticket is left after the moviegoer bought her
    tickets, the *sold out* event for this movie is triggered causing
    all remaining moviegoers to leave.

    """
    with theater.counter.request() as my_turn:
        # Wait until it's our turn or until the movie is sold out
        result = yield my_turn | theater.sold_out[movie]

        # Check if it's our turn or if movie is sold out
        if my_turn not in result:
            theater.num_renegers[movie] += 1
            return

        # Check if enough tickets left.
        if theater.available[movie] < num_tickets:
            # Moviegoer leaves after some discussion
            yield env.timeout(0.5)
            return

        # Buy tickets
        theater.available[movie] -= num_tickets
        if theater.available[movie] < SELLOUT_THRESHOLD:
            # Trigger the "sold out" event for the movie
            theater.sold_out[movie].succeed()
            theater.when_sold_out[movie] = env.now
            theater.available[movie] = 0
        yield env.timeout(1)
# --8<-- [end:moviegoer-buys-or-leaves]


# --8<-- [start:moviegoers-arrive]
def customer_arrivals(env, theater):
    """Create new *moviegoers* until the sim time reaches 120."""
    while True:
        yield env.timeout(random.expovariate(1 / 0.5))

        movie = random.choice(theater.movies)
        num_tickets = random.randint(1, 6)
        if theater.available[movie]:
            env.process(moviegoer(env, movie, num_tickets, theater))
# --8<-- [end:moviegoers-arrive]


# --8<-- [start:theater]
class Theater(NamedTuple):
    counter: simpy.Resource
    movies: List[str]
    available: Dict[str, int]
    sold_out: Dict[str, simpy.Event]
    when_sold_out: Dict[str, Optional[float]]
    num_renegers: Dict[str, int]
# --8<-- [end:theater]


# --8<-- [start:run]
# Setup and start the simulation
print('Movie renege')
random.seed(RANDOM_SEED)
env = simpy.Environment()

# Create movie theater
movies = ['Python Unchained', 'Kill Process', 'Pulp Implementation']
theater = Theater(
    counter=simpy.Resource(env, capacity=1),
    movies=movies,
    available=dict.fromkeys(movies, TICKETS),
    sold_out={movie: env.event() for movie in movies},
    when_sold_out=dict.fromkeys(movies),
    num_renegers=dict.fromkeys(movies, 0),
)

# Start process and run
env.process(customer_arrivals(env, theater))
env.run(until=SIM_TIME)

# Analysis/results
for movie in movies:
    if theater.sold_out[movie]:
        sellout_time = theater.when_sold_out[movie]
        num_renegers = theater.num_renegers[movie]
        print(
            f'Movie "{movie}" sold out {sellout_time:.1f} minutes '
            f'after ticket counter opening.'
        )
        print(f'  Number of people leaving queue when film sold out: {num_renegers}')
# --8<-- [end:run]

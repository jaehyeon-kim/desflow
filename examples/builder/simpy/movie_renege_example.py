"""Movie Renege: SimPy's movie_renege.py example, builder API.

A movie theatre has one ticket counter and three movies with 50 tickets each. A
moviegoer arrives at exponential gaps, picks a movie and 1 to 6 tickets, and does not
join the queue once the movie is sold out. At the counter, a moviegoer who asks for
more tickets than are left argues and leaves; otherwise the sale goes ahead. When fewer
than 2 tickets are left the movie is sold out, and everyone still queueing for it
leaves (reneged).

SimPy's example counts minutes, and here each of its minutes is one second. So a
moviegoer arrives about every 0.5 seconds, an argument takes 0.5 seconds, a sale 1
second, and the run ends after 120 seconds, SimPy's 120 minutes. Each value is drawn
from desflow's random sequence for its place in the model, so the numbers differ from
those SimPy prints.

The counter is a request opened by the moviegoer and held across the tasks that
follow, and each movie's sold-out event is one member of the signal family `sold_out`.

Needs no containers. Runs unpaced and ends after 120 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

MOVIES = ["Python Unchained", "Kill Process", "Pulp Implementation"]


def build() -> SimulationContext:
    """Builds the theatre."""
    app = SimulationContext(sim_id="Movie", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    # --8<-- [start:theater]
    app.add_resource("counter", current_cap=1)
    app.add_variable("available", {movie: 50 for movie in MOVIES})
    app.add_variable("when_sold_out", {})
    app.add_variable("num_renegers", {movie: 0 for movie in MOVIES})
    app.add_signal("sold_out", family=True, once=True)
    # --8<-- [end:theater]
    # --8<-- [start:moviegoers-arrive]
    app.add_arrival(
        "goers",
        "exponential",
        rate=2,
        attributes={
            "movie": generator({"gen": "one_of", "choices": MOVIES}),
            "tickets": generator({"gen": "integer", "bounds": [1, 6]}),
        },
        spawn="moviegoer",
    )
    # --8<-- [end:moviegoers-arrive]

    # --8<-- [start:moviegoer-waits]
    @app.task(
        name="moviegoer",
        balk="registry['Movie.variables.available'][attr.movie] == 0",
        open={"slot": {"request": "counter"}},
        wait={
            "any": {
                "turn": {"handle": "slot"},
                "out": {"signal": {"of": "sold_out", "key": "attr.movie"}},
            }
        },
        next={
            "when": [{"condition": "wait.turn.done", "task": "serve"}],
            "default": "leave",
        },
    )
    def moviegoer(task_id):
        """Waits for the counter or for the movie to sell out."""
        return {"event_type": "at_counter"}

    leave_payload = Template(
        {"event_type": "reneged", "movie": {"gen": "expr", "expr": "attr.movie"}},
        name="leave",
    )

    @app.task(
        name="leave",
        cancel="slot",
        release={"handle": "slot"},
        on=[
            {
                "path": "Movie.variables.num_renegers",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Movie.variables.num_renegers'], attr.movie, get_key(registry['Movie.variables.num_renegers'], attr.movie, 0) + 1)",
                    }
                ),
            }
        ],
    )
    def leave(task_id):
        """Leaves the queue: the movie sold out."""
        return leave_payload

    # --8<-- [end:moviegoer-waits]

    # --8<-- [start:counter-sells]
    @app.task(
        name="serve",
        next={
            "when": [
                {
                    "condition": "registry['Movie.variables.available'][attr.movie] < attr.tickets",
                    "task": "argue",
                }
            ],
            "default": "buy",
        },
    )
    def serve(task_id):
        """Reaches the counter."""
        return {"event_type": "served"}

    @app.task(name="argue", service_time=0.5, release={"handle": "slot"})
    def argue(task_id):
        """Asks for more tickets than are left."""
        return {"event_type": "argued"}

    @app.task(
        name="buy",
        on=[
            {
                "path": "Movie.variables.available",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Movie.variables.available'], attr.movie, registry['Movie.variables.available'][attr.movie] - attr.tickets)",
                    }
                ),
            }
        ],
        next="check",
    )
    def buy(task_id):
        """Buys the tickets."""
        return {"event_type": "bought"}

    @app.task(
        name="check",
        next={
            "when": [
                {
                    "condition": "registry['Movie.variables.available'][attr.movie] < 2",
                    "task": "sold",
                }
            ],
            "default": "finish",
        },
    )
    def check(task_id):
        """Checks how many tickets are left."""
        return {"event_type": "checked"}

    # --8<-- [end:counter-sells]

    # --8<-- [start:film-sells-out]
    sold_payload = Template(
        {"event_type": "sold_out", "movie": {"gen": "expr", "expr": "attr.movie"}},
        name="sold",
    )

    @app.task(
        name="sold",
        on=[
            {"fire": {"signal": {"of": "sold_out", "key": "attr.movie"}}},
            {
                "path": "Movie.variables.when_sold_out",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Movie.variables.when_sold_out'], attr.movie, t)",
                    }
                ),
            },
            {
                "path": "Movie.variables.available",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Movie.variables.available'], attr.movie, 0)",
                    }
                ),
            },
        ],
        next="finish",
    )
    def sold(task_id):
        """Sells the movie out, which sends its queue away."""
        return sold_payload

    @app.task(name="finish", service_time=1, release={"handle": "slot"})
    def finish(task_id):
        """Finishes the sale."""
        return {"event_type": "done"}

    # --8<-- [end:film-sells-out]

    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs 120 simulation seconds."""
    model.run(until=120)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

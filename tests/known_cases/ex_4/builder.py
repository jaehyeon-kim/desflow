"""EX-4: Movie tickets with a sold-out event that makes people leave.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Theater", factor=0, random_seed=42)
    app.add_resource("counter", current_cap=1)
    app.add_variable(
        "available",
        {"Python Unchained": 50, "Kill Process": 50, "Pulp Implementation": 50},
    )
    app.add_variable("when_sold_out", {})
    app.add_variable(
        "num_renegers",
        {"Python Unchained": 0, "Kill Process": 0, "Pulp Implementation": 0},
    )
    app.add_signal("sold_out", family=True, once=True)
    app.add_arrival(
        "goers",
        "constant",
        value=0.5,
        attributes={
            "n": generator({"gen": "sequence", "start": 0}),
            "movie": generator(
                {
                    "gen": "expr",
                    "expr": "['Python Unchained', 'Kill Process', 'Pulp Implementation'][(attr.n * 7) % 3 if attr.n % 5 else 0]",
                }
            ),
            "tickets": generator({"gen": "expr", "expr": "1 + (attr.n * 5) % 6"}),
        },
        spawn="moviegoer",
    )

    @app.task(
        name="moviegoer",
        balk="registry['Theater.variables.available'][attr.movie] == 0",
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
        return {"event_type": "at_counter"}

    @app.task(
        name="leave",
        cancel="slot",
        release={"handle": "slot"},
        on=[
            {
                "path": "Theater.variables.num_renegers",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Theater.variables.num_renegers'], attr.movie, get_key(registry['Theater.variables.num_renegers'], attr.movie, 0) + 1)",
                    }
                ),
            }
        ],
    )
    def leave(task_id):
        return {"event_type": "reneged"}

    @app.task(
        name="serve",
        next={
            "when": [
                {
                    "condition": "registry['Theater.variables.available'][attr.movie] < attr.tickets",
                    "task": "argue",
                }
            ],
            "default": "buy",
        },
    )
    def serve(task_id):
        return {"event_type": "served"}

    @app.task(name="argue", service_time=0.5, release={"handle": "slot"})
    def argue(task_id):
        return {"event_type": "argued"}

    @app.task(
        name="buy",
        on=[
            {
                "path": "Theater.variables.available",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Theater.variables.available'], attr.movie, registry['Theater.variables.available'][attr.movie] - attr.tickets)",
                    }
                ),
            }
        ],
        next="check",
    )
    def buy(task_id):
        return {"event_type": "bought"}

    @app.task(
        name="check",
        next={
            "when": [
                {
                    "condition": "registry['Theater.variables.available'][attr.movie] < 2",
                    "task": "sold",
                }
            ],
            "default": "finish",
        },
    )
    def check(task_id):
        return {"event_type": "checked"}

    @app.task(
        name="sold",
        on=[
            {"fire": {"signal": {"of": "sold_out", "key": "attr.movie"}}},
            {
                "path": "Theater.variables.when_sold_out",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Theater.variables.when_sold_out'], attr.movie, t)",
                    }
                ),
            },
            {
                "path": "Theater.variables.available",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Theater.variables.available'], attr.movie, 0)",
                    }
                ),
            },
        ],
        next="finish",
    )
    def sold(task_id):
        return {"event_type": "sold_out"}

    @app.task(name="finish", service_time=1, release={"handle": "slot"})
    def finish(task_id):
        return {"event_type": "done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=120)

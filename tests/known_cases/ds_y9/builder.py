"""DS-Y9: Fail an event with a computed reason.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator
from desflow.builder.task import NO_RECORD


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Y9", factor=0, random_seed=42)
    app.add_signal("ev", once=True)
    app.add_signal("dv", once=True, defused=True)
    app.add_initial_arrival("w", size, spawn="w")
    app.add_initial_arrival("f", 1, spawn="f", attributes={"n": 7})
    app.add_initial_arrival("d", 1, spawn="d")
    app.add_initial_arrival("c", 1, spawn="c", attributes={"n": 3})

    @app.task(name="w", wait={"signal": "ev"}, on_fail="caught")
    def w(task_id):
        return {"event_type": "value"}

    caught_payload = Template(
        {
            "event_type": "caught",
            "why": {"gen": "expr", "expr": "upstream.reason"},
            "at": {"gen": "expr", "expr": "t"},
        },
        name="caught",
    )

    @app.task(name="caught")
    def caught(task_id):
        return caught_payload

    @app.task(
        name="f",
        wait={"timeout": 1},
        on=[
            {
                "fire": {
                    "signal": "ev",
                    "fail": True,
                    "value": generator(
                        {
                            "gen": "format",
                            "template": "bad {n}",
                            "names": {"n": {"gen": "attr", "name": "n"}},
                        }
                    ),
                }
            }
        ],
    )
    def f(task_id):
        return NO_RECORD

    @app.task(
        name="d",
        wait={"timeout": 1},
        on=[{"fire": {"signal": "dv", "fail": True, "value": "x"}}],
        next="d_end",
    )
    def d(task_id):
        return {"event_type": "failer_goes_on"}

    @app.task(name="d_end", wait={"timeout": 1})
    def d_end(task_id):
        return {"event_type": "failer_ended"}

    @app.task(
        name="c",
        wait={"timeout": 1},
        fail="True",
        fail_reason=generator(
            {
                "gen": "format",
                "template": "missing {n}",
                "names": {"n": {"gen": "attr", "name": "n"}},
            }
        ),
        on_fail="c_caught",
    )
    def c(task_id):
        return {"event_type": "c"}

    c_caught_payload = Template(
        {
            "event_type": "caught",
            "why": {"gen": "expr", "expr": "upstream.reason"},
            "at": {"gen": "expr", "expr": "t"},
        },
        name="c_caught",
    )

    @app.task(name="c_caught")
    def c_caught(task_id):
        return c_caught_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""DS-Y11: A callback added during the run.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator
from desflow.core.task import NO_RECORD


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Y11", factor=0, random_seed=42)
    app.add_variable("cb", 0)
    app.add_variable("n2", 0)
    app.add_variable("n0", 0)
    app.add_signal("ev", once=True)
    app.add_signal("d2", once=True)
    app.add_signal("d0", once=True)
    app.add_initial_arrival("fw", size, spawn="first_waiter")
    app.add_initial_arrival("at", 1, spawn="attach")
    app.add_initial_arrival("fi", 1, spawn="fire")
    app.add_initial_arrival("lw", 1, spawn="late")
    app.add_initial_arrival("p2", 1, spawn="cond2")
    app.add_initial_arrival("p0", 1, spawn="cond0")
    first_waiter_payload = Template(
        {
            "event_type": "first_waiter",
            "value": {"gen": "expr", "expr": "signal.ev.value"},
            "cb": {"gen": "expr", "expr": "registry['Y11.variables.cb']"},
            "now": {"gen": "expr", "expr": "t"},
        },
        name="first_waiter",
    )

    @app.task(name="first_waiter", wait={"signal": "ev"})
    def first_waiter(task_id):
        return first_waiter_payload

    @app.task(name="attach", wait={"timeout": 1}, next="attach2")
    def attach(task_id):
        return NO_RECORD

    @app.task(
        name="attach2",
        listen={
            "signal": "ev",
            "on": [
                {
                    "path": "Y11.variables.cb",
                    "value": generator({"gen": "expr", "expr": "signal.ev.value"}),
                }
            ],
        },
    )
    def attach2(task_id):
        return NO_RECORD

    @app.task(
        name="fire", wait={"timeout": 2}, on=[{"fire": {"signal": "ev", "value": 5}}]
    )
    def fire(task_id):
        return NO_RECORD

    @app.task(name="late", wait={"timeout": 1.5}, next="late_wait")
    def late(task_id):
        return NO_RECORD

    late_wait_payload = Template(
        {
            "event_type": "late_waiter",
            "value": {"gen": "expr", "expr": "signal.ev.value"},
            "cb": {"gen": "expr", "expr": "registry['Y11.variables.cb']"},
            "now": {"gen": "expr", "expr": "t"},
        },
        name="late_wait",
    )

    @app.task(name="late_wait", wait={"signal": "ev"})
    def late_wait(task_id):
        return late_wait_payload

    @app.task(
        name="cond2",
        listen={
            "signal": "d2",
            "on": [
                {
                    "path": "Y11.variables.n2",
                    "value": generator(
                        {
                            "gen": "expr",
                            "expr": "registry['Y11.variables.n2'] + signal.d2.value",
                        }
                    ),
                }
            ],
        },
        wait={"timeout": 1},
        on=[{"fire": {"signal": "d2", "value": 3}}],
        next="cond2_end",
    )
    def cond2(task_id):
        return NO_RECORD

    cond2_end_payload = Template(
        {
            "event_type": "n",
            "n": {"gen": "expr", "expr": "registry['Y11.variables.n2']"},
        },
        name="cond2_end",
    )

    @app.task(name="cond2_end", wait={"timeout": 1})
    def cond2_end(task_id):
        return cond2_end_payload

    @app.task(
        name="cond0",
        wait={"timeout": 1},
        on=[{"fire": {"signal": "d0", "value": 3}}],
        next="cond0_end",
    )
    def cond0(task_id):
        return NO_RECORD

    cond0_end_payload = Template(
        {
            "event_type": "n",
            "n": {"gen": "expr", "expr": "registry['Y11.variables.n0']"},
        },
        name="cond0_end",
    )

    @app.task(name="cond0_end", wait={"timeout": 1})
    def cond0_end(task_id):
        return cond0_end_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

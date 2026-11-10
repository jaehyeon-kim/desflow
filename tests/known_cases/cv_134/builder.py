"""CV-134: A computed list of events made now and waited for later.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="S", factor=0, random_seed=42)
    app.add_resource("r", family=True, members=3, current_cap=1)
    app.add_initial_arrival("a", size, spawn="begin")
    app.add_initial_arrival(
        "h",
        3,
        spawn="hold",
        attributes={"i": generator({"gen": "sequence", "start": 0})},
    )

    @app.task(
        name="hold",
        resource_id={"of": "r", "key": "attr.i"},
        service_time="3 - attr.i",
    )
    def hold(task_id):
        return {"e": "held"}

    @app.task(name="begin", wait={"timeout": 0.5}, next="opener")
    def begin(task_id):
        return {"e": "half"}

    @app.task(
        name="opener",
        open={
            "pool": {
                "each": 3,
                "branch": {"request": {"of": "r", "key": "each.index"}},
            }
        },
        next="waiter",
    )
    def opener(task_id):
        return {"e": "opened"}

    @app.task(name="waiter", wait={"handle": "pool", "index": 2}, next="closer")
    def waiter(task_id):
        return {"e": "got"}

    closer_payload = Template(
        {
            "e": "done",
            "x": {
                "gen": "expr",
                "expr": "[handle.pool.flags, [queue.r[0], queue.r[1], queue.r[2]]]",
            },
        },
        name="closer",
    )

    @app.task(
        name="closer",
        cancel={"handle": "pool"},
        release={"handle": "pool", "index": 2},
    )
    def closer(task_id):
        return closer_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

"""CV-074: Wait on a computed number of requests.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_resource("desk", family=True, members=3, current_cap=1)
    app.add_initial_arrival("h0", size, spawn="hold0")
    app.add_initial_arrival("h1", 1, spawn="hold1")
    app.add_initial_arrival("h2", 1, spawn="hold2")
    app.add_initial_arrival("p", 1, spawn="pause", attributes={"count": 3})

    @app.task(name="hold0", resource_id={"of": "desk", "key": "0"}, service_time=3)
    def hold0(task_id):
        return {"event_type": "held0"}

    @app.task(name="hold1", resource_id={"of": "desk", "key": "1"}, service_time=2)
    def hold1(task_id):
        return {"event_type": "held1"}

    @app.task(name="hold2", resource_id={"of": "desk", "key": "2"}, service_time=1)
    def hold2(task_id):
        return {"event_type": "held2"}

    @app.task(name="pause", wait={"timeout": 0.5}, next="any_desk")
    def pause(task_id):
        return {"event_type": "paused"}

    any_desk_payload = Template(
        {"event_type": "got_one", "at": {"gen": "expr", "expr": "t"}}, name="any_desk"
    )

    @app.task(
        name="any_desk",
        wait={
            "each": "attr.count",
            "group": "any",
            "branch": {"request": {"of": "desk", "key": "each.index"}},
        },
    )
    def any_desk(task_id):
        return any_desk_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

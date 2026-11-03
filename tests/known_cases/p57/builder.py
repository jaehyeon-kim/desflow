"""P57: Clone with its own attributes.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_service("one", dist="constant", value=1)
    app.add_service("two", dist="constant", value=2)
    app.add_initial_arrival("a", size, spawn="split")

    @app.task(name="split", spawn={"task": "c1"}, next="split2")
    def split(task_id):
        return {}

    @app.task(name="split2", spawn={"task": "c2"}, next="join")
    def split2(task_id):
        return {}

    @app.task(
        name="join",
        wait={
            "all": {
                "r1": {"running": {"task": "c1"}},
                "r2": {"running": {"task": "c2"}},
            }
        },
    )
    def join(task_id):
        return {}

    @app.task(name="c1", service_id="one", set={"x": 1}, next="c1b")
    def c1(task_id):
        return {}

    @app.task(name="c1b")
    def c1b(task_id, entity):
        return {"x": entity.attributes.get("x")}

    @app.task(name="c2", service_id="two", set={"x": 2}, next="c2b")
    def c2(task_id):
        return {}

    @app.task(name="c2b")
    def c2b(task_id, entity):
        return {"x": entity.attributes.get("x")}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

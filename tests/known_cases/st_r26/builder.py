"""ST-R26: A later get still matches after a mismatch.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_store("s", current_cap=2)
    app.add_initial_arrival("g", size, spawn="getter")
    app.add_initial_arrival("p", 1, spawn="putter")
    getter_payload = Template(
        {
            "event_type": "first",
            "spam_first": {"gen": "expr", "expr": "handle.spam.triggered"},
        },
        name="getter",
    )

    @app.task(
        name="getter",
        open={
            "eggs": {"get": {"store": "s", "filter": "item == 'eggs'"}},
            "spam": {"get": {"store": "s", "filter": "item == 'spam'"}},
        },
        wait={"any": {"spam": {"handle": "spam"}, "eggs": {"handle": "eggs"}}},
        next="second",
    )
    def getter(task_id):
        return getter_payload

    @app.task(name="second", wait={"handle": "eggs"})
    def second(task_id):
        return {"event_type": "eggs_at_one"}

    @app.task(name="putter", put={"store": "s", "item": "spam"}, next="later")
    def putter(task_id):
        return {"event_type": "put_spam"}

    @app.task(name="later", service_time=1, next="put_eggs")
    def later(task_id):
        return {"event_type": "pause"}

    @app.task(name="put_eggs", put={"store": "s", "item": "eggs"})
    def put_eggs(task_id):
        return {"event_type": "put_eggs"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

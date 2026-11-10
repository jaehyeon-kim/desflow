"""CV-071: Two units held by one process.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def _reads(event: str, expression: str, name: str) -> Template:
    return Template({"e": event, "x": {"gen": "expr", "expr": expression}}, name=name)


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_resource("m", current_cap=2)
    app.add_resource("n", current_cap=2)
    app.add_initial_arrival("a", size, spawn="one")
    app.add_initial_arrival("b", 1, spawn="subject")
    one_payload = _reads("one", "users.m", "one")
    two_payload = _reads("both", "users.m", "two")
    after_first_payload = _reads("after_first", "users.m", "after_first")
    after_second_payload = _reads("after_second", "users.m", "after_second")
    subject_payload = _reads("both", "users.n", "subject")
    after_q_payload = _reads("after_q", "[users.n, handle.p.triggered]", "after_q")
    after_p_payload = _reads("after_p", "users.n", "after_p")

    @app.task(name="one", seize="m", next="two")
    def one(task_id):
        return one_payload

    @app.task(name="two", seize="m", next="first")
    def two(task_id):
        return two_payload

    @app.task(name="first", service_time=1, release="m", next="after_first")
    def first(task_id):
        return {"e": "first"}

    @app.task(name="after_first", next="second")
    def after_first(task_id):
        return after_first_payload

    @app.task(name="second", service_time=1, release="m", next="after_second")
    def second(task_id):
        return {"e": "second"}

    @app.task(name="after_second")
    def after_second(task_id):
        return after_second_payload

    @app.task(
        name="subject",
        open={"p": {"request": "n"}, "q": {"request": "n"}},
        wait={"all": {"x": {"handle": "p"}, "y": {"handle": "q"}}},
        next="free_q",
    )
    def subject(task_id):
        return subject_payload

    @app.task(name="free_q", service_time=1, release={"handle": "q"}, next="after_q")
    def free_q(task_id):
        return {"e": "free_q"}

    @app.task(name="after_q", next="free_p")
    def after_q(task_id):
        return after_q_payload

    @app.task(name="free_p", service_time=1, release={"handle": "p"}, next="after_p")
    def free_p(task_id):
        return {"e": "free_p"}

    @app.task(name="after_p")
    def after_p(task_id):
        return after_p_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

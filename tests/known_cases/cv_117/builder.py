"""CV-117: A nested generator called recursively.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="g")
    g_payload = Template(
        {"event_type": "total", "total": {"gen": "expr", "expr": "wait.f.value.ret"}},
        name="g",
    )

    @app.task(name="g", wait={"task": "f", "args": {"k": 2}})
    def g(task_id):
        return g_payload

    f_payload = Template(
        {"event_type": "call", "k": {"gen": "expr", "expr": "arg.k"}}, name="f"
    )

    @app.task(
        name="f",
        next={"when": [{"condition": "arg.k > 0", "task": "step"}], "default": "base"},
    )
    def f(task_id):
        return f_payload

    step_payload = Template(
        {"event_type": "tick", "k": {"gen": "expr", "expr": "arg.k"}}, name="step"
    )

    @app.task(name="step", service_time=1, next="down")
    def step(task_id):
        return step_payload

    down_payload = Template(
        {
            "event_type": "level",
            "k": {"gen": "expr", "expr": "arg.k"},
            "v": {"gen": "expr", "expr": "wait.f.value.ret"},
            "ret": {"gen": "expr", "expr": "wait.f.value.ret + arg.k"},
        },
        name="down",
    )

    @app.task(
        name="down",
        wait={
            "task": "f",
            "args": {"k": generator({"gen": "expr", "expr": "arg.k - 1"})},
        },
    )
    def down(task_id):
        return down_payload

    @app.task(name="base")
    def base(task_id):
        return {"event_type": "base", "ret": 0}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

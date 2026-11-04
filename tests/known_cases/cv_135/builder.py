"""CV-135: A class with fields and methods.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="deposit", attributes={"balance": 0})
    deposit_payload = Template(
        {
            "event_type": "deposit",
            "balance": {"gen": "expr", "expr": "attr.balance + 10"},
        },
        name="deposit",
    )

    @app.task(
        name="deposit",
        set={"balance": generator({"gen": "expr", "expr": "attr.balance + 10"})},
        next="withdraw",
    )
    def deposit(task_id):
        return deposit_payload

    @app.task(
        name="withdraw",
        set={"balance": generator({"gen": "expr", "expr": "attr.balance - 4"})},
        next={
            "when": [{"condition": "attr.balance == 6", "task": "spend"}],
            "default": "save",
        },
    )
    def withdraw(task_id):
        return {"event_type": "withdraw"}

    spend_payload = Template(
        {"event_type": "spend", "balance": {"gen": "attr", "name": "balance"}},
        name="spend",
    )
    save_payload = Template(
        {"event_type": "save", "balance": {"gen": "attr", "name": "balance"}},
        name="save",
    )

    @app.task(name="spend")
    def spend(task_id):
        return spend_payload

    @app.task(name="save")
    def save(task_id):
        return save_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

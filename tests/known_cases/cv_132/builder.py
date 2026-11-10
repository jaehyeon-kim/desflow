"""CV-132: A process waited for by two waiters.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template


def build() -> SimulationContext:
    app = SimulationContext(sim_id="C", factor=0, random_seed=42)
    app.add_signal("done", once=True)
    app.add_initial_arrival("w", 1, spawn="worker")
    app.add_initial_arrival("a", 1, spawn="waiter_a")
    app.add_arrival("b", "constant", value=5, spawn="waiter_b", max_events=1)

    @app.task(
        name="worker",
        service_time=2,
        on=[{"fire": {"signal": "done", "value": 7}}],
    )
    def worker(task_id):
        return {"event_type": "worker_done"}

    waiter_a_payload = Template(
        {
            "event_type": "a_saw",
            "at": {"gen": "expr", "expr": "t"},
            "v": {"gen": "expr", "expr": "wait.done.value"},
        },
        name="tasks.waiter_a",
    )

    @app.task(name="waiter_a", wait={"signal": "done"})
    def waiter_a(task_id):
        return waiter_a_payload

    waiter_b_payload = Template(
        {
            "event_type": "b_saw_after",
            "at": {"gen": "expr", "expr": "t"},
            "v": {"gen": "expr", "expr": "wait.done.value"},
        },
        name="tasks.waiter_b",
    )

    @app.task(name="waiter_b", wait={"signal": "done"})
    def waiter_b(task_id):
        return waiter_b_payload

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

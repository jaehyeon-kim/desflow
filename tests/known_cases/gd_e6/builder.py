"""GD-E6: Event, process, timeout, all of and any of through the environment.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="E", factor=0, random_seed=42)
    app.add_signal("ev", once=True)
    app.add_initial_arrival("a", size, spawn="use_all")

    @app.task(
        name="use_all",
        wait={
            "all": {"t": {"timeout": 3}, "p": {"task": "child"}, "e": {"signal": "ev"}}
        },
        next="use_any",
    )
    def use_all(task_id):
        return {"event_type": "all_done"}

    @app.task(
        name="use_any", wait={"any": {"t": {"timeout": 1}, "e": {"signal": "ev"}}}
    )
    def use_any(task_id):
        return {"event_type": "any_done"}

    @app.task(name="child", service_time=2, on=[{"fire": {"signal": "ev"}}])
    def child(task_id):
        return {"event_type": "child_done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

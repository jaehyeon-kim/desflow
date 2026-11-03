"""GD-V6: Any of and all of over a list of events.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="V", factor=0, random_seed=42)
    app.add_initial_arrival("a", size, spawn="both")

    @app.task(
        name="both",
        wait={"all": {"a": {"timeout": 1}, "b": {"timeout": 2}}},
        next="either",
    )
    def both(task_id):
        return {"event_type": "all_done"}

    @app.task(name="either", wait={"any": {"a": {"timeout": 1}, "b": {"timeout": 2}}})
    def either(task_id):
        return {"event_type": "any_done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

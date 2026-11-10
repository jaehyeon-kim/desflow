"""P54: Assembly from two stores.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_store("sa", current_cap=10)
    app.add_store("sb", current_cap=10)
    app.add_arrival("a", "constant", value=1, spawn="pa")
    app.add_arrival("b", "constant", value=2, spawn="pb")
    app.add_arrival("asm", "constant", value=0.5, spawn="assemble")

    @app.task(name="pa", put={"store": "sa"})
    def pa(task_id):
        return {"part": "a"}

    @app.task(name="pb", put={"store": "sb"})
    def pb(task_id):
        return {"part": "b"}

    @app.task(
        name="assemble",
        wait={"all": {"x": {"get": {"store": "sa"}}, "y": {"get": {"store": "sb"}}}},
    )
    def assemble(task_id):
        return {"done": 1}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=4.2)

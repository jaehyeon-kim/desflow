"""P09: Switch one source off and on.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build() -> SimulationContext:
    app = SimulationContext(sim_id="P", factor=0, random_seed=42)
    app.add_arrival("src", "constant", value=1, off_mode="drop", spawn="t")
    app.add_arrival("off", "constant", value=2.5)
    app.add_arrival("on", "constant", value=5.5)

    @app.task(name="t")
    def t(task_id):
        return {"event_type": "arrival"}

    @app.task(name="src_off", deactivate="src")
    def src_off(task_id):
        return {"event_type": "off"}

    @app.task(name="src_on", activate="src")
    def src_on(task_id):
        return {"event_type": "on"}

    @app.arrival_loop("off")
    def off_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("off")
            context.spawn(src_off(context.next_instance("src_off"), entity=entity))

    @app.arrival_loop("on")
    def on_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("on")
            context.spawn(src_on(context.next_instance("src_on"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=8)
